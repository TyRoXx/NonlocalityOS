use astraea::{
    storage::{LoadStoreTree, StrongReference},
    tree::TREE_BLOB_MAX_LENGTH,
};
use bytes::Bytes;
use dogbox_tree::serialization::{FileName, FileNameError};
use dogbox_tree_editor::{FileCreationMode, NormalizedPath, OpenDirectory, TreeEditor, WallClock};
use dropbox_sdk::{async_routes::files, default_async_client::UserAuthDefaultClient};
use futures::io::AsyncReadExt;
use relative_path::RelativePath;
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

enum ImportFileOutcome {
    Success,
    UnsupportedFileName(FileNameError),
}

async fn import_file(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    metadata: &files::FileMetadata,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) -> std::io::Result<ImportFileOutcome> {
    let file_name = match FileName::try_from(metadata.name.clone()) {
        Ok(success) => success,
        Err(e) => {
            info!("Unsupported file name {}: {e}", metadata.name);
            return Ok(ImportFileOutcome::UnsupportedFileName(e));
        }
    };
    let empty_file_reference = TreeEditor::store_empty_file(storage.clone())
        .await
        .map_err(|e| {
            error!("Error storing empty file for {}: {e}", metadata.name);
            std::io::Error::other(format!(
                "Failed to store empty file for {}: {e}",
                metadata.name
            ))
        })?;
    let open_file = into_directory
        .clone()
        .open_file(
            &file_name,
            &empty_file_reference,
            FileCreationMode::create_new(),
        )
        .await
        .map_err(|e| {
            error!("Error opening file {}: {e}", metadata.name);
            std::io::Error::other(format!("Failed to open file {}: {e}", metadata.name))
        })?;
    let write_permission = open_file.get_write_permission();
    // download the file from Dropbox in pieces:
    // Start a download session for the file from Dropbox
    let dropbox_file_path = format!("{}/{}", from_directory, metadata.name);
    info!("Starting download for {}", dropbox_file_path);
    let download_arg = files::DownloadArg::new(dropbox_file_path).with_rev(metadata.rev.clone());
    let file_size = metadata.size;
    let response = match files::download(dropbox_client, &download_arg, None, None).await {
        Ok(res) => res,
        Err(e) => {
            error!("Failed to start download for {}: {e}", metadata.name);
            return Err(std::io::Error::other(format!(
                "Failed to download file {}: {e}",
                metadata.name
            )));
        }
    };

    assert_eq!(Some(file_size), response.content_length);
    let mut total_bytes_read = 0;
    let mut stream = response.body.expect("Failed to get response body");
    loop {
        let remaining_bytes = file_size - total_bytes_read;
        if remaining_bytes == 0 {
            break;
        }
        let chunk_size = std::cmp::min(
            remaining_bytes,
            /*use chunk size preferred by Dogbox for efficiency*/
            TREE_BLOB_MAX_LENGTH as u64,
        ) as usize;
        let mut buffer = vec![0u8; chunk_size];
        stream.read(&mut buffer).await.map_err(|e| {
            error!("Error reading download stream for {}: {e}", metadata.name);
            std::io::Error::other(format!(
                "Failed to read download stream for file {}: {e}",
                metadata.name
            ))
        })?;

        let read_size = buffer.len() as u64;
        assert_eq!(chunk_size as u64, read_size);
        open_file
            .write_bytes(&write_permission, total_bytes_read, Bytes::from(buffer))
            .await
            .map_err(|e| {
                error!("Error writing to file {}: {e}", metadata.name);
                std::io::Error::other(format!("Failed to write to file {}: {e}", metadata.name))
            })?;
        total_bytes_read += read_size;
        assert!(total_bytes_read <= file_size);
    }
    assert_eq!(file_size, open_file.size().await);
    info!("Downloaded {} bytes for {}", file_size, metadata.name);
    Ok(ImportFileOutcome::Success)
}

enum ImportFolderOutcome {
    Success,
    UnsupportedFileName(FileNameError),
}

async fn import_folder(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    metadata: &files::FolderMetadata,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    clock: WallClock,
    empty_directory_reference: &StrongReference,
) -> std::io::Result<ImportFolderOutcome> {
    let folder_name = match FileName::try_from(metadata.name.clone()) {
        Ok(success) => success,
        Err(e) => {
            info!("Unsupported folder name {}: {e}", metadata.name);
            return Ok(ImportFolderOutcome::UnsupportedFileName(e));
        }
    };
    let relative_path = match NormalizedPath::try_from(RelativePath::new(metadata.name.as_str())) {
        Ok(success) => success,
        Err(e) => {
            info!("Unsupported folder name {}: {e}", metadata.name);
            return Ok(ImportFolderOutcome::UnsupportedFileName(e));
        }
    };
    into_directory
        .clone()
        .create_subdirectory(folder_name, empty_directory_reference)
        .await
        .map_err(|e| {
            error!("Error creating subdirectory {}: {e}", metadata.name);
            std::io::Error::other(format!(
                "Failed to create subdirectory {}: {e}",
                metadata.name
            ))
        })?;
    let open_subdirectory = into_directory
        .clone()
        .open_directory(relative_path)
        .await
        .map_err(|e| {
            error!("Error opening subdirectory {}: {e}", metadata.name);
            std::io::Error::other(format!(
                "Failed to open subdirectory {}: {e}",
                metadata.name
            ))
        })?;
    Box::pin(import_directory_impl(
        dropbox_client,
        &format!("{}/{}", from_directory, metadata.name),
        &open_subdirectory,
        storage,
        clock,
        empty_directory_reference,
    ))
    .await?;
    open_subdirectory.request_save().await.map_err(|e| {
        error!("Error saving subdirectory {}: {e}", metadata.name);
        std::io::Error::other(format!(
            "Failed to save subdirectory {}: {e}",
            metadata.name
        ))
    })?;
    Ok(ImportFolderOutcome::Success)
}

async fn import_directory_impl(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    clock: WallClock,
    empty_directory_reference: &StrongReference,
) -> std::io::Result<()> {
    info!("Listing Dropbox directory {}", from_directory);
    let mut list_folder_result = match files::list_folder(
        dropbox_client,
        &files::ListFolderArg::new(from_directory.to_string()).with_recursive(false),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            error!("Error from list_folder: {e}");
            todo!()
        }
    };
    let mut cursor = list_folder_result.cursor;
    loop {
        info!("Directory entries: {}", list_folder_result.entries.len());
        for entry in list_folder_result.entries {
            match entry {
                files::Metadata::Folder(entry) => {
                    info!("Folder entry: {}", entry.name);
                    match import_folder(
                        dropbox_client,
                        from_directory,
                        &entry,
                        into_directory,
                        storage.clone(),
                        clock.clone(),
                        empty_directory_reference,
                    )
                    .await?
                    {
                        ImportFolderOutcome::Success => {
                            info!("Successfully imported folder {}", entry.name);
                        }
                        ImportFolderOutcome::UnsupportedFileName(e) => {
                            // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                            warn!(
                                "Skipping folder {} due to unsupported folder name: {e}",
                                entry.name
                            );
                        }
                    }
                }
                files::Metadata::File(entry) => {
                    info!("File entry: {}", entry.name);
                    match import_file(
                        dropbox_client,
                        from_directory,
                        &entry,
                        into_directory,
                        storage.clone(),
                    )
                    .await?
                    {
                        ImportFileOutcome::Success => {
                            info!("Successfully imported file {}", entry.name);
                        }
                        ImportFileOutcome::UnsupportedFileName(e) => {
                            // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                            warn!(
                                "Skipping file {} due to unsupported file name: {e}",
                                entry.name
                            );
                        }
                    }
                }
                files::Metadata::Deleted(entry) => {
                    info!("Ignoring deleted entry: {:?}", entry);
                }
            }
        }
        if !list_folder_result.has_more {
            break;
        }
        list_folder_result = match files::list_folder_continue(
            dropbox_client,
            &files::ListFolderContinueArg::new(cursor.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(e) => {
                error!("Error from list_folder_continue: {e}");
                todo!()
            }
        };
        if cursor != list_folder_result.cursor {
            warn!(
                "Cursor changed from {} to {}. Normally it doesn't change.",
                cursor, list_folder_result.cursor
            );
        }
        cursor = list_folder_result.cursor;
    }
    Ok(())
}

pub async fn import_directory(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    clock: WallClock,
) -> std::io::Result<Arc<OpenDirectory>> {
    let open_directory = Arc::new(
        OpenDirectory::create_directory(
            PathBuf::new(),
            storage.clone(),
            clock.clone(),
            /*don't know which number would be good*/ 64,
        )
        .await
        .expect("Failed to create root directory in storage"),
    );
    let empty_directory_reference = open_directory.latest_reference();
    import_directory_impl(
        dropbox_client,
        from_directory,
        &open_directory,
        storage,
        clock,
        &empty_directory_reference,
    )
    .await?;
    Ok(open_directory)
}
