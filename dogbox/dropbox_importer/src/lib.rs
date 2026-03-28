use astraea::{storage::LoadStoreTree, tree::TREE_BLOB_MAX_LENGTH};
use bytes::Bytes;
use dogbox_tree::serialization::FileName;
use dogbox_tree_editor::{FileCreationMode, OpenDirectory, TreeEditor, WallClock};
use dropbox_sdk::{async_routes::files, default_async_client::UserAuthDefaultClient};
use futures::io::AsyncReadExt;
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

mod lib_tests;

async fn import_file(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    metadata: &files::FileMetadata,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) -> std::io::Result<()> {
    let file_name = FileName::try_from(metadata.name.clone()).map_err(|e| {
        error!("Unsupported file name {}: {e}", metadata.name);
        std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Invalid file name {}: {e}", metadata.name),
        )
    })?;
    let empty_file_reference = TreeEditor::store_empty_file(storage.clone())
        .await
        .map_err(|e| {
            error!("Error storing empty file for {}: {e}", metadata.name);
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to store empty file for {}: {e}", metadata.name),
            )
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
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to open file {}: {e}", metadata.name),
            )
        })?;
    let write_permission = open_file.get_write_permission();
    // download the file from Dropbox in pieces:
    // Start a download session for the file from Dropbox
    let dropbox_file_path = format!("{}/{}", from_directory, metadata.name);
    info!("Starting download for {}", dropbox_file_path);
    let download_arg = files::DownloadArg::new(dropbox_file_path).with_rev(metadata.rev.clone());
    let mut downloaded_until = 0;
    let file_size = metadata.size;
    loop {
        let remaining = file_size - downloaded_until;
        if remaining == 0 {
            info!("Finished downloading file {}", metadata.name);
            break;
        }
        let piece = std::cmp::min(remaining, 64 * TREE_BLOB_MAX_LENGTH as u64);
        let response = match files::download(
            dropbox_client,
            &download_arg,
            Some(downloaded_until),
            Some(downloaded_until + piece),
        )
        .await
        {
            Ok(res) => res,
            Err(e) => {
                error!("Failed to start download for {}: {e}", metadata.name);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to download file {}: {e}", metadata.name),
                ));
            }
        };

        assert_eq!(Some(piece), response.content_length);
        let mut stream = response.body.expect("Failed to get response body");
        let mut buffer = Vec::new();
        stream.read_to_end(&mut buffer).await.map_err(|e| {
            error!("Error reading download stream for {}: {e}", metadata.name);
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to read download stream for file {}: {e}",
                    metadata.name
                ),
            )
        })?;
        assert_eq!(piece, buffer.len() as u64);

        open_file
            .write_bytes(&write_permission, downloaded_until, Bytes::from(buffer))
            .await
            .map_err(|e| {
                error!("Error writing to file {}: {e}", metadata.name);
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to write to file {}: {e}", metadata.name),
                )
            })?;
        downloaded_until += piece;
        info!(
            "Downloaded {} / {} bytes for {}",
            downloaded_until, file_size, metadata.name
        );
    }
    Ok(())
}

pub struct DropboxImporter {}

impl DropboxImporter {
    pub async fn import_directory(
        &self,
        dropbox_client: &UserAuthDefaultClient,
        from_directory: &str,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
        clock: WallClock,
    ) -> std::io::Result<Arc<OpenDirectory>> {
        let open_directory = Arc::new(
            OpenDirectory::create_directory(
                PathBuf::new(),
                storage.clone(),
                clock,
                /*don't know which number would be good*/ 64,
            )
            .await
            .expect("Failed to create root directory in storage"),
        );
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
                    files::Metadata::Folder(_entry) => {
                        todo!()
                    }
                    files::Metadata::File(entry) => {
                        info!("File entry: {}", entry.name);
                        import_file(
                            dropbox_client,
                            from_directory,
                            &entry,
                            &open_directory,
                            storage.clone(),
                        )
                        .await?;
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
        Ok(open_directory)
    }
}
