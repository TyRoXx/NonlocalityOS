use crate::dropbox_content_hash::DropboxContentHasher;
use astraea::{
    storage::{LoadStoreTree, StrongReference},
    tree::TREE_BLOB_MAX_LENGTH,
};
use async_trait::async_trait;
use bytes::Bytes;
use dogbox_tree::serialization::{FileName, FileNameError};
use dogbox_tree_editor::{
    FileCreationMode, NormalizedPath, OpenDirectory, OpenFileContentBuffer, OptimizedWriteBuffer,
    TreeEditor, WallClock, DEFAULT_WRITE_BUFFER_IN_BLOCKS,
};
use dropbox_sdk::{async_routes::files, default_async_client::UserAuthDefaultClient};
use futures::io::AsyncReadExt;
use relative_path::RelativePath;
use sha2::Sha256;
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

#[cfg(test)]
mod lib_tests;

mod dropbox_content_hash;

#[cfg(test)]
mod dropbox_content_hash_tests;

pub type Sha256Digest = sha2::digest::Output<Sha256>;

pub fn parse_sha256_hex(content_hash_string: &str) -> Option<Sha256Digest> {
    match hex::decode(content_hash_string) {
        Ok(success) => Sha256Digest::from_exact_iter(success.iter().copied()),
        Err(error) => {
            info!("Failed to decode hex string: {}", error);
            None
        }
    }
}

// TODO: an implementation with caching using dropbox_content_hash as the cache key
#[async_trait]
pub trait DownloadDropboxFile {
    async fn download_dropbox_file(
        &self,
        dropbox_client: &UserAuthDefaultClient,
        dropbox_file_path: &str,
        dropbox_file_rev: &files::Rev,
        dropbox_content_hash: &Sha256Digest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)>;
}

pub struct NonCachingDropboxFileDownloader;

#[async_trait]
impl DownloadDropboxFile for NonCachingDropboxFileDownloader {
    async fn download_dropbox_file(
        &self,
        dropbox_client: &UserAuthDefaultClient,
        dropbox_file_path: &str,
        dropbox_file_rev: &files::Rev,
        dropbox_content_hash: &Sha256Digest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        // download the file from Dropbox in pieces:
        // Start a download session for the file from Dropbox
        info!("Starting download for {}", dropbox_file_path);
        let download_arg = files::DownloadArg::new(dropbox_file_path.to_string())
            .with_rev(dropbox_file_rev.clone());
        let response = match files::download(dropbox_client, &download_arg, None, None).await {
            Ok(res) => res,
            Err(e) => {
                return Err(std::io::Error::other(format!(
                    "Failed to download file {}: {e}",
                    dropbox_file_path
                )));
            }
        };

        let file_size = match response.content_length {
            Some(size) => size,
            None => {
                return Err(std::io::Error::other(format!(
                    "Content length is missing for file {}",
                    dropbox_file_path
                )));
            }
        };

        let empty_file_reference = TreeEditor::store_empty_file(storage.clone())
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to store empty file for {}: {e}",
                    dropbox_file_path
                ))
            })?;
        let mut open_file_content_buffer = OpenFileContentBuffer::NotLoaded {
            reference: empty_file_reference,
            size: 0,
            write_buffer_in_blocks: DEFAULT_WRITE_BUFFER_IN_BLOCKS,
        };
        let mut dropbox_hasher = DropboxContentHasher::new();
        let mut total_bytes_read = 0;
        let mut stream = match response.body {
            Some(stream) => stream,
            None => {
                return Err(std::io::Error::other(format!(
                    "Failed to get response body for file {}",
                    dropbox_file_path
                )));
            }
        };
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
            let bytes_read = stream.read(&mut buffer).await.map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to read download stream for file {}: {e}",
                    dropbox_file_path
                ))
            })?;
            if bytes_read == 0 {
                return Err(std::io::Error::other(format!(
                    "Unexpected end of stream while downloading file {}: expected {} bytes, got {} bytes",
                    dropbox_file_path, file_size, total_bytes_read)));
            }
            buffer.truncate(bytes_read);

            dropbox_hasher.update(&buffer);

            let read_size = buffer.len() as u64;
            assert!(read_size <= chunk_size as u64);
            open_file_content_buffer
                .write(
                    total_bytes_read,
                    OptimizedWriteBuffer::from_bytes(total_bytes_read, Bytes::from(buffer)).await,
                    storage.clone(),
                )
                .await
                .map_err(|e| {
                    std::io::Error::other(format!(
                        "Failed to write to file {}: {e}",
                        dropbox_file_path
                    ))
                })?;

            total_bytes_read += read_size;
            assert!(total_bytes_read <= file_size);
        }

        // we should never break the loop unless the buffer is completely filled
        assert_eq!(file_size, open_file_content_buffer.size());

        info!("Downloaded {} bytes for {}", file_size, dropbox_file_path);

        let calculated_dropbox_content_hash: Sha256Digest = dropbox_hasher.finalize();
        if dropbox_content_hash != &calculated_dropbox_content_hash {
            return Err(std::io::Error::other(format!(
                "Content hash mismatch for file {}: expected {}, got {}",
                dropbox_file_path,
                hex::encode(dropbox_content_hash),
                hex::encode(calculated_dropbox_content_hash)
            )));
        }

        open_file_content_buffer
            .store_all(storage)
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to store content buffer for file {}: {e}",
                    dropbox_file_path
                ))
            })?;
        let (digest_status, size, reference) = open_file_content_buffer.last_known_digest();
        assert_eq!(file_size, size);
        assert!(digest_status.is_digest_up_to_date);
        Ok((reference, file_size))
    }
}

enum ImportFileOutcome {
    Success,
    UnsupportedFileName(FileNameError),
    MissingContentHash,
    InvalidContentHash(String),
}

pub fn join_dropbox_path(parent: &str, child: &str) -> String {
    let child = child.trim_start_matches('/');
    let parent = parent.trim_end_matches('/');
    if parent.is_empty() {
        format!("/{}", child)
    } else {
        format!("{parent}/{child}")
    }
}

async fn import_file(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    metadata: &files::FileMetadata,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    download: &dyn DownloadDropboxFile,
) -> std::io::Result<ImportFileOutcome> {
    let file_name = match FileName::try_from(metadata.name.clone()) {
        Ok(success) => success,
        Err(e) => {
            info!("Unsupported file name {}: {e}", metadata.name);
            return Ok(ImportFileOutcome::UnsupportedFileName(e));
        }
    };
    let dropbox_file_path = join_dropbox_path(from_directory, &metadata.name);
    let content_hash: Sha256Digest = match &metadata.content_hash {
        Some(content_hash_string) => match parse_sha256_hex(content_hash_string) {
            Some(hash) => hash,
            None => {
                info!(
                    "Invalid content hash for file {}: {}",
                    dropbox_file_path, content_hash_string
                );
                return Ok(ImportFileOutcome::InvalidContentHash(
                    content_hash_string.clone(),
                ));
            }
        },
        None => {
            info!("Content hash missing for file {}", dropbox_file_path);
            return Ok(ImportFileOutcome::MissingContentHash);
        }
    };
    let (content_reference, content_size) = download
        .download_dropbox_file(
            dropbox_client,
            &dropbox_file_path,
            &metadata.rev,
            &content_hash,
            storage.clone(),
        )
        .await
        .map_err(|e| {
            error!("Error downloading {}: {e}", metadata.name);
            std::io::Error::other(format!("Failed to download {}: {e}", metadata.name))
        })?;
    let open_file = into_directory
        .clone()
        .open_file(
            &file_name,
            FileCreationMode::create_new(content_reference, content_size),
        )
        .await
        .map_err(|e| {
            error!("Error opening file {}: {e}", metadata.name);
            std::io::Error::other(format!("Failed to open file {}: {e}", metadata.name))
        })?;
    open_file.request_save().await.map_err(|e| {
        error!("Error saving file {}: {e}", metadata.name);
        std::io::Error::other(format!("Failed to save file {}: {e}", metadata.name))
    })?;
    Ok(ImportFileOutcome::Success)
}

enum ImportFolderOutcome {
    Success,
    UnsupportedFileName(FileNameError),
}

struct DropboxImporter<'t> {
    dropbox_client: &'t UserAuthDefaultClient,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    empty_directory_reference: &'t StrongReference,
    download: &'t dyn DownloadDropboxFile,
}

impl<'t> DropboxImporter<'t> {
    async fn import_folder(
        &self,
        from_directory: &str,
        metadata: &files::FolderMetadata,
        into_directory: &Arc<OpenDirectory>,
    ) -> std::io::Result<ImportFolderOutcome> {
        let folder_name = match FileName::try_from(metadata.name.clone()) {
            Ok(success) => success,
            Err(e) => {
                info!("Unsupported folder name {}: {e}", metadata.name);
                return Ok(ImportFolderOutcome::UnsupportedFileName(e));
            }
        };
        let relative_path =
            match NormalizedPath::try_from(RelativePath::new(metadata.name.as_str())) {
                Ok(success) => success,
                Err(e) => {
                    info!("Unsupported folder name {}: {e}", metadata.name);
                    return Ok(ImportFolderOutcome::UnsupportedFileName(e));
                }
            };
        into_directory
            .clone()
            .create_subdirectory(folder_name, self.empty_directory_reference)
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
        Box::pin(self.import_directory_impl(
            &join_dropbox_path(from_directory, &metadata.name),
            &open_subdirectory,
        ))
        .await?;
        Ok(ImportFolderOutcome::Success)
    }

    async fn import_directory_impl(
        &self,
        from_directory: &str,
        into_directory: &Arc<OpenDirectory>,
    ) -> std::io::Result<()> {
        info!("Listing Dropbox directory {}", from_directory);
        let mut list_folder_result = match files::list_folder(
            self.dropbox_client,
            &files::ListFolderArg::new(from_directory.to_string()).with_recursive(false),
        )
        .await
        {
            Ok(result) => result,
            Err(e) => {
                return Err(std::io::Error::other(format!(
                    "Failed to list_folder {}: {e}",
                    from_directory
                )));
            }
        };
        let mut cursor = list_folder_result.cursor;
        loop {
            info!("Directory entries: {}", list_folder_result.entries.len());
            for entry in list_folder_result.entries {
                match entry {
                    files::Metadata::Folder(entry) => {
                        info!("Folder entry: {}", entry.name);
                        match self
                            .import_folder(from_directory, &entry, into_directory)
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
                            self.dropbox_client,
                            from_directory,
                            &entry,
                            into_directory,
                            self.storage.clone(),
                            self.download,
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
                            ImportFileOutcome::MissingContentHash => {
                                // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                                warn!("Skipping file {} due to missing content hash", entry.name);
                            }
                            ImportFileOutcome::InvalidContentHash(content_hash_string) => {
                                // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                                warn!(
                                    "Skipping file {} due to invalid content hash: {}",
                                    entry.name, content_hash_string
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
                self.dropbox_client,
                &files::ListFolderContinueArg::new(cursor.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(e) => {
                    error!("Error from list_folder_continue: {e}");
                    return Err(std::io::Error::other(format!(
                        "Failed to list_folder_continue {}: {e}",
                        from_directory
                    )));
                }
            };
            if cursor != list_folder_result.cursor {
                warn!("Dropbox list_folder_continue cursor changed. Normally it doesn't change.");
            }
            cursor = list_folder_result.cursor;
        }
        Ok(())
    }
}

pub async fn import_directory(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    clock: WallClock,
    download: &dyn DownloadDropboxFile,
) -> std::io::Result<Arc<OpenDirectory>> {
    let open_directory = Arc::new(
        OpenDirectory::create_directory(
            PathBuf::new(),
            storage.clone(),
            clock.clone(),
            DEFAULT_WRITE_BUFFER_IN_BLOCKS,
        )
        .await
        .map_err(|e| {
            error!("Failed to create root directory in storage: {e}");
            std::io::Error::other(format!("Failed to create root directory in storage: {e}"))
        })?,
    );
    let empty_directory_reference = open_directory.latest_reference();
    let importer = DropboxImporter {
        dropbox_client,
        storage,
        empty_directory_reference: &empty_directory_reference,
        download,
    };
    importer
        .import_directory_impl(from_directory, &open_directory)
        .await?;
    open_directory.request_save().await.map_err(|e| {
        std::io::Error::other(format!(
            "Failed to save directory imported from {}: {e}",
            from_directory
        ))
    })?;
    Ok(open_directory)
}
