use crate::{
    dropbox_api::{
        join_dropbox_path, parse_sha256_hex, DownloadRequest, DropboxApi, DropboxFileMetaData,
        DropboxFolderEntry, DropboxFolderEntryKind, Sha256Digest,
    },
    dropbox_content_hash::{format_dropbox_content_hash, DropboxContentHasher},
    file_cache::FileCache,
};
use astraea::{
    storage::{LoadStoreTree, LoadTree, StrongReference},
    tree::TREE_BLOB_MAX_LENGTH,
};
use dogbox_tree::serialization::{DeserializationError, FileName, FileNameError};
use dogbox_tree_editor::{
    segmented_blob::{load_segmented_blob, save_segmented_blob, DEFAULT_MAX_CHILDREN_PER_TREE},
    FileCreationMode, NormalizedPath, OpenDirectory, WallClock, DEFAULT_WRITE_BUFFER_IN_BLOCKS,
};
use futures::StreamExt;
use relative_path::RelativePath;
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

#[derive(Debug, PartialEq, Eq)]
pub enum ImportFileOutcome {
    Success,
    UnsupportedFileName(FileNameError),
    MissingContentHash,
    InvalidContentHash(String),
}

async fn hash_segmented_blob(
    hasher: &mut DropboxContentHasher,
    reference: &StrongReference,
    storage: &(dyn LoadTree + Send + Sync),
) -> std::result::Result<(), DeserializationError> {
    let (segments, _) = load_segmented_blob(reference.digest(), storage).await?;
    for segment in segments.iter() {
        let delayed_hashed_tree = storage
            .load_tree(segment.digest())
            .await
            .map_err(DeserializationError::Load)?;
        // TODO: parallelize the hashing of segments to improve performance for large files with many segments.
        let hashed_tree = match delayed_hashed_tree.delayed_tree().clone().hash() {
            Some(success) => success,
            None => return Err(DeserializationError::TreeHashMismatch(*segment.digest())),
        };
        hasher.update(hashed_tree.tree().blob().as_slice());
    }
    Ok(())
}

async fn download_file_in_chunks(
    dropbox_file_path: String,
    rev: &str,
    content_hash: &Sha256Digest,
    file_size: u64,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    dropbox_api: &(dyn DropboxApi + Send + Sync),
    download_cache: &dyn FileCache,
) -> std::io::Result<(StrongReference, u64)> {
    // Chunk size is configurable for testing purposes.
    let max_chunk_size = download_cache.chunk_size();
    assert_ne!(max_chunk_size, 0);
    assert_eq!(max_chunk_size % TREE_BLOB_MAX_LENGTH as u64, 0);
    // DEFAULT_CHUNK_SIZE is pretty large at ~25 MB, so this list shouldn't get too long even for large files.
    let mut chunks = Vec::new();
    let chunk_count = file_size.div_ceil(max_chunk_size);
    info!(
        "Downloading file {} (size: {}) in {} chunks",
        dropbox_file_path, file_size, chunk_count
    );
    let mut dropbox_hasher = DropboxContentHasher::new();
    let mut downloaded_bytes = 0;
    while downloaded_bytes < file_size {
        let chunk_index = (downloaded_bytes / max_chunk_size) as u32;
        let chunk_size = std::cmp::min(max_chunk_size, file_size - downloaded_bytes);
        let chunk_cache_key =
            crate::file_cache::Sha256ChunkCacheKey::new((*content_hash).into(), chunk_index);
        let (chunk_reference, downloaded_chunk_size) = download_cache
            .require(&chunk_cache_key, {
                let dropbox_file_path = dropbox_file_path.clone();
                let dropbox_file_rev = rev.to_string();
                let storage = storage.clone();
                Box::new(move || {
                    Box::pin(async move {
                        dropbox_api
                            .download_file(
                                &dropbox_file_path,
                                &DownloadRequest {
                                    dropbox_rev: dropbox_file_rev,
                                    dropbox_content_hash: *content_hash,
                                    offset: downloaded_bytes,
                                    length_to_download: chunk_size,
                                },
                                storage,
                            )
                            .await
                    })
                })
            })
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to download chunk {} of {}: {e}",
                    chunk_index, dropbox_file_path
                ))
            })?;
        if chunk_size != downloaded_chunk_size {
            return Err(std::io::Error::other(format!(
                "Downloaded chunk size {} does not match expected chunk size {} for chunk {} of {}",
                downloaded_chunk_size, chunk_size, chunk_index, dropbox_file_path
            )));
        }

        hash_segmented_blob(&mut dropbox_hasher, &chunk_reference, storage.as_ref())
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to hash chunk {} of {}: {e}",
                    chunk_index, dropbox_file_path
                ))
            })?;

        downloaded_bytes += downloaded_chunk_size;
        chunks.push(chunk_reference);
    }

    let calculated_dropbox_content_hash: Sha256Digest = dropbox_hasher.finalize();
    if content_hash != &calculated_dropbox_content_hash {
        return Err(std::io::Error::other(format!(
            "Content hash mismatch for file {}: expected {}, got {}",
            dropbox_file_path,
            format_dropbox_content_hash(content_hash),
            format_dropbox_content_hash(&calculated_dropbox_content_hash)
        )));
    }

    let concatenated_reference = save_segmented_blob(
        &chunks,
        file_size,
        DEFAULT_MAX_CHILDREN_PER_TREE,
        storage.as_ref(),
    )
    .await
    .map_err(|e| {
        std::io::Error::other(format!(
            "Failed to save concatenated blob for file {}: {e}",
            dropbox_file_path
        ))
    })?;

    Ok((concatenated_reference, file_size))
}

pub async fn import_file(
    from_directory: &str,
    file_name_raw: &str,
    metadata: &DropboxFileMetaData,
    into_directory: &Arc<OpenDirectory>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    dropbox_api: &(dyn DropboxApi + Send + Sync),
    download_cache: &dyn FileCache,
) -> std::io::Result<ImportFileOutcome> {
    let file_name = match FileName::try_from(file_name_raw) {
        Ok(success) => success,
        Err(e) => {
            info!("Unsupported file name {}: {e}", file_name_raw);
            return Ok(ImportFileOutcome::UnsupportedFileName(e));
        }
    };
    let dropbox_file_path = join_dropbox_path(from_directory, file_name_raw);
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
    let (content_reference, content_size) = download_file_in_chunks(
        dropbox_file_path.clone(),
        &metadata.rev,
        &content_hash,
        metadata.size,
        storage.clone(),
        dropbox_api,
        download_cache,
    )
    .await?;
    let open_file = into_directory
        .clone()
        .open_file(
            &file_name,
            FileCreationMode::create_new(content_reference, content_size),
        )
        .await
        .map_err(|e| {
            error!("Error opening file {}: {e}", file_name);
            std::io::Error::other(format!("Failed to open file {}: {e}", file_name))
        })?;
    open_file.request_save().await.map_err(|e| {
        error!("Error saving file {}: {e}", file_name);
        std::io::Error::other(format!("Failed to save file {}: {e}", file_name))
    })?;
    Ok(ImportFileOutcome::Success)
}

pub enum ImportFolderOutcome {
    Success,
    UnsupportedFileName(FileNameError),
}

pub struct DropboxImporter<'t> {
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    empty_directory_reference: &'t StrongReference,
    dropbox_api: &'t (dyn DropboxApi + Send + Sync),
    download_cache: &'t dyn FileCache,
}

impl<'t> DropboxImporter<'t> {
    pub fn new(
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
        empty_directory_reference: &'t StrongReference,
        dropbox_api: &'t (dyn DropboxApi + Send + Sync),
        download_cache: &'t dyn FileCache,
    ) -> Self {
        Self {
            storage,
            empty_directory_reference,
            dropbox_api,
            download_cache,
        }
    }

    async fn import_directory(
        &self,
        from_directory: &str,
        folder_name_raw: &str,
        into_directory: &Arc<OpenDirectory>,
    ) -> std::io::Result<ImportFolderOutcome> {
        let folder_name = match FileName::try_from(folder_name_raw) {
            Ok(success) => success,
            Err(e) => {
                info!("Unsupported folder name {}: {e}", folder_name_raw);
                return Ok(ImportFolderOutcome::UnsupportedFileName(e));
            }
        };
        let relative_path = match NormalizedPath::try_from(RelativePath::new(folder_name_raw)) {
            Ok(success) => success,
            Err(e) => {
                info!("Unsupported folder name {}: {e}", folder_name_raw);
                return Ok(ImportFolderOutcome::UnsupportedFileName(e));
            }
        };
        into_directory
            .clone()
            .create_subdirectory(folder_name, self.empty_directory_reference)
            .await
            .map_err(|e| {
                error!("Error creating subdirectory {}: {e}", folder_name_raw);
                std::io::Error::other(format!(
                    "Failed to create subdirectory {}: {e}",
                    folder_name_raw
                ))
            })?;
        let open_subdirectory = into_directory
            .clone()
            .open_directory(relative_path)
            .await
            .map_err(|e| {
                error!("Error opening subdirectory {}: {e}", folder_name_raw);
                std::io::Error::other(format!(
                    "Failed to open subdirectory {}: {e}",
                    folder_name_raw
                ))
            })?;
        Box::pin(self.import_directory_impl(
            &join_dropbox_path(from_directory, folder_name_raw),
            &open_subdirectory,
        ))
        .await?;
        Ok(ImportFolderOutcome::Success)
    }

    pub async fn import_directory_impl(
        &self,
        from_directory: &str,
        into_directory: &Arc<OpenDirectory>,
    ) -> std::io::Result<()> {
        info!("Listing Dropbox directory {}", from_directory);
        let mut folder_entries = self.dropbox_api.list_folder(from_directory).await?;
        while let Some(entry_result) = folder_entries.next().await {
            let entry = entry_result?;
            self.import_directory_entry(from_directory, &entry, into_directory)
                .await?;
        }
        Ok(())
    }

    pub async fn import_directory_entry(
        &self,
        from_directory: &str,
        entry: &DropboxFolderEntry,
        into_directory: &Arc<OpenDirectory>,
    ) -> std::io::Result<()> {
        match &entry.kind {
            DropboxFolderEntryKind::Folder => {
                info!("Folder entry: {}", entry.name);
                match self
                    .import_directory(from_directory, &entry.name, into_directory)
                    .await?
                {
                    ImportFolderOutcome::Success => {
                        info!("Successfully imported folder {}", entry.name);
                        Ok(())
                    }
                    ImportFolderOutcome::UnsupportedFileName(e) => {
                        // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                        warn!(
                            "Skipping folder {} due to unsupported folder name: {e}",
                            entry.name
                        );
                        Ok(())
                    }
                }
            }
            DropboxFolderEntryKind::File { metadata } => {
                info!("File entry: {}", entry.name);
                match import_file(
                    from_directory,
                    &entry.name,
                    metadata,
                    into_directory,
                    self.storage.clone(),
                    self.dropbox_api,
                    self.download_cache,
                )
                .await?
                {
                    ImportFileOutcome::Success => {
                        info!("Successfully imported file {}", entry.name);
                        Ok(())
                    }
                    ImportFileOutcome::UnsupportedFileName(e) => {
                        // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                        warn!(
                            "Skipping file {} due to unsupported file name: {e}",
                            entry.name
                        );
                        Ok(())
                    }
                    ImportFileOutcome::MissingContentHash => {
                        // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                        warn!("Skipping file {} due to missing content hash", entry.name);
                        Ok(())
                    }
                    ImportFileOutcome::InvalidContentHash(content_hash_string) => {
                        // TODO: return this information somehow to the caller so that they can decide what to do with it (e.g. show a warning to the user)
                        warn!(
                            "Skipping file {} due to invalid content hash: {}",
                            entry.name, content_hash_string
                        );
                        Ok(())
                    }
                }
            }
        }
    }
}

pub async fn import_directory(
    from_directory: &str,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    clock: WallClock,
    dropbox_api: &(dyn DropboxApi + Send + Sync),
    download_cache: &dyn FileCache,
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
    let importer = DropboxImporter::new(
        storage,
        &empty_directory_reference,
        dropbox_api,
        download_cache,
    );
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
