use crate::{
    dropbox_api::{
        join_dropbox_path, parse_sha256_hex, DropboxApi, DropboxFileMetaData, DropboxFolderEntry,
        DropboxFolderEntryKind, Sha256Digest,
    },
    file_cache::FileCache,
};
use astraea::storage::{LoadStoreTree, StrongReference};
use dogbox_tree::serialization::{FileName, FileNameError};
use dogbox_tree_editor::{FileCreationMode, NormalizedPath, OpenDirectory};
use futures::StreamExt;
use relative_path::RelativePath;
use std::sync::Arc;
use tracing::{error, info, warn};

#[derive(Debug, PartialEq, Eq)]
pub enum ImportFileOutcome {
    Success,
    UnsupportedFileName(FileNameError),
    MissingContentHash,
    InvalidContentHash(String),
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
    let (content_reference, content_size) = download_cache
        .require(&content_hash, {
            let dropbox_file_path = dropbox_file_path.clone();
            let dropbox_file_rev = metadata.rev.clone();
            let storage = storage.clone();
            Box::new(move || {
                Box::pin(async move {
                    dropbox_api
                        .download_file(
                            &dropbox_file_path,
                            &dropbox_file_rev,
                            &content_hash,
                            storage,
                        )
                        .await
                })
            })
        })
        .await
        .map_err(|e| {
            error!("Error downloading {}: {e}", dropbox_file_path);
            std::io::Error::other(format!("Failed to download {}: {e}", dropbox_file_path))
        })?;
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

    async fn import_folder(
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
                    .import_folder(from_directory, &entry.name, into_directory)
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
