use crate::{dropbox_api::DropboxApi, file_cache::FileCache, importer::DropboxImporter};
use astraea::storage::LoadStoreTree;
use dogbox_tree_editor::{OpenDirectory, WallClock, DEFAULT_WRITE_BUFFER_IN_BLOCKS};
use std::{path::PathBuf, sync::Arc};
use tracing::error;

mod dropbox_content_hash;

#[cfg(test)]
mod dropbox_content_hash_tests;

pub mod file_cache;

#[cfg(test)]
mod file_cache_tests;

pub mod dropbox_api;

#[cfg(test)]
mod dropbox_api_tests;

pub mod importer;

#[cfg(test)]
mod importer_tests;

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
