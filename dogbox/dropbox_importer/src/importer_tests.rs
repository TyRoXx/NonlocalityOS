use crate::{
    dropbox_api::{
        DropboxApi, DropboxFileMetaData, DropboxFolderEntry, DropboxFolderEntryKind, Sha256Digest,
    },
    file_cache::{FileCacheMap, PersistableFileCacheEntry, Sha256CacheKey},
    importer::{import_file, DropboxImporter, ImportFileOutcome},
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadStoreTree, StoreTree, StrongReference},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use async_trait::async_trait;
use dogbox_tree_editor::{DigestStatus, OpenDirectory, OpenDirectoryStatus, OpenFileStats};
use dropbox_sdk::async_routes::files;
use futures::StreamExt;
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

struct UnreachableDropboxApi {}

#[async_trait]
impl DropboxApi for UnreachableDropboxApi {
    async fn download_file(
        &self,
        _dropbox_file_path: &str,
        _dropbox_file_rev: &files::Rev,
        _dropbox_content_hash: &Sha256Digest,
        _storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        unreachable!()
    }

    async fn list_folder(
        &self,
        _dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        unreachable!()
    }
}

struct FailingDropboxApi {}

#[async_trait]
impl DropboxApi for FailingDropboxApi {
    async fn download_file(
        &self,
        _dropbox_file_path: &str,
        _dropbox_file_rev: &files::Rev,
        _dropbox_content_hash: &Sha256Digest,
        _storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        Err(std::io::Error::other("Simulated download failure"))
    }

    async fn list_folder(
        &self,
        _dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        Err(std::io::Error::other("Simulated folder listing failure"))
    }
}

#[test_log::test(tokio::test)]
async fn test_import_file_missing_content_hash() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let original_content_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let modified = clock();
    let open_directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(original_content_reference.clone(), false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        clock,
        1,
    ));
    let dropbox_api = UnreachableDropboxApi {};
    let outcome = import_file(
        "/test",
        "file.txt",
        &DropboxFileMetaData {
            content_hash: None,
            rev: "1".to_string(),
        },
        &open_directory,
        storage.clone(),
        &dropbox_api,
        &download_cache,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ImportFileOutcome::MissingContentHash);
    let status = open_directory
        .request_save()
        .await
        .expect("Failed to save directory");
    // the digest doesn't really matter here
    let new_reference = status.digest.last_known_digest.clone();
    assert_eq!(
        status,
        OpenDirectoryStatus::new(
            DigestStatus::new(new_reference, true),
            1,
            0,
            OpenFileStats::new(0, 0, 0, 0, 0),
            modified,
        ),
    )
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let original_content_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let modified = clock();
    let open_directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(original_content_reference.clone(), false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        clock,
        1,
    ));
    let dropbox_api = FailingDropboxApi {};
    let importer = DropboxImporter::new(
        storage.clone(),
        &original_content_reference,
        &dropbox_api,
        &download_cache,
    );
    let error = importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(
                            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                                .to_string(),
                        ),
                        rev: "1".to_string(),
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Failed to download /file.txt: Simulated download failure"
    );
    let mut entries = open_directory.read().await;
    if entries.next().await.is_some() {
        panic!("Unexpected directory entry")
    }
}
