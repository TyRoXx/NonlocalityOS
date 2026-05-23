use crate::{
    dropbox_api::{DropboxApi, DropboxFileMetaData, DropboxFolderEntry, Sha256Digest},
    file_cache::{PersistableFileCache, PersistableFileCacheEntry, Sha256CacheKey},
    importer::{import_file, ImportFileOutcome},
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadStoreTree, StoreTree, StrongReference},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use async_trait::async_trait;
use dogbox_tree_editor::{DigestStatus, OpenDirectory, OpenDirectoryStatus, OpenFileStats};
use dropbox_sdk::async_routes::files;
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

struct FakeDropboxApi {}

#[async_trait]
impl DropboxApi for FakeDropboxApi {
    async fn download_file(
        &self,
        _dropbox_file_path: &str,
        _dropbox_file_rev: &files::Rev,
        _dropbox_content_hash: &Sha256Digest,
        _storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        todo!()
    }

    async fn list_folder(
        &self,
        _dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        todo!()
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
    let download_cache = PersistableFileCache::new(download_cache_tree, &*storage);
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
    let dropbox_api = FakeDropboxApi {};
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
