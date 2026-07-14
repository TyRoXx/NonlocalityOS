use crate::file_cache::{
    FileCache, FileCacheMap, PersistentFileCacheMap, Sha256ChunkCacheKey, DEFAULT_CHUNK_SIZE,
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{StoreError, StoreTree, StrongReference, UpdateRoot},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use pretty_assertions::assert_eq;
use std::{pin::Pin, sync::Arc};

#[test_log::test(tokio::test)]
async fn test_require_miss_and_hit() {
    let storage = InMemoryTreeStorage::empty();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let cache = FileCacheMap::new(
        sorted_tree::prolly_tree_editable_node::EditableNode::new(),
        &storage,
        chunk_size,
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::new()).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let length = 0u64;
    let chunk_cache_key = Sha256ChunkCacheKey::new([0u8; 32], 0);
    let download_file_counter = Arc::new(tokio::sync::Mutex::new(0));
    let make_download_file = || {
        let reference = reference.clone();
        let download_file_counter = download_file_counter.clone();
        Box::new(move || {
            Box::pin(async move {
                let mut counter = download_file_counter.lock().await;
                *counter += 1;
                Ok((reference, length))
            }) as Pin<Box<dyn std::future::Future<Output = Result<(_, u64), _>> + Send>>
        })
    };
    assert_eq!(0, cache.number_of_entries().await.unwrap());
    assert_eq!(0, *download_file_counter.lock().await);
    // cache miss
    {
        let (result_reference, result_length) = cache
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, cache.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }
    // cache hit
    {
        let (result_reference, result_length) = cache
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, cache.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }
}

#[test_log::test(tokio::test)]
async fn test_download_error() {
    let storage = InMemoryTreeStorage::empty();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let cache = FileCacheMap::new(
        sorted_tree::prolly_tree_editable_node::EditableNode::new(),
        &storage,
        chunk_size,
    );
    let chunk_cache_key = Sha256ChunkCacheKey::new([0u8; 32], 0);
    let download_file_counter = Arc::new(tokio::sync::Mutex::new(0));
    let make_download_file = || {
        let download_file_counter = download_file_counter.clone();
        Box::new(move || {
            Box::pin(async move {
                let mut counter = download_file_counter.lock().await;
                *counter += 1;
                Err(std::io::Error::other("simulated download error"))
            }) as Pin<Box<dyn std::future::Future<Output = Result<(_, u64), _>> + Send>>
        })
    };
    assert_eq!(0, cache.number_of_entries().await.unwrap());
    assert_eq!(0, *download_file_counter.lock().await);
    // download will fail
    let error = cache
        .require(&chunk_cache_key, make_download_file())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert_eq!(error.to_string(), "simulated download error");
    assert_eq!(0, cache.number_of_entries().await.unwrap());
    assert_eq!(1, *download_file_counter.lock().await);
}

#[test_log::test(tokio::test)]
async fn test_save_and_load_empty() {
    let storage = InMemoryTreeStorage::empty();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let original_cache = FileCacheMap::new(
        sorted_tree::prolly_tree_editable_node::EditableNode::new(),
        &storage,
        chunk_size,
    );
    let cache_saved_reference = original_cache.save(&storage).await.unwrap();
    assert_eq!(
        cache_saved_reference.digest(),
        &BlobDigest::parse_hex_string(concat!(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a27",
            "01a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ))
        .unwrap()
    );
    let cache_loaded = FileCacheMap::load(&cache_saved_reference, &storage, chunk_size)
        .await
        .unwrap();
    assert_eq!(0, cache_loaded.number_of_entries().await.unwrap());
}

#[test_log::test(tokio::test)]
async fn test_save_and_load_non_empty() {
    let storage = InMemoryTreeStorage::empty();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let original_cache = FileCacheMap::new(
        sorted_tree::prolly_tree_editable_node::EditableNode::new(),
        &storage,
        chunk_size,
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::new()).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let length = 0u64;
    let chunk_cache_key = Sha256ChunkCacheKey::new([0u8; 32], 0);
    let download_file_counter = Arc::new(tokio::sync::Mutex::new(0));
    let make_download_file = || {
        let reference = reference.clone();
        let download_file_counter = download_file_counter.clone();
        Box::new(move || {
            Box::pin(async move {
                let mut counter = download_file_counter.lock().await;
                *counter += 1;
                Ok((reference, length))
            }) as Pin<Box<dyn std::future::Future<Output = Result<(_, u64), _>> + Send>>
        })
    };
    assert_eq!(0, original_cache.number_of_entries().await.unwrap());
    assert_eq!(0, *download_file_counter.lock().await);
    // add a cache entry
    {
        let (result_reference, result_length) = original_cache
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, original_cache.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }
    let cache_saved_reference = original_cache.save(&storage).await.unwrap();
    assert_eq!(
        cache_saved_reference.digest(),
        &BlobDigest::parse_hex_string(concat!(
            "4b2bd26620b490261ff1e76b42d5504aa11f9bdfdaea670e871883460765b799",
            "4df19e9dbb9661549e19755f76bd6b4bd1a4802454ddeca0e4da3a2ffc7474ae"
        ))
        .unwrap()
    );
    let cache_loaded = FileCacheMap::load(&cache_saved_reference, &storage, chunk_size)
        .await
        .unwrap();
    assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
    // the entry should still exist
    {
        let (result_reference, result_length) = cache_loaded
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }
}

struct PersistentSaveAndLoadNonEmptyUpdateRoot {
    current_value: tokio::sync::Mutex<Option<StrongReference>>,
}

impl PersistentSaveAndLoadNonEmptyUpdateRoot {
    pub fn new() -> Self {
        Self {
            current_value: tokio::sync::Mutex::new(None),
        }
    }

    pub async fn current_value(&self) -> Option<StrongReference> {
        self.current_value.lock().await.clone()
    }
}

#[async_trait]
impl UpdateRoot for PersistentSaveAndLoadNonEmptyUpdateRoot {
    async fn update_root(
        &self,
        name: &str,
        target: &StrongReference,
    ) -> std::result::Result<(), StoreError> {
        assert_eq!(name, "test_root");
        let mut current_value = self.current_value.lock().await;
        *current_value = Some(target.clone());
        Ok(())
    }
}

#[test_log::test(tokio::test)]
async fn test_persistent_save_and_load_non_empty() {
    let storage = InMemoryTreeStorage::empty();
    let update_root = PersistentSaveAndLoadNonEmptyUpdateRoot::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let original_cache = PersistentFileCacheMap::new(
        FileCacheMap::new(
            sorted_tree::prolly_tree_editable_node::EditableNode::new(),
            &storage,
            chunk_size,
        ),
        &storage,
        &update_root,
        "test_root".to_string(),
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::new()).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let length = 0u64;
    let chunk_cache_key = Sha256ChunkCacheKey::new([0u8; 32], 0);
    let download_file_counter = Arc::new(tokio::sync::Mutex::new(0));
    let make_download_file = || {
        let reference = reference.clone();
        let download_file_counter = download_file_counter.clone();
        Box::new(move || {
            Box::pin(async move {
                let mut counter = download_file_counter.lock().await;
                *counter += 1;
                Ok((reference, length))
            }) as Pin<Box<dyn std::future::Future<Output = Result<(_, u64), _>> + Send>>
        })
    };
    assert_eq!(0, original_cache.number_of_entries().await.unwrap());
    assert_eq!(0, *download_file_counter.lock().await);
    assert_eq!(None, update_root.current_value().await);
    // add a cache entry
    {
        let (result_reference, result_length) = original_cache
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, original_cache.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }
    let cache_saved_reference = update_root.current_value().await.unwrap();
    assert_eq!(
        cache_saved_reference.digest(),
        &BlobDigest::parse_hex_string(concat!(
            "4b2bd26620b490261ff1e76b42d5504aa11f9bdfdaea670e871883460765b799",
            "4df19e9dbb9661549e19755f76bd6b4bd1a4802454ddeca0e4da3a2ffc7474ae"
        ))
        .unwrap()
    );
    let cache_loaded = PersistentFileCacheMap::new(
        FileCacheMap::load(&cache_saved_reference, &storage, chunk_size)
            .await
            .unwrap(),
        &storage,
        &update_root,
        "test_root".to_string(),
    );
    assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
    // the entry should still exist
    {
        let (result_reference, result_length) = cache_loaded
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
        assert_eq!(
            update_root.current_value().await,
            Some(cache_saved_reference)
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_persistent_file_cache_require_same_chunk_multiple_times() {
    // This test is to ensure that if multiple concurrent calls to require() are made for the same chunk,
    // only one of them will actually download the chunk, and the others will wait for it to finish and then return the same result.
    let storage = InMemoryTreeStorage::empty();
    let update_root = PersistentSaveAndLoadNonEmptyUpdateRoot::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let original_cache = PersistentFileCacheMap::new(
        FileCacheMap::new(
            sorted_tree::prolly_tree_editable_node::EditableNode::new(),
            &storage,
            chunk_size,
        ),
        &storage,
        &update_root,
        "test_root".to_string(),
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::new()).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let length = 0u64;
    let chunk_cache_key = Sha256ChunkCacheKey::new([0u8; 32], 0);
    let download_file_counter = Arc::new(tokio::sync::Mutex::new(0));
    let make_download_file = || {
        let reference = reference.clone();
        let download_file_counter = download_file_counter.clone();
        Box::new(move || {
            Box::pin(async move {
                let mut counter = download_file_counter.lock().await;
                *counter += 1;
                Ok((reference, length))
            }) as Pin<Box<dyn std::future::Future<Output = Result<(_, u64), _>> + Send>>
        })
    };
    assert_eq!(0, original_cache.number_of_entries().await.unwrap());
    assert_eq!(0, *download_file_counter.lock().await);
    assert_eq!(None, update_root.current_value().await);
    const CONCURRENCY: usize = 10;
    join_all((0..CONCURRENCY).map(|_| async {
        let (result_reference, result_length) = original_cache
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, original_cache.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
    }))
    .await;
    let cache_saved_reference = update_root.current_value().await.unwrap();
    assert_eq!(
        cache_saved_reference.digest(),
        &BlobDigest::parse_hex_string(concat!(
            "4b2bd26620b490261ff1e76b42d5504aa11f9bdfdaea670e871883460765b799",
            "4df19e9dbb9661549e19755f76bd6b4bd1a4802454ddeca0e4da3a2ffc7474ae"
        ))
        .unwrap()
    );
    let cache_loaded = PersistentFileCacheMap::new(
        FileCacheMap::load(&cache_saved_reference, &storage, chunk_size)
            .await
            .unwrap(),
        &storage,
        &update_root,
        "test_root".to_string(),
    );
    assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
    {
        let (result_reference, result_length) = cache_loaded
            .require(&chunk_cache_key, make_download_file())
            .await
            .unwrap();
        assert_eq!(result_reference, reference);
        assert_eq!(result_length, length);
        assert_eq!(1, cache_loaded.number_of_entries().await.unwrap());
        assert_eq!(1, *download_file_counter.lock().await);
        assert_eq!(
            update_root.current_value().await,
            Some(cache_saved_reference)
        );
    }
}
