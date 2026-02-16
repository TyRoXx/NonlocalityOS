use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{
        LoadError, LoadStoreTree, LoadTree, StoreError, StoreTree, StrongDelayedHashedTree,
        StrongReference,
    },
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;
use cached::Cached;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
struct CacheValue {
    tree: HashedTree,
    reference: StrongReference,
}

#[derive(Debug)]
pub struct LoadCache {
    next: Arc<dyn LoadStoreTree + Send + Sync>,
    entries: Mutex<cached::stores::SizedCache<BlobDigest, CacheValue>>,
}

impl LoadCache {
    pub fn new(next: Arc<dyn LoadStoreTree + Send + Sync>, max_entries: usize) -> Self {
        Self {
            next,
            entries: Mutex::new(cached::stores::SizedCache::with_size(max_entries)),
        }
    }

    async fn load_tree_impl(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<CacheValue, LoadError> {
        {
            let mut entries_locked = self.entries.lock().await;
            if let Some(found) = entries_locked.cache_get(reference) {
                return Ok(found.clone());
            }
        }
        let loaded = match self.next.load_tree(reference).await {
            Ok(loaded) => loaded,
            Err(err) => return Err(err),
        };
        let maybe_hashed_tree = loaded.hash();
        match maybe_hashed_tree {
            Some(success) => {
                let mut entries_locked = self.entries.lock().await;
                let result = CacheValue {
                    tree: success.hashed_tree().clone(),
                    reference: success.reference().clone(),
                };
                entries_locked.cache_set(*reference, result.clone());
                Ok(result)
            }
            None => Err(LoadError::TreeNotFound(*reference)),
        }
    }
}

#[async_trait]
impl LoadTree for LoadCache {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        self.load_tree_impl(reference).await.map(|v| {
            StrongDelayedHashedTree::new(v.reference, DelayedHashedTree::immediate(v.tree))
        })
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        self.next.approximate_tree_count().await
    }
}

#[async_trait]
impl StoreTree for LoadCache {
    async fn store_tree(
        &self,
        tree: &HashedTree,
    ) -> std::result::Result<StrongReference, StoreError> {
        self.next.store_tree(tree).await
    }
}

impl LoadStoreTree for LoadCache {}
