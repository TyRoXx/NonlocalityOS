use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{LoadError, LoadStoreTree, LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;
use cached::Cached;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct LoadCache {
    next: Arc<dyn LoadStoreTree + Send + Sync>,
    entries: Mutex<cached::stores::SizedCache<BlobDigest, HashedTree>>,
}

impl LoadCache {
    pub fn new(next: Arc<dyn LoadStoreTree + Send + Sync>, max_entries: usize) -> Self {
        Self {
            next,
            entries: Mutex::new(cached::stores::SizedCache::with_size(max_entries)),
        }
    }
}

#[async_trait]
impl LoadTree for LoadCache {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<DelayedHashedTree, LoadError> {
        {
            let mut entries_locked = self.entries.lock().await;
            if let Some(found) = entries_locked.cache_get(reference) {
                return Ok(DelayedHashedTree::immediate(found.clone()));
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
                entries_locked.cache_set(*reference, success.clone());
                Ok(DelayedHashedTree::immediate(success))
            }
            None => Err(LoadError::TreeNotFound(*reference)),
        }
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        self.next.approximate_tree_count().await
    }
}

#[async_trait]
impl StoreTree for LoadCache {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        self.next.store_tree(tree).await
    }
}

impl LoadStoreTree for LoadCache {}
