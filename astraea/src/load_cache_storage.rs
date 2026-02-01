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

#[derive(Debug)]
struct CacheValue {
    tree: HashedTree,
    reference: Option<StrongReference>,
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

    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<CacheValue, LoadError> {
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
                return Ok(DelayedHashedTree::immediate(found.tree.clone()));
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
                entries_locked.cache_set(
                    *reference,
                    CacheValue {
                        tree: success.clone(),
                        reference: None,
                    },
                );
                Ok(DelayedHashedTree::immediate(success))
            }
            None => Err(LoadError::TreeNotFound(*reference)),
        }
    }

    async fn load_tree_v2(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        todo!()
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
