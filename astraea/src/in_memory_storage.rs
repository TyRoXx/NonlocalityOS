use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{LoadError, LoadStoreTree, LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct InMemoryTreeStorage {
    reference_to_tree: Mutex<BTreeMap<BlobDigest, HashedTree>>,
}

impl InMemoryTreeStorage {
    pub fn new(reference_to_tree: Mutex<BTreeMap<BlobDigest, HashedTree>>) -> InMemoryTreeStorage {
        InMemoryTreeStorage { reference_to_tree }
    }

    pub fn empty() -> InMemoryTreeStorage {
        Self {
            reference_to_tree: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn clear(&self) {
        self.reference_to_tree.lock().await.clear();
    }

    pub async fn number_of_trees(&self) -> usize {
        self.reference_to_tree.lock().await.len()
    }

    pub async fn digests(&self) -> BTreeSet<BlobDigest> {
        self.reference_to_tree
            .lock()
            .await
            .keys()
            .copied()
            .collect()
    }
}

#[async_trait]
impl StoreTree for InMemoryTreeStorage {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        let mut lock = self.reference_to_tree.lock().await;
        let reference = *tree.digest();
        lock.entry(reference).or_insert_with(|| tree.clone());
        Ok(reference)
    }
}

#[async_trait]
impl LoadTree for InMemoryTreeStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<DelayedHashedTree, LoadError> {
        let lock = self.reference_to_tree.lock().await;
        match lock.get(reference) {
            Some(found) => Ok(DelayedHashedTree::immediate(found.clone())),
            None => return Err(LoadError::TreeNotFound(*reference)),
        }
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        let lock = self.reference_to_tree.lock().await;
        Ok(lock.len() as u64)
    }
}

impl LoadStoreTree for InMemoryTreeStorage {}
