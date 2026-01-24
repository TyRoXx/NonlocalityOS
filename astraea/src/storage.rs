use crate::tree::{BlobDigest, HashedTree, Tree, TreeSerializationError};
use async_trait::async_trait;
use cached::Cached;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    NoSpace,
    Rusqlite(String),
    TreeSerializationError(TreeSerializationError),
    Unrepresentable,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StoreError {}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LoadError {
    Rusqlite(String),
    TreeNotFound(BlobDigest),
    Deserialization(BlobDigest, TreeSerializationError),
    Inconsistency(BlobDigest, String),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for LoadError {}

#[async_trait::async_trait]
pub trait StoreTree {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError>;
}

// TODO: This enum and the DelayedHashedTree wrapper implement a performance optimization pattern.
// When should "delayed" be used vs "immediate"? What are the trade-offs?
// Is this pattern primarily for avoiding redundant hash calculations when loading from storage?
// Should there be documentation explaining when each variant is appropriate to use?
#[derive(Debug, Clone, PartialEq)]
enum DelayedHashedTreeAlternatives {
    Delayed(Arc<Tree>, BlobDigest),
    Immediate(HashedTree),
}

// TODO: Document this pattern! This appears to be an optimization to defer hash verification.
// When loading from trusted storage, Delayed can skip immediate hashing.
// When creating new trees, Immediate ensures the hash is already computed.
// What are the security implications of trusting the expected_digest in Delayed variant?
#[derive(Debug, Clone, PartialEq)]
pub struct DelayedHashedTree {
    alternatives: DelayedHashedTreeAlternatives,
}

impl DelayedHashedTree {
    pub fn delayed(tree: Arc<Tree>, expected_digest: BlobDigest) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Delayed(tree, expected_digest),
        }
    }

    pub fn immediate(tree: HashedTree) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Immediate(tree),
        }
    }

    //#[instrument(skip_all)]
    // TODO: Why does this return Option instead of Result? What does None signify - hash mismatch?
    // Should hash verification failure be an error type instead of None for better error handling?
    // When hash() returns None for the Delayed variant, is this a security issue or data corruption?
    pub fn hash(self) -> Option<HashedTree> {
        match self.alternatives {
            DelayedHashedTreeAlternatives::Delayed(tree, expected_digest) => {
                let hashed_tree = HashedTree::from(tree);
                if hashed_tree.digest() == &expected_digest {
                    Some(hashed_tree)
                } else {
                    None
                }
            }
            DelayedHashedTreeAlternatives::Immediate(hashed_tree) => Some(hashed_tree),
        }
    }
}

#[async_trait::async_trait]
pub trait LoadTree: std::fmt::Debug {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<DelayedHashedTree, LoadError>;
    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError>;
}

pub trait LoadStoreTree: LoadTree + StoreTree {}

#[async_trait]
pub trait UpdateRoot {
    async fn update_root(
        &self,
        name: &str,
        target: &BlobDigest,
    ) -> std::result::Result<(), StoreError>;
}

#[async_trait]
pub trait LoadRoot {
    async fn load_root(&self, name: &str) -> std::result::Result<Option<BlobDigest>, LoadError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GarbageCollectionStats {
    pub trees_collected: u64,
}

#[async_trait]
pub trait CollectGarbage {
    async fn collect_some_garbage(&self)
        -> std::result::Result<GarbageCollectionStats, StoreError>;
}

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

#[async_trait]
pub trait CommitChanges {
    async fn commit_changes(&self) -> Result<(), rusqlite::Error>;
}
