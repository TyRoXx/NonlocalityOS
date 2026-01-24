use crate::tree::{BlobDigest, HashedTree, Tree, TreeSerializationError};
use async_trait::async_trait;
use std::sync::Arc;

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

#[async_trait]
pub trait CommitChanges {
    async fn commit_changes(&self) -> Result<(), rusqlite::Error>;
}
