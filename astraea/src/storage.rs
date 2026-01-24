use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    tree::{BlobDigest, HashedTree, TreeSerializationError},
};
use async_trait::async_trait;

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
    async fn commit_changes(&self) -> Result<(), StoreError>;
}
