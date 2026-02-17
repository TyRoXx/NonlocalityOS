use astraea::{
    storage::{
        CollectGarbage, CommitChanges, GarbageCollectionStats, LoadError, LoadRoot, LoadTree,
        StoreError, StoreTree, StrongDelayedHashedTree, StrongReference, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct ShardedStorage {}

#[async_trait]
impl LoadTree for ShardedStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        unimplemented!()
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        unimplemented!()
    }
}

#[async_trait]
impl StoreTree for ShardedStorage {
    async fn store_tree(
        &self,
        tree: &HashedTree,
    ) -> std::result::Result<StrongReference, StoreError> {
        unimplemented!()
    }
}

#[async_trait]
impl UpdateRoot for ShardedStorage {
    async fn update_root(
        &self,
        name: &str,
        target: &StrongReference,
    ) -> std::result::Result<(), StoreError> {
        unimplemented!()
    }
}

#[async_trait]
impl LoadRoot for ShardedStorage {
    async fn load_root(
        &self,
        name: &str,
    ) -> std::result::Result<Option<StrongReference>, LoadError> {
        unimplemented!()
    }
}

#[async_trait]
impl CollectGarbage for ShardedStorage {
    async fn collect_some_garbage(
        &self,
    ) -> std::result::Result<GarbageCollectionStats, StoreError> {
        unimplemented!()
    }
}

#[async_trait]
impl CommitChanges for ShardedStorage {
    async fn commit_changes(&self) -> Result<u64, StoreError> {
        unimplemented!()
    }
}
