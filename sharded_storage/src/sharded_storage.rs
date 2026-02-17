use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{
        CommitChanges, LoadError, LoadTree, StoreError, StoreTree, StrongDelayedHashedTree,
        StrongReference,
    },
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;

pub trait StorageShard: LoadTree + StoreTree + CommitChanges {}

impl StorageShard for InMemoryTreeStorage {}

#[derive(Debug)]
pub struct ShardedStorage {
    shards: Vec<Box<dyn StorageShard + Send + Sync>>,
}

impl ShardedStorage {
    pub fn try_from(shards: Vec<Box<dyn StorageShard + Send + Sync>>) -> Option<Self> {
        if shards.is_empty() {
            return None;
        }
        Some(Self { shards })
    }
}

fn get_shard_index(reference: &BlobDigest, shard_count: usize) -> usize {
    let simplified_digest = u64::from_be_bytes(
        reference
            .0
             .1
            .split_at(24)
            .1
            .try_into()
            .expect("There are enough bytes in the array"),
    );
    (simplified_digest % (shard_count as u64)) as usize
}

#[async_trait]
impl LoadTree for ShardedStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        let shard_index = get_shard_index(reference, self.shards.len());
        self.shards[shard_index].load_tree(reference).await
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.approximate_tree_count().await?;
        }
        Ok(total)
    }
}

#[async_trait]
impl StoreTree for ShardedStorage {
    async fn store_tree(
        &self,
        tree: &HashedTree,
    ) -> std::result::Result<StrongReference, StoreError> {
        let shard_index = get_shard_index(&tree.digest(), self.shards.len());
        self.shards[shard_index].store_tree(tree).await
    }
}

#[async_trait]
impl CommitChanges for ShardedStorage {
    async fn commit_changes(&self) -> Result<u64, StoreError> {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.commit_changes().await?;
        }
        Ok(total)
    }
}
