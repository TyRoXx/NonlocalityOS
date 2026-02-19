use crate::sharded_storage::ShardedStorage;
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadTree, StoreTree},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_store_and_load() {
    let storage = ShardedStorage::try_from(vec![
        Box::new(InMemoryTreeStorage::empty()),
        Box::new(InMemoryTreeStorage::empty()),
    ])
    .unwrap();
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let result = storage.load_tree(reference.digest()).await.unwrap();
    assert_eq!(result.reference().digest(), reference.digest());
}
