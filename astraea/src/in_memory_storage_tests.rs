use crate::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadTree, StoreTree},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_approximate_tree_count() {
    let storage = InMemoryTreeStorage::empty();
    assert_eq!(storage.approximate_tree_count().await.unwrap(), 0);
    storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    assert_eq!(storage.approximate_tree_count().await.unwrap(), 1);
}
