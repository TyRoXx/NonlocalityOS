use crate::sorted_tree::{self, NodeValue};
use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::BlobDigest,
};
use serde::{de::DeserializeOwned, Serialize};

pub async fn new_tree<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &dyn StoreTree,
) -> Result<BlobDigest, StoreError> {
    sorted_tree::new_tree::<Key, Value>(store_tree).await
}

pub async fn insert<Key: Serialize + DeserializeOwned + Ord, Value: NodeValue + Clone>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    key: Key,
    value: Value,
) -> Result<BlobDigest, StoreError> {
    let loaded = match load_tree.load_tree(root).await {
        Some(tree) => tree,
        None => todo!(),
    };
    let hashed = match loaded.hash() {
        Some(tree) => tree,
        None => todo!(),
    };
    let tree = hashed.tree();
    if tree.references().is_empty() {
        let mut node = sorted_tree::node_from_tree::<Key, Value>(tree);
        node.insert(key, value);
        sorted_tree::store_node(store_tree, &node).await
    } else {
        todo!()
    }
}

pub async fn find<Key: Serialize + DeserializeOwned + PartialEq + Ord, Value: NodeValue + Clone>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
    key: &Key,
) -> Option<Value> {
    let loaded = load_tree.load_tree(root).await?;
    let hashed = loaded.hash()?;
    let tree = hashed.tree();
    if tree.references().is_empty() {
        let node = sorted_tree::node_from_tree::<Key, Value>(tree);
        node.find(key)
    } else {
        todo!()
    }
}
