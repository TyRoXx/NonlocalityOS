use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Hash)]
pub struct Node<Key: Serialize, Value: Serialize> {
    entries: Vec<(Key, Value)>,
}

pub async fn store_node<Key: Serialize, Value: Serialize>(
    store_tree: &dyn StoreTree,
    node: &Node<Key, Value>,
) -> Result<BlobDigest, StoreError> {
    store_tree
        .store_tree(&HashedTree::from(std::sync::Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_stdvec(&node).expect("serializing a new tree should always succeed"),
            ))
            .expect("this should always fit"),
            Vec::new(),
        ))))
        .await
}

pub async fn load_node<Key: Serialize + DeserializeOwned, Value: Serialize + DeserializeOwned>(
    load_tree: &dyn LoadTree,
    root: BlobDigest,
) -> Node<Key, Value> {
    let delayed_hashed_tree = match load_tree.load_tree(&root).await {
        Some(tree) => tree,
        None => todo!(),
    };
    let hashed_tree = match delayed_hashed_tree.hash() {
        Some(tree) => tree,
        None => todo!(),
    };
    let node = postcard::from_bytes::<Node<Key, Value>>(hashed_tree.tree().blob().as_slice())
        .expect("this should always work");
    node
}

pub async fn new_tree<Key: Serialize, Value: Serialize>(
    store_tree: &dyn StoreTree,
) -> Result<BlobDigest, StoreError> {
    let root = Node::<Key, Value> {
        entries: Vec::new(),
    };
    store_node(store_tree, &root).await
}

pub async fn insert<Key: Serialize + DeserializeOwned, Value: Serialize + DeserializeOwned>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: BlobDigest,
    key: Key,
    value: Value,
) -> Result<BlobDigest, StoreError> {
    let mut node = load_node::<Key, Value>(load_tree, root).await;
    node.entries.push((key, value));
    store_node(store_tree, &node).await
}

pub async fn find<
    Key: Serialize + DeserializeOwned + PartialEq,
    Value: Serialize + DeserializeOwned,
>(
    load_tree: &dyn LoadTree,
    root: BlobDigest,
    key: &Key,
) -> Option<Value> {
    let node = load_node::<Key, Value>(load_tree, root).await;
    node.entries
        .into_iter()
        .find_map(|(k, v)| if &k == key { Some(v) } else { None })
}
