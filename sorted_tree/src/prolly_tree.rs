use crate::sorted_tree::{self, NodeValue, TreeReference};
use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::BlobDigest,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha3::{Digest, Sha3_512};

pub fn hash_key<Key: Serialize>(key: &Key) -> u8 {
    // TODO: use a better hash function (https://docs.dolthub.com/architecture/storage-engine/prolly-tree#controlling-chunk-size)
    let key_serialized = postcard::to_stdvec(key).expect("serializing key should succeed");
    let mut hasher = Sha3_512::new();
    hasher.update(&key_serialized);
    let result: [u8; 64] = hasher.finalize().into();
    result[0]
}

#[derive(Serialize, Deserialize, Clone, Hash)]
pub struct Metadata {
    pub is_leaf: bool,
}

pub async fn new_tree<Key: Serialize + Ord + Clone, Value: NodeValue + Clone>(
    store_tree: &dyn StoreTree,
) -> Result<BlobDigest, StoreError> {
    let root = sorted_tree::Node::<Key, Value>::new();
    store_node(store_tree, &root, &Metadata { is_leaf: true }).await
}

pub fn split_node_as_necessary<
    Key: Serialize + DeserializeOwned + Ord + Clone,
    Value: NodeValue + Clone,
>(
    original: sorted_tree::Node<Key, Value>,
) -> Vec<sorted_tree::Node<Key, Value>> {
    let chunk_boundary_threshold = 10;
    let mut results = Vec::new();
    results.push(sorted_tree::Node::new());
    for (key, value) in original.entries() {
        results
            .last_mut()
            .expect("at least one node should be available")
            .insert((*key).clone(), value.clone());
        if hash_key(key) < chunk_boundary_threshold {
            results.push(sorted_tree::Node::new());
        }
    }
    if results
        .last()
        .expect("at least one node should be available")
        .entries()
        .is_empty()
    {
        results.pop();
    }
    results
}

pub async fn store_node<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &dyn StoreTree,
    node: &sorted_tree::Node<Key, Value>,
    metadata: &Metadata,
) -> Result<BlobDigest, StoreError> {
    let metadata_serialized =
        postcard::to_stdvec(metadata).expect("serializing metadata should always succeed");
    crate::sorted_tree::store_node(store_tree, node, &bytes::Bytes::from(metadata_serialized)).await
}

pub async fn insert<Key: Serialize + DeserializeOwned + Ord + Clone, Value: NodeValue + Clone>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    key: Key,
    value: Value,
) -> Result<BlobDigest, StoreError> {
    let chunks = insert_impl::<Key, Value>(load_tree, store_tree, root, key, value).await?;
    assert!(!chunks.is_empty());
    if chunks.len() == 1 {
        Ok(chunks
            .first()
            .expect("at least one chunk should be available")
            .1
            .reference()
            .clone())
    } else {
        let mut node = sorted_tree::Node::<Key, TreeReference>::new();
        for (key, digest) in chunks.into_iter() {
            node.insert(key, digest);
        }
        store_node(store_tree, &node, &Metadata { is_leaf: false }).await
    }
}

pub enum EitherNodeType<Key: Serialize + Ord, Value: NodeValue> {
    Leaf(sorted_tree::Node<Key, Value>),
    Internal(sorted_tree::Node<Key, TreeReference>),
}

pub async fn load_node<
    Key: Serialize + DeserializeOwned + PartialEq + Ord,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
) -> Option<EitherNodeType<Key, Value>> {
    let loaded = load_tree.load_tree(root).await?;
    let hashed = loaded.hash()?;
    let tree = hashed.tree();
    let (metadata, sorted_tree_data) =
        match postcard::take_from_bytes::<Metadata>(tree.blob().as_slice()) {
            Ok((metadata, sorted_tree_data)) => (metadata, sorted_tree_data),
            Err(_) => todo!("failed to parse metadata"),
        };
    if metadata.is_leaf {
        let node = sorted_tree::node_from_tree::<Key, Value>(
            tree,
            tree.blob().as_slice().len() - sorted_tree_data.len(),
        );
        Some(EitherNodeType::Leaf(node))
    } else {
        let node = sorted_tree::node_from_tree::<Key, TreeReference>(
            tree,
            tree.blob().as_slice().len() - sorted_tree_data.len(),
        );
        Some(EitherNodeType::Internal(node))
    }
}

pub async fn insert_impl<
    Key: Serialize + DeserializeOwned + Ord + Clone,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    key: Key,
    value: Value,
) -> Result<Vec<(Key, TreeReference)>, StoreError> {
    let loaded = load_node(load_tree, root).await.expect("TODO");
    match loaded {
        EitherNodeType::Leaf(mut node) => {
            node.insert(key, value);
            let nodes = split_node_as_necessary(node);
            let mut results = Vec::new();
            for node in nodes.iter() {
                let first_key = node.entries().first().expect("node is not empty").0.clone();
                let digest = store_node(store_tree, &node, &Metadata { is_leaf: true }).await?;
                results.push((first_key, TreeReference::new(digest)));
            }
            Ok(results)
        }
        EitherNodeType::Internal(mut node) => {
            let partition_point = node.entries().partition_point(|element| element.0 <= key);
            if partition_point == 0 {
                let mut new_chunk = sorted_tree::Node::<Key, Value>::new();
                new_chunk.insert(key.clone(), value);
                let stored =
                    store_node(store_tree, &new_chunk, &Metadata { is_leaf: true }).await?;
                node.insert(key, TreeReference::new(stored));
            } else {
                let chunks = Box::pin(insert_impl(
                    load_tree,
                    store_tree,
                    node.entries()[partition_point - 1].1.reference(),
                    key,
                    value,
                ))
                .await?;
                node.replace_chunk(partition_point - 1, &chunks);
            }
            let nodes = split_node_as_necessary(node);
            let mut results = Vec::new();
            for node in nodes.iter() {
                let first_key = node.entries().first().expect("node is not empty").0.clone();
                let digest = store_node(store_tree, &node, &Metadata { is_leaf: false }).await?;
                results.push((first_key, TreeReference::new(digest)));
            }
            Ok(results)
        }
    }
}

pub async fn find<
    Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
    key: &Key,
) -> Option<Value> {
    let loaded = load_node(load_tree, root).await.expect("TODO");
    match loaded {
        EitherNodeType::Leaf(node) => node.find(key),
        EitherNodeType::Internal(node) => {
            let partition_point = node.entries().partition_point(|element| element.0 <= *key);
            if partition_point == 0 {
                if node.entries().is_empty() {
                    None
                } else {
                    Box::pin(find(
                        load_tree,
                        node.entries()
                            .first()
                            .expect("at least one entry")
                            .1
                            .reference(),
                        key,
                    ))
                    .await
                }
            } else {
                Box::pin(find(
                    load_tree,
                    node.entries()[partition_point - 1].1.reference(),
                    key,
                ))
                .await
            }
        }
    }
}
