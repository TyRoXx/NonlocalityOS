use crate::sorted_tree::{self, NodeValue, TreeReference};
use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::BlobDigest,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha3::{Digest, Sha3_512};
use std::collections::BTreeMap;

pub fn hash_key<Key: Serialize>(key: &Key) -> u8 {
    // TODO: use a better hash function (https://docs.dolthub.com/architecture/storage-engine/prolly-tree#controlling-chunk-size)
    let key_serialized = postcard::to_stdvec(key).expect("serializing key should succeed");
    let mut hasher = Sha3_512::new();
    hasher.update(&key_serialized);
    let result: [u8; 64] = hasher.finalize().into();
    result[0]
}

pub type IsSplitAfterKey<Key> = fn(key: &Key, chunk_size: usize) -> bool;

pub fn default_is_split_after_key<Key: Serialize>(key: &Key, chunk_size: usize) -> bool {
    if chunk_size < 10 {
        // No point in splitting small chunks.
        // TODO: use Tree efficiently
        return false;
    }
    // TODO: why are we even hashing the key if we already consider the chunk size?
    let hash = hash_key(key);
    let chunk_boundary_threshold = 10;
    hash < chunk_boundary_threshold
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

pub fn split_node_once<
    Key: Serialize + DeserializeOwned + Ord + Clone,
    Value: NodeValue + Clone,
>(
    original: sorted_tree::Node<Key, Value>,
    is_split_after_key: IsSplitAfterKey<Key>,
) -> Vec<sorted_tree::Node<Key, Value>> {
    let mut results = Vec::new();
    if original.entries().is_empty() {
        results.push(original);
        return results;
    }
    let mut current_chunk = sorted_tree::Node::<Key, Value>::new();
    for (key, value) in original.entries().iter() {
        current_chunk.insert((*key).clone(), value.clone());
        if is_split_after_key(key, current_chunk.entries().len()) {
            results.push(current_chunk);
            current_chunk = sorted_tree::Node::<Key, Value>::new();
        }
    }
    if !current_chunk.entries().is_empty() {
        results.push(current_chunk);
    }
    results
}

pub async fn build_node_hierarchy_from_internal_node<
    Key: Serialize + DeserializeOwned + Ord + Clone,
>(
    original: sorted_tree::Node<Key, TreeReference>,
    is_split_after_key: IsSplitAfterKey<Key>,
    store_tree: &dyn StoreTree,
) -> Result<sorted_tree::Node<Key, TreeReference>, StoreError> {
    let split_nodes = split_node_once(original, is_split_after_key);
    if split_nodes.len() == 1 {
        return Ok(split_nodes
            .into_iter()
            .next()
            .expect("at least one node should be available"));
    }
    let mut new_node = sorted_tree::Node::<Key, TreeReference>::new();
    for node in split_nodes.iter() {
        let key = node.entries().first().expect("node is not empty").0.clone();
        let digest = store_node(store_tree, node, &Metadata { is_leaf: false }).await?;
        new_node.insert(key, TreeReference::new(digest));
    }
    Box::pin(build_node_hierarchy_from_internal_node(
        new_node,
        is_split_after_key,
        store_tree,
    ))
    .await
}

pub async fn build_node_hierarchy_from_leaves<
    Key: Serialize + DeserializeOwned + Ord + Clone,
    Value: NodeValue + Clone,
>(
    original: sorted_tree::Node<Key, Value>,
    is_split_after_key: IsSplitAfterKey<Key>,
    store_tree: &dyn StoreTree,
) -> Result<EitherNodeType<Key, Value>, StoreError> {
    let is_empty = original.entries().is_empty();
    if is_empty {
        return Ok(EitherNodeType::Leaf(original));
    }
    let split_nodes = split_node_once(original, is_split_after_key);
    if split_nodes.len() == 1 {
        return Ok(EitherNodeType::Leaf(
            split_nodes
                .into_iter()
                .next()
                .expect("at least one node should be available"),
        ));
    }
    let mut new_node = sorted_tree::Node::<Key, TreeReference>::new();
    for node in split_nodes.iter() {
        let key = node.entries().first().expect("node is not empty").0.clone();
        let digest = store_node(store_tree, node, &Metadata { is_leaf: true }).await?;
        new_node.insert(key, TreeReference::new(digest));
    }
    Ok(EitherNodeType::Internal(
        Box::pin(build_node_hierarchy_from_internal_node(
            new_node,
            is_split_after_key,
            store_tree,
        ))
        .await?,
    ))
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

pub async fn insert<
    Key: Serialize + DeserializeOwned + Ord + Clone + std::fmt::Debug,
    Value: NodeValue + Clone + PartialEq + std::fmt::Debug,
>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    key: Key,
    value: Value,
    is_split_after_key: IsSplitAfterKey<Key>,
) -> Result<BlobDigest, StoreError> {
    insert_many(
        load_tree,
        store_tree,
        root,
        vec![(key, value)],
        is_split_after_key,
    )
    .await
}

pub async fn insert_many<
    Key: Serialize + DeserializeOwned + Ord + Clone + std::fmt::Debug,
    Value: NodeValue + Clone + PartialEq + std::fmt::Debug,
>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    entries: Vec<(Key, Value)>,
    is_split_after_key: IsSplitAfterKey<Key>,
) -> Result<BlobDigest, StoreError> {
    let mut expected_values_for_debug_check = BTreeMap::new();
    for (key, value) in entries.iter() {
        expected_values_for_debug_check.insert(key.clone(), value.clone());
    }
    let node =
        insert_impl::<Key, Value>(load_tree, store_tree, root, entries, is_split_after_key).await?;
    let digest = match node {
        EitherNodeType::Leaf(leaf_node) => {
            store_node(store_tree, &leaf_node, &Metadata { is_leaf: true }).await?
        }
        EitherNodeType::Internal(internal_node) => {
            store_node(store_tree, &internal_node, &Metadata { is_leaf: false }).await?
        }
    };
    for (key, value) in expected_values_for_debug_check.iter() {
        let found = find::<Key, Value>(load_tree, &digest, key)
            .await
            .expect("just inserted entry should be found");
        assert_eq!(*value, found);
    }
    match verify_integrity::<Key, Value>(load_tree, &digest).await {
        Some(result) => match result {
            IntegrityCheckResult::Valid(maybe_child_range) => match maybe_child_range {
                Some((min_key, max_key)) => {
                    let expected_min_key = expected_values_for_debug_check
                        .first_entry()
                        .expect("at least one entry")
                        .key()
                        .clone();
                    let expected_max_key = expected_values_for_debug_check
                        .last_entry()
                        .expect("at least one entry")
                        .key()
                        .clone();
                    assert!(expected_min_key >= min_key);
                    assert!(expected_max_key <= max_key);
                }
                None => {
                    assert!(
                        expected_values_for_debug_check.is_empty(),
                        "expected no entries in tree"
                    );
                }
            },
            IntegrityCheckResult::Corrupted(reason) => {
                panic!("Tree integrity check failed after insert_many: {}", reason);
            }
        },
        None => {
            panic!("Tree integrity check returned None after insert_many");
        }
    }
    Ok(digest)
}

#[derive(Debug, PartialEq)]
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

async fn insert_impl<
    Key: Serialize + DeserializeOwned + Ord + Clone + std::fmt::Debug,
    Value: NodeValue + Clone + PartialEq + std::fmt::Debug,
>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    entries: Vec<(Key, Value)>,
    is_split_after_key: IsSplitAfterKey<Key>,
) -> Result<EitherNodeType<Key, Value>, StoreError> {
    let loaded = load_node(load_tree, root).await.expect("TODO");
    match loaded {
        EitherNodeType::Leaf(mut node) => {
            for (key, value) in entries.into_iter() {
                node.insert(key, value);
            }
            build_node_hierarchy_from_leaves(node, is_split_after_key, store_tree).await
        }
        EitherNodeType::Internal(mut node) => {
            let mut partition_point_to_entries: BTreeMap<usize, Vec<(Key, Value)>> =
                BTreeMap::new();
            for (key, value) in entries.into_iter() {
                let partition_point = node.entries().partition_point(|element| element.0 <= key);
                partition_point_to_entries
                    .entry(partition_point)
                    .or_default()
                    .push((key, value));
            }
            for (partition_point, entries) in partition_point_to_entries.into_iter().rev() {
                if partition_point == 0 {
                    let mut new_chunk = sorted_tree::Node::<Key, Value>::new();
                    let expected_min_key = entries.first().expect("at least one entry").0.clone();
                    let expected_max_key = entries.last().expect("at least one entry").0.clone();
                    for (key, value) in entries.into_iter() {
                        new_chunk.insert(key.clone(), value);
                    }
                    let key = new_chunk
                        .entries()
                        .first()
                        .expect("node is not empty")
                        .0
                        .clone();
                    let digest = match build_node_hierarchy_from_leaves(
                        new_chunk,
                        is_split_after_key,
                        store_tree,
                    )
                    .await?
                    {
                        EitherNodeType::Leaf(leaf_node) => {
                            store_node(store_tree, &leaf_node, &Metadata { is_leaf: true }).await?
                        }
                        EitherNodeType::Internal(internal_node) => {
                            store_node(store_tree, &internal_node, &Metadata { is_leaf: false })
                                .await?
                        }
                    };
                    match verify_integrity::<Key, Value>(load_tree, &digest).await {
                        Some(result) => match result {
                            IntegrityCheckResult::Valid(maybe_child_range) => {
                                let (min_key, max_key) = match maybe_child_range {
                                    Some((min_key, max_key)) => (min_key, max_key),
                                    None => {
                                        panic!("Tree integrity check returned no key range after inserting into child node");
                                    }
                                };
                                assert_eq!(expected_min_key, min_key);
                                assert_eq!(expected_max_key, max_key);
                            }
                            IntegrityCheckResult::Corrupted(reason) => {
                                panic!("Tree integrity check failed after inserting into child node: {}", reason);
                            }
                        },
                        None => {
                            panic!("Tree integrity check returned None after inserting into child node");
                        }
                    }
                    node.insert(key, TreeReference::new(digest));
                } else {
                    let key = entries.first().expect("at least one entry").0.clone();
                    let new_chunk_key =
                        std::cmp::min(key, node.entries()[partition_point - 1].0.clone());
                    let stored = Box::pin(insert_many(
                        load_tree,
                        store_tree,
                        node.entries()[partition_point - 1].1.reference(),
                        entries,
                        is_split_after_key,
                    ))
                    .await?;
                    node.replace_chunk(
                        partition_point - 1,
                        &[(new_chunk_key, TreeReference::new(stored))],
                    );
                }
            }
            Ok(EitherNodeType::Internal(
                build_node_hierarchy_from_internal_node(node, is_split_after_key, store_tree)
                    .await?,
            ))
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
            if node.entries().is_empty() {
                // This shouldn't normally happen because insert doesn't create empty internal nodes.
                return None;
            }
            let partition_point = node.entries().partition_point(|element| element.0 <= *key);
            if partition_point == 0 {
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

pub async fn size<
    Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
) -> Option<u64> {
    let loaded: EitherNodeType<Key, Value> = load_node(load_tree, root).await?;
    match loaded {
        EitherNodeType::Leaf(node) => Some(node.entries().len() as u64),
        EitherNodeType::Internal(node) => {
            let mut total_size = 0;
            for (_key, child) in node.entries() {
                let child_size = Box::pin(size::<Key, Value>(load_tree, child.reference())).await?;
                total_size += child_size;
            }
            Some(total_size)
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum IntegrityCheckResult<Key> {
    Valid(Option<(Key, Key)>),
    Corrupted(String),
}

pub async fn verify_integrity<
    Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone + std::fmt::Debug,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    node_digest: &BlobDigest,
) -> Option<IntegrityCheckResult<Key>> {
    let loaded: EitherNodeType<Key, Value> = load_node(load_tree, node_digest).await?;
    match loaded {
        EitherNodeType::Leaf(node) => {
            if !node.entries().is_sorted_by_key(|entry| &entry.0) {
                Some(IntegrityCheckResult::Corrupted(
                    "Leaf node entries are not sorted".into(),
                ))
            } else if node.entries().is_empty() {
                Some(IntegrityCheckResult::Valid(None))
            } else {
                Some(IntegrityCheckResult::Valid(Some((
                    node.entries().first().unwrap().0.clone(),
                    node.entries().last().unwrap().0.clone(),
                ))))
            }
        }
        EitherNodeType::Internal(node) => {
            if !node.entries().is_sorted_by_key(|entry| &entry.0) {
                return Some(IntegrityCheckResult::Corrupted(
                    "Internal node entries are not sorted".into(),
                ));
            }
            if node.entries().is_empty() {
                return Some(IntegrityCheckResult::Corrupted(
                    "Internal node has no entries".into(),
                ));
            }
            let mut last_max_child_key = None;
            for (key, child) in node.entries() {
                if let Some(child_max_key) = &last_max_child_key {
                    if child_max_key >= key {
                        return Some(IntegrityCheckResult::Corrupted(format!(
                            "Child max key {:?} is expected to be less than the next child key {:?}",
                            child_max_key, key
                        )));
                    }
                }
                let child_result =
                    Box::pin(verify_integrity::<Key, Value>(load_tree, child.reference())).await?;
                match child_result {
                    IntegrityCheckResult::Valid(maybe_child_range) => {
                        let (child_min_key, child_max_key) = match maybe_child_range {
                            Some((min_key, max_key)) => (min_key, max_key),
                            None => {
                                return Some(IntegrityCheckResult::Corrupted(
                                    "Child returned no key range".to_string(),
                                ));
                            }
                        };
                        assert!(child_min_key <= child_max_key, "It really is a bug right in this function if child min key {:?} is greater than child max key {:?}", child_min_key, child_max_key);
                        if child_min_key != *key {
                            return Some(IntegrityCheckResult::Corrupted(format!(
                                "Child min key {:?} was expected to be {:?}",
                                child_min_key, key
                            )));
                        }
                        last_max_child_key = Some(child_max_key.clone());
                    }
                    IntegrityCheckResult::Corrupted(reason) => {
                        return Some(IntegrityCheckResult::Corrupted(format!(
                            "Key {:?} has corrupted child: {}",
                            key, reason
                        )));
                    }
                }
            }
            Some(IntegrityCheckResult::Valid(Some((
                node.entries().first().unwrap().0.clone(),
                last_max_child_key.unwrap(),
            ))))
        }
    }
}

#[derive(Debug)]
pub enum RecursiveLeafCount {
    Leaf(usize),
    Internal(Vec<RecursiveLeafCount>),
}

#[derive(Debug)]
pub struct InMemoryLeafNode<Key, Value> {
    pub entries: Vec<(Key, Value)>,
}

#[derive(Debug)]
pub enum InMemoryEitherNodeType<Key, Value> {
    Leaf(InMemoryLeafNode<Key, Value>),
    Internal(InMemoryInternalNode<Key, Value>),
}

impl<Key, Value> InMemoryEitherNodeType<Key, Value> {
    pub fn count(&self) -> RecursiveLeafCount {
        match self {
            InMemoryEitherNodeType::Leaf(node) => RecursiveLeafCount::Leaf(node.entries.len()),
            InMemoryEitherNodeType::Internal(node) => {
                let mut counts = Vec::new();
                for (_key, child) in &node.entries {
                    counts.push(child.count());
                }
                RecursiveLeafCount::Internal(counts)
            }
        }
    }
}

#[derive(Debug)]
pub struct InMemoryInternalNode<Key, Value> {
    pub entries: Vec<(Key, InMemoryEitherNodeType<Key, Value>)>,
}

pub async fn load_in_memory_node<
    Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
) -> InMemoryEitherNodeType<Key, Value> {
    let loaded = load_node(load_tree, root).await.expect("TODO");
    match loaded {
        EitherNodeType::Leaf(node) => InMemoryEitherNodeType::Leaf(InMemoryLeafNode {
            entries: node.entries().to_vec(),
        }),
        EitherNodeType::Internal(node) => {
            let mut entries = Vec::new();
            for (key, tree_reference) in node.entries() {
                let child =
                    Box::pin(load_in_memory_node(load_tree, tree_reference.reference())).await;
                entries.push((key.clone(), child));
            }
            InMemoryEitherNodeType::Internal(InMemoryInternalNode { entries })
        }
    }
}
