use crate::sorted_tree::{self, NodeValue, TreeReference};
use astraea::{
    storage::{load_children, LoadError, LoadTree, StoreError, StoreTree, StrongReference},
    tree::{BlobDigest, TREE_BLOB_MAX_LENGTH},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};

#[derive(Debug, PartialEq)]
pub enum IntegrityCheckResult {
    Valid { depth: usize },
    Corrupted(String),
}

// TODO: Why does this only return the first byte of the hash? Is 256 buckets enough for good distribution?
// Would using more bytes improve the split point selection quality?
pub fn hash_key<Key: Serialize>(key: &Key) -> u8 {
    // TODO: use a better hash function (https://docs.dolthub.com/architecture/storage-engine/prolly-tree#controlling-chunk-size)
    let key_serialized = postcard::to_stdvec(key).expect("serializing key should succeed");
    let hasher = rapidhash::quality::SeedableState::fixed();
    let result: [u8; 8] = hasher.hash_one(&key_serialized).to_le_bytes();
    result[0]
}

// TODO: Why does this function determine split points based on key hash + size but not based on the actual tree balance?
// Should this be using a rolling hash or Rabin fingerprinting for content-defined chunking?
// The threshold value of 10 (out of 256 possible hash values) gives ~4% split probability - is this optimal?
// What happens if keys are not uniformly distributed? Could this lead to unbalanced trees?
pub fn is_split_after_key<Key: Serialize>(key: &Key, chunk_size_in_bytes: usize) -> bool {
    if chunk_size_in_bytes < 1000 {
        // No point in splitting small chunks.
        // TODO: use Tree efficiently
        return false;
    }
    if chunk_size_in_bytes >= TREE_BLOB_MAX_LENGTH / 2 {
        // TODO: try to pack more elements in a chunk before splitting
        return true;
    }
    let hash = hash_key(key);
    // TODO: Why is chunk_boundary_threshold hardcoded to 10? Should this be configurable or derived from target chunk size?
    // Is there a mathematical relationship between this threshold and the desired average chunk size?
    let chunk_boundary_threshold = 10;
    if hash < chunk_boundary_threshold {
        // written with an if expression so that we can see whether the tests cover both branches
        true
    } else {
        false
    }
}

pub struct SizeTracker {
    element_count: usize,
    total_element_size: usize,
}

impl Default for SizeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SizeTracker {
    pub fn new() -> Self {
        SizeTracker {
            element_count: 0,
            total_element_size: 0,
        }
    }

    pub fn add_entry<Key: Serialize, Value: Serialize>(&mut self, key: &Key, value: &Value) {
        let entry_serialized: Vec<u8> =
            postcard::to_stdvec(&(key, value)).expect("serializing entry should succeed");
        self.element_count += 1;
        self.total_element_size += entry_serialized.len();
    }

    pub fn size(&self) -> usize {
        // TODO: optimize size calculation
        let metadata_serialized: Vec<u8> =
            postcard::to_stdvec(&Metadata { is_leaf: true }).unwrap();
        let element_count_serialized: Vec<u8> = postcard::to_stdvec(&self.element_count).unwrap();
        metadata_serialized.len() + element_count_serialized.len() + self.total_element_size
    }
}

#[derive(Serialize, Deserialize, Clone, Hash)]
pub struct Metadata {
    pub is_leaf: bool,
}

pub async fn store_node<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &(dyn StoreTree + Send + Sync),
    node: &sorted_tree::Node<Key, Value>,
    metadata: &Metadata,
) -> Result<StrongReference, StoreError> {
    let metadata_serialized =
        postcard::to_stdvec(metadata).expect("serializing metadata should always succeed");
    crate::sorted_tree::store_node(store_tree, node, &bytes::Bytes::from(metadata_serialized)).await
}

#[derive(Debug, PartialEq)]
pub enum EitherNodeType<Key: Serialize + Ord, Value: NodeValue> {
    Leaf(sorted_tree::Node<Key, Value>),
    Internal(sorted_tree::Node<Key, TreeReference>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DeserializationError {
    Load(LoadError),
    TreeHashMismatch(BlobDigest),
}

impl std::fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DeserializationError {}

pub async fn load_node<
    Key: Serialize + DeserializeOwned + PartialEq + Ord,
    Value: NodeValue + Clone,
>(
    load_tree: &(dyn LoadTree + Send + Sync),
    root: &BlobDigest,
) -> Result<EitherNodeType<Key, Value>, Box<dyn std::error::Error>> {
    let loaded = match load_tree.load_tree(root).await {
        Ok(loaded) => loaded,
        Err(error) => return Err(DeserializationError::Load(error).into()),
    };
    let hashed = match loaded.hash() {
        Some(hashed) => hashed,
        None => return Err(DeserializationError::TreeHashMismatch(*root).into()),
    };
    let tree = hashed.hashed_tree().tree();
    let (metadata, sorted_tree_data) =
        postcard::take_from_bytes::<Metadata>(tree.blob().as_slice())?;
    let children = load_children(load_tree, tree)
        .await?
        .into_iter()
        .map(|child| child.reference().clone())
        .collect::<Vec<_>>();
    if metadata.is_leaf {
        let node = sorted_tree::node_from_tree::<Key, Value>(
            tree.blob(),
            &children,
            tree.blob().as_slice().len() - sorted_tree_data.len(),
        );
        Ok(EitherNodeType::Leaf(node))
    } else {
        let node = sorted_tree::node_from_tree::<Key, TreeReference>(
            tree.blob(),
            &children,
            tree.blob().as_slice().len() - sorted_tree_data.len(),
        );
        Ok(EitherNodeType::Internal(node))
    }
}

#[derive(Debug, Clone)]
pub enum EditableNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    Reference(StrongReference),
    Loaded(EditableLoadedNode<Key, Value>),
}

impl<
        Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone + Debug,
        Value: NodeValue + Clone,
    > Default for EditableNode<Key, Value>
{
    fn default() -> Self {
        Self::new()
    }
}

impl<
        Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone + Debug,
        Value: NodeValue + Clone,
    > EditableNode<Key, Value>
{
    pub fn new() -> Self {
        EditableNode::Loaded(EditableLoadedNode::Leaf(EditableLeafNode {
            entries: BTreeMap::new(),
        }))
    }

    pub async fn require_loaded(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<&mut EditableLoadedNode<Key, Value>, Box<dyn std::error::Error>> {
        match self {
            EditableNode::Reference(strong_reference) => {
                let loaded: EitherNodeType<Key, Value> =
                    load_node(load_tree, strong_reference.digest()).await?;
                *self = EditableNode::Loaded(EditableLoadedNode::new(loaded));
            }
            EditableNode::Loaded(_loaded_node) => {}
        };
        let loaded = match self {
            EditableNode::Loaded(loaded_node) => loaded_node,
            _ => unreachable!(),
        };
        Ok(loaded)
    }

    pub async fn insert(
        &mut self,
        key: Key,
        value: Value,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (self_top_key, nodes_split) = self.insert_impl(key, value, load_tree).await?;
        if nodes_split.is_empty() {
            return Ok(());
        }
        let mut entries = BTreeMap::new();
        entries.insert(self_top_key, self.clone());
        for node in nodes_split {
            entries.insert(
                node.top_key().expect("Node cannot be empty here").clone(),
                EditableNode::Loaded(node),
            );
        }
        *self = EditableNode::Loaded(EditableLoadedNode::Internal(EditableInternalNode {
            entries,
        }));
        Ok(())
    }

    pub async fn insert_impl(
        &mut self,
        key: Key,
        value: Value,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(Key, Vec<EditableLoadedNode<Key, Value>>), Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        let nodes_split = Box::pin(loaded.insert(key, value, load_tree)).await?;
        Ok((
            loaded.top_key().expect("Node cannot be empty here").clone(),
            nodes_split,
        ))
    }

    pub async fn remove(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let (maybe_top_key, maybe_removed) = self.remove_impl(key, load_tree).await?;
        if maybe_top_key.is_none() {
            *self = EditableNode::Loaded(EditableLoadedNode::Leaf(EditableLeafNode {
                entries: BTreeMap::new(),
            }));
        } else {
            let loaded = self.require_loaded(load_tree).await?;
            if let Some(simplified) = loaded.simplify() {
                *self = simplified;
            }
        }
        Ok(maybe_removed)
    }

    pub async fn remove_impl(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(Option<Key>, Option<Value>), Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        let result = loaded.remove(key, load_tree).await?;
        Ok(result)
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        loaded.find(key, load_tree).await
    }

    pub async fn count(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        Box::pin(loaded.count(load_tree)).await
    }

    pub async fn save(
        &mut self,
        store_tree: &(dyn StoreTree + Send + Sync),
    ) -> Result<StrongReference, Box<dyn std::error::Error>> {
        match self {
            EditableNode::Reference(strong_reference) => Ok(strong_reference.clone()),
            EditableNode::Loaded(loaded_node) => loaded_node.save(store_tree).await,
        }
    }

    pub async fn load(
        digest: &BlobDigest,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let loaded: EitherNodeType<Key, Value> = load_node(load_tree, digest).await?;
        Ok(EditableNode::Loaded(EditableLoadedNode::new(loaded)))
    }

    pub async fn verify_integrity(
        &mut self,
        expected_top_key: Option<&Key>,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        if loaded.top_key() != expected_top_key {
            return Ok(IntegrityCheckResult::Corrupted(
                "Top key mismatch".to_string(),
            ));
        }
        Box::pin(loaded.verify_integrity(load_tree)).await
    }

    pub async fn merge(
        &mut self,
        other: Self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(Key, Vec<EditableLoadedNode<Key, Value>>), Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        let other_loaded = match other {
            EditableNode::Reference(strong_reference) => {
                let loaded: EitherNodeType<Key, Value> =
                    load_node(load_tree, strong_reference.digest()).await?;
                EditableLoadedNode::new(loaded)
            }
            EditableNode::Loaded(loaded_node) => loaded_node,
        };
        match (loaded, other_loaded) {
            (EditableLoadedNode::Leaf(self_leaf), EditableLoadedNode::Leaf(other_leaf)) => {
                for (key, value) in other_leaf.entries {
                    self_leaf.entries.insert(key, value);
                }
                let split_nodes = self_leaf.check_split();
                Ok((
                    self_leaf
                        .top_key()
                        .expect("Leaf cannot be empty here")
                        .clone(),
                    split_nodes
                        .into_iter()
                        .map(|n| EditableLoadedNode::Leaf(n))
                        .collect(),
                ))
            }
            (
                EditableLoadedNode::Internal(self_internal),
                EditableLoadedNode::Internal(other_internal),
            ) => {
                for (key, child_node) in other_internal.entries {
                    let previous_entry = self_internal.entries.insert(key, child_node);
                    if let Some(_existing_child) = previous_entry {
                        return Err(Box::new(std::io::Error::other("Merge node key collision")));
                    }
                }
                let split_nodes = self_internal.check_split();
                Ok((
                    self_internal
                        .top_key()
                        .expect("Internal node cannot be empty here")
                        .clone(),
                    split_nodes
                        .into_iter()
                        .map(|n| EditableLoadedNode::Internal(n))
                        .collect(),
                ))
            }
            _ => unreachable!(),
        }
    }

    pub async fn is_naturally_split(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        Ok(loaded.is_naturally_split())
    }
}

#[derive(Debug, Clone)]
pub struct EditableLeafNode<Key, Value> {
    entries: BTreeMap<Key, Value>,
}

impl<Key: std::cmp::Ord + Clone + Serialize, Value: Clone + NodeValue>
    EditableLeafNode<Key, Value>
{
    pub fn create(entries: BTreeMap<Key, Value>) -> Option<Self> {
        if entries.is_empty() {
            None
        } else {
            Some(EditableLeafNode { entries })
        }
    }

    pub async fn insert(&mut self, key: Key, value: Value) -> Vec<EditableLeafNode<Key, Value>> {
        self.entries.insert(key, value);
        self.check_split()
    }

    pub async fn remove(
        &mut self,
        key: &Key,
    ) -> Result<(Option<Key>, Option<Value>), Box<dyn std::error::Error>> {
        let removed = self.entries.remove(key);
        let top_key = self.top_key().cloned();
        Ok((top_key, removed))
    }

    fn check_split(&mut self) -> Vec<EditableLeafNode<Key, Value>> {
        let mut result = Vec::new();
        let mut current_node = BTreeMap::new();
        let mut current_node_size_tracker = SizeTracker::new();
        for entry in self.entries.iter() {
            current_node_size_tracker.add_entry(entry.0, &entry.1.to_content());
            current_node.insert(entry.0.clone(), entry.1.clone());
            if is_split_after_key(entry.0, current_node_size_tracker.size()) {
                result.push(
                    EditableLeafNode::create(current_node)
                        .expect("Must succeed because list is not empty"),
                );
                current_node = BTreeMap::new();
                current_node_size_tracker = SizeTracker::new();
            }
        }
        if !current_node.is_empty() {
            result.push(
                EditableLeafNode::create(current_node)
                    .expect("Must succeed because list is not empty"),
            );
        }
        *self = result.remove(0);
        result
    }

    pub fn top_key(&self) -> Option<&Key> {
        self.entries.keys().next_back()
    }

    pub fn find(&mut self, key: &Key) -> Option<Value> {
        self.entries.get(key).cloned()
    }

    pub fn verify_integrity(&mut self) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        let mut size_tracker = SizeTracker::new();
        for (index, (key, value)) in self.entries.iter().enumerate() {
            size_tracker.add_entry(key, &value.to_content());
            let is_split = is_split_after_key(key, size_tracker.size());
            if (index < self.entries.len() - 1) && is_split {
                return Ok(IntegrityCheckResult::Corrupted(format!(
                    "Leaf node integrity check failed: Key at index {} indicates split but node is not final (number of keys: {})",
                    index, self.entries.len()
                )));
            }
        }
        Ok(IntegrityCheckResult::Valid { depth: 0 })
    }

    pub fn is_naturally_split(&self) -> bool {
        let mut size_tracker = SizeTracker::new();
        for entry in self.entries.iter() {
            size_tracker.add_entry(entry.0, &entry.1.to_content());
        }
        is_split_after_key(
            self.entries.keys().last().expect("leaf node is not empty"),
            size_tracker.size(),
        )
    }

    pub fn entries(&self) -> &BTreeMap<Key, Value> {
        &self.entries
    }
}

#[derive(Debug, Clone)]
pub struct EditableInternalNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    entries: BTreeMap<Key, EditableNode<Key, Value>>,
}

impl<
        Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone + Debug,
        Value: NodeValue + Clone,
    > EditableInternalNode<Key, Value>
{
    pub fn create(entries: BTreeMap<Key, EditableNode<Key, Value>>) -> Option<Self> {
        if entries.is_empty() {
            None
        } else {
            Some(EditableInternalNode { entries })
        }
    }

    pub async fn insert(
        &mut self,
        key: Key,
        value: Value,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Vec<EditableInternalNode<Key, Value>>, Box<dyn std::error::Error>> {
        let last_index = self.entries.len() - 1;
        // TODO: optimize search
        for (index, (entry_key, entry_value)) in self.entries.iter_mut().enumerate() {
            if (index == last_index) || (key <= *entry_key) {
                let (updated_key, split_nodes) =
                    entry_value.insert_impl(key, value, load_tree).await?;
                if updated_key != *entry_key {
                    let old_key = entry_key.clone();
                    let old_value = self.entries.remove(&old_key).expect("key must exist");
                    let previous_entry = self.entries.insert(updated_key, old_value);
                    assert!(previous_entry.is_none(), "Split node key collision");
                }
                for node in split_nodes {
                    let previous_entry = self.entries.insert(
                        node.top_key().expect("Node cannot be empty here").clone(),
                        EditableNode::Loaded(node),
                    );
                    assert!(previous_entry.is_none(), "Split node key collision");
                }
                self.update_chunk_boundaries(load_tree).await?;
                break;
            }
        }
        Ok(self.check_split())
    }

    pub async fn remove(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(Option<Key>, Option<Value>), Box<dyn std::error::Error>> {
        // TODO: optimize search
        for (entry_key, entry_value) in self.entries.iter_mut() {
            if key <= entry_key {
                let (maybe_new_top_key, maybe_removed) =
                    Box::pin(entry_value.remove_impl(key, load_tree)).await?;
                match maybe_new_top_key {
                    Some(new_top_key) => {
                        if new_top_key != *entry_key {
                            let entry_key = entry_key.clone();
                            let entry_value_removed = self
                                .entries
                                .remove(&entry_key)
                                .expect("Must exist because we just iterated over the entries");
                            self.entries.insert(new_top_key, entry_value_removed);
                        }
                    }
                    None => {
                        let entry_key = entry_key.clone();
                        self.entries.remove(&entry_key);
                    }
                }
                let top_key = self.top_key().cloned();
                match maybe_removed {
                    Some(removed) => {
                        if !self.entries.is_empty() {
                            self.update_chunk_boundaries(load_tree).await?;
                        }
                        return Ok((top_key, Some(removed)));
                    }
                    None => {
                        return Ok((top_key, None));
                    }
                }
            }
        }
        Ok((self.top_key().cloned(), None))
    }

    async fn update_chunk_boundaries(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let merge_candidates = self.find_merge_candidates(load_tree).await?;
            match merge_candidates {
                Some((low_key, high_key)) => {
                    let mut low = self.entries.remove(&low_key).expect("key must exist");
                    let high = self.entries.remove(&high_key).expect("key must exist");
                    let (low_top_key, split_nodes) = low.merge(high, load_tree).await?;
                    assert!(split_nodes.is_empty() || low.is_naturally_split(load_tree).await?);
                    assert_ne!(low_key, low_top_key, "Merge did not change low key");
                    let previous_entry = self.entries.insert(low_top_key, low);
                    assert!(previous_entry.is_none(), "Merge node key collision");
                    let split_nodes_len = split_nodes.len();
                    for (index, node) in split_nodes.into_iter().enumerate() {
                        assert!((index == split_nodes_len - 1) || node.is_naturally_split());
                        let previous_entry = self.entries.insert(
                            node.top_key().expect("Node cannot be empty here").clone(),
                            EditableNode::Loaded(node),
                        );
                        assert!(previous_entry.is_none(), "Merge node key collision");
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    async fn find_merge_candidates(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Option<(Key, Key)>, Box<dyn std::error::Error>> {
        let last_index = self.entries.len() - 1;
        let mut needs_merge: Option<&Key> = None;
        // TODO: optimize search
        for (index, (entry_key, entry_value)) in self.entries.iter_mut().enumerate() {
            if let Some(merge_value) = needs_merge.take() {
                return Ok(Some((merge_value.clone(), entry_key.clone())));
            }
            let is_split = entry_value.is_naturally_split(load_tree).await?;
            if (index != last_index) && !is_split {
                needs_merge = Some(entry_key);
            }
        }
        Ok(None)
    }

    fn check_split(&mut self) -> Vec<EditableInternalNode<Key, Value>> {
        let mut result = Vec::new();
        let mut current_node = BTreeMap::new();
        let mut current_node_size_tracker = SizeTracker::new();
        for entry in self.entries.iter() {
            current_node_size_tracker.add_entry(
                entry.0,
                &TreeReference::new(StrongReference::from_weak(BlobDigest::new(&[0; 64])))
                    .to_content(),
            );
            current_node.insert(entry.0.clone(), entry.1.clone());
            if is_split_after_key(entry.0, current_node_size_tracker.size()) {
                result.push(
                    EditableInternalNode::create(current_node)
                        .expect("Must succeed because list is not empty"),
                );
                current_node = BTreeMap::new();
                current_node_size_tracker = SizeTracker::new();
            }
        }
        if !current_node.is_empty() {
            result.push(
                EditableInternalNode::create(current_node)
                    .expect("Must succeed because list is not empty"),
            );
        }
        *self = result.remove(0);
        result
    }

    pub fn top_key(&self) -> Option<&Key> {
        self.entries.keys().next_back()
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        // TODO: optimize search
        for (entry_key, entry_value) in self.entries.iter_mut() {
            if key <= entry_key {
                return Box::pin(entry_value.find(key, load_tree)).await;
            }
        }
        Ok(None)
    }

    pub async fn verify_integrity(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        if self.entries.is_empty() {
            return Ok(IntegrityCheckResult::Corrupted(
                "Internal node integrity check failed: Node has no entries".to_string(),
            ));
        }
        let mut child_depth = None;
        for (index, (key, value)) in self.entries.iter_mut().enumerate() {
            match value.verify_integrity(Some(key), load_tree).await? {
                IntegrityCheckResult::Valid { depth } => {
                    if let Some(existing_depth) = child_depth {
                        if existing_depth != depth {
                            return Ok(IntegrityCheckResult::Corrupted(format!(
                                "Internal node integrity check failed at index {}: Child node depth mismatch (expected {}, found {})",
                                index, existing_depth, depth
                            )));
                        }
                    } else {
                        child_depth = Some(depth);
                    }
                }
                IntegrityCheckResult::Corrupted(reason) => {
                    return Ok(IntegrityCheckResult::Corrupted(format!(
                        "Internal node integrity check failed at index {}: {}",
                        index, reason
                    )));
                }
            }
        }
        Ok(IntegrityCheckResult::Valid {
            depth: child_depth.expect("Internal node has to have at least one child") + 1,
        })
    }

    pub fn is_naturally_split(&self) -> bool {
        let last_key = self
            .entries
            .keys()
            .last()
            .expect("internal node is not empty");
        let mut size_tracker = SizeTracker::new();
        for entry in self.entries.iter() {
            size_tracker.add_entry(
                entry.0,
                &TreeReference::new(StrongReference::from_weak(BlobDigest::new(&[0; 64])))
                    .to_content(),
            );
        }
        is_split_after_key(last_key, size_tracker.size())
    }
}

#[derive(Debug, Clone)]
pub enum EditableLoadedNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    Leaf(EditableLeafNode<Key, Value>),
    Internal(EditableInternalNode<Key, Value>),
}

impl<Key: Serialize + DeserializeOwned + Ord + Clone + Debug, Value: NodeValue + Clone>
    EditableLoadedNode<Key, Value>
{
    pub fn new(loaded: EitherNodeType<Key, Value>) -> Self {
        match loaded {
            EitherNodeType::Leaf(leaf_node) => {
                let mut entries = BTreeMap::new();
                for (key, value) in leaf_node.entries {
                    entries.insert(key, value);
                }
                EditableLoadedNode::Leaf(EditableLeafNode { entries })
            }
            EitherNodeType::Internal(internal_node) => {
                let mut entries = BTreeMap::new();
                for (key, child_node) in internal_node.entries {
                    entries.insert(key, EditableNode::Reference(child_node.reference().clone()));
                }
                EditableLoadedNode::Internal(EditableInternalNode { entries })
            }
        }
    }

    pub async fn insert(
        &mut self,
        key: Key,
        value: Value,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Vec<EditableLoadedNode<Key, Value>>, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => {
                let split_nodes = leaf_node.insert(key, value).await;
                Ok(split_nodes
                    .into_iter()
                    .map(|node| EditableLoadedNode::Leaf(node))
                    .collect())
            }
            EditableLoadedNode::Internal(internal_node) => {
                let split_nodes = internal_node.insert(key, value, load_tree).await?;
                Ok(split_nodes
                    .into_iter()
                    .map(|node| EditableLoadedNode::Internal(node))
                    .collect())
            }
        }
    }

    pub async fn remove(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<(Option<Key>, Option<Value>), Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.remove(key).await,
            EditableLoadedNode::Internal(internal_node) => {
                internal_node.remove(key, load_tree).await
            }
        }
    }

    pub fn simplify(&mut self) -> Option<EditableNode<Key, Value>> {
        match self {
            EditableLoadedNode::Internal(internal_node) => {
                if internal_node.entries.len() == 1 {
                    let (_, only_child) = internal_node
                        .entries
                        .iter_mut()
                        .next()
                        .expect("internal node has one entry");
                    Some(only_child.clone())
                } else {
                    None
                }
            }
            EditableLoadedNode::Leaf(_) => None,
        }
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.find(key)),
            EditableLoadedNode::Internal(internal_node) => internal_node.find(key, load_tree).await,
        }
    }

    pub fn top_key(&self) -> Option<&Key> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.top_key(),
            EditableLoadedNode::Internal(internal_node) => internal_node.top_key(),
        }
    }

    pub async fn count(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<u64, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.entries.len() as u64),
            EditableLoadedNode::Internal(internal_node) => {
                let mut total_count = 0;
                for child_node in internal_node.entries.values_mut() {
                    total_count += child_node.count(load_tree).await?;
                }
                Ok(total_count)
            }
        }
    }

    pub async fn save(
        &mut self,
        store_tree: &(dyn StoreTree + Send + Sync),
    ) -> Result<StrongReference, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => {
                let mut new_node = crate::sorted_tree::Node {
                    entries: Vec::new(),
                };
                for (key, value) in &leaf_node.entries {
                    new_node.entries.push((key.clone(), value.clone()));
                }
                let digest = store_node(store_tree, &new_node, &Metadata { is_leaf: true }).await?;
                Ok(digest)
            }
            EditableLoadedNode::Internal(internal_node) => {
                let mut new_node = crate::sorted_tree::Node {
                    entries: Vec::new(),
                };
                for (key, child_node) in &mut internal_node.entries {
                    let child_reference = Box::pin(child_node.save(store_tree)).await?;
                    new_node
                        .entries
                        .push((key.clone(), TreeReference::new(child_reference.clone())));
                }
                let digest =
                    store_node(store_tree, &new_node, &Metadata { is_leaf: false }).await?;
                Ok(digest)
            }
        }
    }

    pub async fn verify_integrity(
        &mut self,
        load_tree: &(dyn LoadTree + Send + Sync),
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.verify_integrity(),
            EditableLoadedNode::Internal(internal_node) => {
                internal_node.verify_integrity(load_tree).await
            }
        }
    }

    pub fn is_naturally_split(&self) -> bool {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.is_naturally_split(),
            EditableLoadedNode::Internal(internal_node) => internal_node.is_naturally_split(),
        }
    }
}

pub struct Iterator<
    't,
    Key: Serialize + DeserializeOwned + Ord + Clone + Debug,
    Value: NodeValue + Clone,
> {
    next: Vec<&'t mut EditableNode<Key, Value>>,
    leaf_iterator: Option<std::collections::btree_map::Iter<'t, Key, Value>>,
    load_tree: &'t (dyn LoadTree + Send + Sync),
}

impl<'t, Key, Value> Iterator<'t, Key, Value>
where
    Key: Serialize + DeserializeOwned + Ord + Clone + Debug,
    Value: NodeValue + Clone,
{
    pub fn new(
        node: &'t mut EditableNode<Key, Value>,
        load_tree: &'t (dyn LoadTree + Send + Sync),
    ) -> Self {
        Iterator {
            next: vec![node],
            leaf_iterator: None,
            load_tree,
        }
    }

    pub async fn next(&mut self) -> Result<Option<(Key, Value)>, Box<dyn std::error::Error>> {
        loop {
            if let Some(current_node) = self.leaf_iterator.as_mut() {
                match current_node.next() {
                    Some((key, value)) => return Ok(Some((key.clone(), value.clone()))),
                    None => {
                        self.leaf_iterator = None;
                    }
                }
            }
            match self.next.pop() {
                Some(next_node) => {
                    let loaded = next_node.require_loaded(self.load_tree).await?;
                    match loaded {
                        EditableLoadedNode::Leaf(leaf_node) => {
                            self.leaf_iterator = Some(leaf_node.entries().iter());
                            continue;
                        }
                        EditableLoadedNode::Internal(internal_node) => {
                            internal_node
                                .entries
                                .values_mut()
                                .rev()
                                .for_each(|child_node| {
                                    self.next.push(child_node);
                                });
                        }
                    };
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }
}
