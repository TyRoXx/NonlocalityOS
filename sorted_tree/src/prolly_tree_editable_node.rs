use crate::{
    prolly_tree::{default_is_split_after_key, load_node, EitherNodeType},
    sorted_tree::{NodeValue, TreeReference},
};
use astraea::{
    storage::{LoadTree, StoreTree},
    tree::BlobDigest,
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;

#[derive(Debug, PartialEq)]
pub enum IntegrityCheckResult {
    Valid { depth: usize },
    Corrupted(String),
}

pub fn is_split_after_key<Key: Serialize>(key: &Key, chunk_size: usize) -> bool {
    default_is_split_after_key(key, chunk_size)
}

#[derive(Debug, Clone)]
pub enum EditableNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    Reference(TreeReference),
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

    async fn require_loaded(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<&mut EditableLoadedNode<Key, Value>, Box<dyn std::error::Error>> {
        match self {
            EditableNode::Reference(tree_ref) => {
                let loaded: EitherNodeType<Key, Value> =
                    load_node(load_tree, tree_ref.reference()).await.unwrap(/*TODO */);
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
        load_tree: &dyn LoadTree,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (self_top_key, nodes_split) = self.insert_impl(key, value, load_tree).await?;
        if nodes_split.is_empty() {
            return Ok(());
        }
        let mut entries = BTreeMap::new();
        entries.insert(self_top_key, self.clone());
        for node in nodes_split {
            entries.insert(node.top_key().clone(), EditableNode::Loaded(node));
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
        load_tree: &dyn LoadTree,
    ) -> Result<(Key, Vec<EditableLoadedNode<Key, Value>>), Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        let nodes_split = Box::pin(loaded.insert(key, value, load_tree)).await?;
        Ok((loaded.top_key().clone(), nodes_split))
    }

    pub async fn remove(
        &mut self,
        key: &Key,
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        loaded.remove(key, load_tree).await
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        loaded.find(key, load_tree).await
    }

    pub async fn size(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        Box::pin(loaded.size(load_tree)).await
    }

    pub async fn save(
        &mut self,
        store_tree: &dyn StoreTree,
    ) -> Result<BlobDigest, Box<dyn std::error::Error>> {
        match self {
            EditableNode::Reference(tree_ref) => Ok(*tree_ref.reference()),
            EditableNode::Loaded(loaded_node) => loaded_node.save(store_tree).await,
        }
    }

    pub async fn verify_integrity(
        &mut self,
        expected_top_key: &Key,
        is_final_node: bool,
        load_tree: &dyn LoadTree,
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        if loaded.top_key() != expected_top_key {
            return Ok(IntegrityCheckResult::Corrupted(
                "Top key mismatch".to_string(),
            ));
        }
        Box::pin(loaded.verify_integrity(is_final_node, load_tree)).await
    }

    pub async fn merge(
        &mut self,
        other: Self,
        load_tree: &dyn LoadTree,
    ) -> Result<(Key, Vec<EditableLoadedNode<Key, Value>>), Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        let other_loaded = match other {
            EditableNode::Reference(tree_ref) => {
                let loaded: EitherNodeType<Key, Value> =
                    load_node(load_tree, tree_ref.reference()).await.unwrap(/*TODO */);
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
                    self_leaf.top_key().clone(),
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
                    self_internal.top_key().clone(),
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
        load_tree: &dyn LoadTree,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let loaded = self.require_loaded(load_tree).await?;
        match loaded {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.is_naturally_split()),
            EditableLoadedNode::Internal(internal_node) => internal_node.is_naturally_split(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EditableLeafNode<Key, Value> {
    entries: BTreeMap<Key, Value>,
}

impl<Key: std::cmp::Ord + Clone + Serialize, Value: Clone> EditableLeafNode<Key, Value> {
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

    pub async fn remove(&mut self, key: &Key) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        Ok(self.entries.remove(key))
    }

    fn check_split(&mut self) -> Vec<EditableLeafNode<Key, Value>> {
        let mut result = Vec::new();
        let mut current_node = BTreeMap::new();
        for entry in self.entries.iter() {
            current_node.insert(entry.0.clone(), entry.1.clone());
            if is_split_after_key(entry.0, current_node.len()) {
                result.push(EditableLeafNode::create(current_node).unwrap());
                current_node = BTreeMap::new();
            }
        }
        if !current_node.is_empty() {
            result.push(EditableLeafNode::create(current_node).unwrap());
        }
        *self = result.remove(0);
        result
    }

    pub fn top_key(&self) -> &Key {
        self.entries
            .keys()
            .next_back()
            .expect("leaf node is not empty")
    }

    pub fn find(&mut self, key: &Key) -> Option<Value> {
        self.entries.get(key).cloned()
    }

    pub fn verify_integrity(
        &mut self,
        is_final_node: bool,
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        for (index, key) in self.entries.keys().enumerate() {
            let is_split = is_split_after_key(key, index + 1);
            if index == self.entries.len() - 1 {
                if !is_final_node && !is_split {
                    return Ok(IntegrityCheckResult::Corrupted(
                        "Leaf node integrity check failed: Final key does not indicate split"
                            .to_string(),
                    ));
                }
            } else if is_split {
                return Ok(IntegrityCheckResult::Corrupted(format!(
                    "Leaf node integrity check failed: Key at index {} indicates split but node is not final (number of keys: {})",
                    index, self.entries.len()
                )));
            }
        }
        Ok(IntegrityCheckResult::Valid { depth: 0 })
    }

    pub fn is_naturally_split(&self) -> bool {
        is_split_after_key(
            self.entries.keys().last().expect("leaf node is not empty"),
            self.entries.len(),
        )
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
        load_tree: &dyn LoadTree,
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
                    let previous_entry = self
                        .entries
                        .insert(node.top_key().clone(), EditableNode::Loaded(node));
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
        _key: &Key,
        _load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        todo!()
    }

    async fn update_chunk_boundaries(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            let merge_candidates = self.find_merge_candidates(load_tree).await?;
            match merge_candidates {
                Some((low_key, high_key)) => {
                    let mut low = self.entries.remove(&low_key).expect("key must exist");
                    let high = self.entries.remove(&high_key).expect("key must exist");
                    let (low_top_key, split_nodes) = low.merge(high, load_tree).await?;
                    assert_ne!(low_key, low_top_key, "Merge did not change low key");
                    let previous_entry = self.entries.insert(low_top_key, low);
                    assert!(previous_entry.is_none(), "Merge node key collision");
                    for node in split_nodes {
                        let previous_entry = self
                            .entries
                            .insert(node.top_key().clone(), EditableNode::Loaded(node));
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
        load_tree: &dyn LoadTree,
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
        for entry in self.entries.iter() {
            current_node.insert(entry.0.clone(), entry.1.clone());
            if is_split_after_key(entry.0, current_node.len()) {
                result.push(EditableInternalNode::create(current_node).unwrap());
                current_node = BTreeMap::new();
            }
        }
        if !current_node.is_empty() {
            result.push(EditableInternalNode::create(current_node).unwrap());
        }
        *self = result.remove(0);
        result
    }

    pub fn top_key(&self) -> &Key {
        self.entries
            .keys()
            .next_back()
            .expect("internal node is not empty")
    }

    pub async fn find(
        &mut self,
        key: &Key,
        _load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        // TODO: optimize search
        for (entry_key, entry_value) in self.entries.iter_mut() {
            if key <= entry_key {
                return Box::pin(entry_value.find(key, _load_tree)).await;
            }
        }
        Ok(None)
    }

    pub async fn verify_integrity(
        &mut self,
        _is_final_node: bool,
        load_tree: &dyn LoadTree,
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        let last_index = self.entries.len() - 1;
        let mut child_depth = None;
        for (index, (key, value)) in self.entries.iter_mut().enumerate() {
            match value
                .verify_integrity(key, index == last_index, load_tree)
                .await?
            {
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

    pub fn is_naturally_split(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let last_key = self
            .entries
            .keys()
            .last()
            .expect("internal node is not empty");
        Ok(is_split_after_key(last_key, self.entries.len()))
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
                    entries.insert(key, EditableNode::Reference(child_node));
                }
                EditableLoadedNode::Internal(EditableInternalNode { entries })
            }
        }
    }

    pub async fn insert(
        &mut self,
        key: Key,
        value: Value,
        load_tree: &dyn LoadTree,
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
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.remove(key).await,
            EditableLoadedNode::Internal(internal_node) => {
                internal_node.remove(key, load_tree).await
            }
        }
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.find(key)),
            EditableLoadedNode::Internal(internal_node) => internal_node.find(key, load_tree).await,
        }
    }

    pub fn top_key(&self) -> &Key {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.top_key(),
            EditableLoadedNode::Internal(internal_node) => internal_node.top_key(),
        }
    }

    pub async fn size(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.entries.len() as u64),
            EditableLoadedNode::Internal(internal_node) => {
                let mut total_size = 0;
                for child_node in internal_node.entries.values_mut() {
                    total_size += child_node.size(load_tree).await?;
                }
                Ok(total_size)
            }
        }
    }

    pub async fn save(
        &mut self,
        store_tree: &dyn StoreTree,
    ) -> Result<BlobDigest, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => {
                let mut new_node = crate::sorted_tree::Node {
                    entries: Vec::new(),
                };
                for (key, value) in &leaf_node.entries {
                    new_node.entries.push((key.clone(), value.clone()));
                }
                let digest = crate::prolly_tree::store_node(
                    store_tree,
                    &new_node,
                    &crate::prolly_tree::Metadata { is_leaf: true },
                )
                .await?;
                Ok(digest)
            }
            EditableLoadedNode::Internal(internal_node) => {
                let mut new_node = crate::sorted_tree::Node {
                    entries: Vec::new(),
                };
                for (key, child_node) in &mut internal_node.entries {
                    let child_digest = Box::pin(child_node.save(store_tree)).await?;
                    new_node
                        .entries
                        .push((key.clone(), TreeReference::new(child_digest)));
                }
                let digest = crate::prolly_tree::store_node(
                    store_tree,
                    &new_node,
                    &crate::prolly_tree::Metadata { is_leaf: false },
                )
                .await?;
                Ok(digest)
            }
        }
    }

    pub async fn verify_integrity(
        &mut self,
        is_final_node: bool,
        load_tree: &dyn LoadTree,
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => leaf_node.verify_integrity(is_final_node),
            EditableLoadedNode::Internal(internal_node) => {
                internal_node
                    .verify_integrity(is_final_node, load_tree)
                    .await
            }
        }
    }
}
