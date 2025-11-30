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

impl<Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone, Value: NodeValue + Clone>
    EditableNode<Key, Value>
{
    pub fn new() -> Self {
        EditableNode::Loaded(EditableLoadedNode::Leaf(EditableLeafNode {
            entries: BTreeMap::new(),
        }))
    }

    pub fn from_reference(reference: TreeReference) -> Self {
        EditableNode::Reference(reference)
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
            EditableNode::Reference(tree_ref) => Ok(tree_ref.reference().clone()),
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

    pub async fn find(
        &mut self,
        _key: &Key,
        _load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        todo!()
    }

    pub fn verify_integrity(
        &mut self,
        is_final_node: bool,
    ) -> Result<IntegrityCheckResult, Box<dyn std::error::Error>> {
        for (index, key) in self.entries.keys().enumerate() {
            let is_split = is_split_after_key(key, index + 1);
            if index == self.entries.len() - 1 {
                if !is_final_node && !is_split {
                    return Ok(IntegrityCheckResult::Corrupted(format!(
                        "Leaf node integrity check failed: Final key does not indicate split"
                    )));
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
}

#[derive(Debug, Clone)]
pub struct EditableInternalNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    entries: BTreeMap<Key, EditableNode<Key, Value>>,
}

impl<Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone, Value: NodeValue + Clone>
    EditableInternalNode<Key, Value>
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
}

#[derive(Debug, Clone)]
pub enum EditableLoadedNode<Key: std::cmp::Ord + Clone, Value: Clone> {
    Leaf(EditableLeafNode<Key, Value>),
    Internal(EditableInternalNode<Key, Value>),
}

impl<Key: Serialize + DeserializeOwned + Ord + Clone, Value: NodeValue + Clone>
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
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.entries.get(key).cloned()),
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
                for (_key, child_node) in &mut internal_node.entries {
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
