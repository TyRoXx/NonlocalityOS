use crate::{
    prolly_tree::{load_node, EitherNodeType},
    sorted_tree::{NodeValue, TreeReference},
};
use astraea::{
    storage::{LoadTree, StoreTree},
    tree::BlobDigest,
};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;

#[derive(Debug)]
pub enum EditableNode<Key, Value> {
    Reference(TreeReference),
    Loaded(EditableLoadedNode<Key, Value>),
}

impl<Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone, Value: NodeValue + Clone>
    EditableNode<Key, Value>
{
    pub async fn insert(
        &mut self,
        entries: &[(Key, Value)],
        load_tree: &dyn LoadTree,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let loaded = match self {
            EditableNode::Reference(tree_ref) => {
                let loaded: EitherNodeType<Key, Value> =
                    load_node(load_tree, tree_ref.reference()).await.unwrap(/*TODO */);
                *self = EditableNode::Loaded(EditableLoadedNode::new(loaded));
                todo!()
            }
            EditableNode::Loaded(loaded_node) => loaded_node,
        };
        loaded.insert(entries).await
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn size(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn normalize(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn save(
        &mut self,
        store_tree: &dyn StoreTree,
    ) -> Result<BlobDigest, Box<dyn std::error::Error>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct EditableLeafNode<Key, Value> {
    entries: BTreeMap<Key, Value>,
}

#[derive(Debug)]
pub struct EditableInternalNode<Key, Value> {
    entries: BTreeMap<Key, EditableNode<Key, Value>>,
}

impl<Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone, Value: NodeValue + Clone>
    EditableInternalNode<Key, Value>
{
    pub async fn insert(
        &mut self,
        entries: &[(Key, Value)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

#[derive(Debug)]
pub enum EditableLoadedNode<Key, Value> {
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
        entries: &[(Key, Value)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => {
                for (key, value) in entries {
                    leaf_node.entries.insert(key.clone(), value.clone());
                }
                todo!()
            }
            EditableLoadedNode::Internal(internal_node) => internal_node.insert(entries).await,
        }
    }
}
