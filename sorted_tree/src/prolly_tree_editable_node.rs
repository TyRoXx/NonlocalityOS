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
        let loaded = self.require_loaded(load_tree).await?;
        loaded.insert(key, value).await
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
    ) -> Result<usize, Box<dyn std::error::Error>> {
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
        key: Key,
        value: Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }

    pub async fn find(
        &mut self,
        key: &Key,
        load_tree: &dyn LoadTree,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
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
        key: Key,
        value: Value,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => {
                leaf_node.entries.insert(key, value);
                Ok(())
            }
            EditableLoadedNode::Internal(internal_node) => internal_node.insert(key, value).await,
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

    pub async fn size(
        &mut self,
        load_tree: &dyn LoadTree,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        match self {
            EditableLoadedNode::Leaf(leaf_node) => Ok(leaf_node.entries.len()),
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
}
