use std::sync::Arc;

use crate::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeSerializationError},
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeepTree {
    blob: TreeBlob,
    references: Vec<DeepTree>,
}

impl DeepTree {
    pub fn new(blob: TreeBlob, references: Vec<DeepTree>) -> DeepTree {
        DeepTree { blob, references }
    }

    pub fn empty() -> DeepTree {
        DeepTree {
            blob: TreeBlob::empty(),
            references: Vec::new(),
        }
    }

    pub fn try_from_string(value: &str) -> Result<DeepTree, TreeSerializationError> {
        Ok(DeepTree::new(
            TreeBlob::try_from(bytes::Bytes::copy_from_slice(value.as_bytes()))?,
            Vec::new(),
        ))
    }

    pub fn blob(&self) -> &TreeBlob {
        &self.blob
    }

    pub fn references(&self) -> &[DeepTree] {
        &self.references
    }

    pub async fn deserialize(root: &BlobDigest, load_tree: &dyn LoadTree) -> Option<DeepTree> {
        let tree = load_tree.load_tree(root).await?.hash()?;
        let blob = tree.tree().blob();
        let mut references = Vec::new();
        for reference in tree.tree().references() {
            if let Some(deep_tree) = Box::pin(DeepTree::deserialize(reference, load_tree)).await {
                references.push(deep_tree);
            } else {
                return None;
            }
        }
        Some(DeepTree::new(blob.clone(), references))
    }

    pub async fn serialize(&self, store_tree: &dyn StoreTree) -> Result<BlobDigest, StoreError> {
        let mut references = Vec::new();
        for reference in &self.references {
            references.push(Box::pin(reference.serialize(store_tree)).await?);
        }
        let tree = Arc::new(Tree::new(self.blob.clone(), references));
        store_tree.store_tree(&HashedTree::from(tree)).await
    }
}
