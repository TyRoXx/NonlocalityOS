use crate::{
    storage::{LoadError, LoadTree, StoreError, StoreTree, StrongReference},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeSerializationError},
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeepTreeChildren {
    references: Vec<DeepTree>,
}

impl DeepTreeChildren {
    pub fn empty() -> DeepTreeChildren {
        DeepTreeChildren {
            references: Vec::new(),
        }
    }

    pub fn try_from(references: Vec<DeepTree>) -> Option<DeepTreeChildren> {
        if references.len() > crate::tree::TREE_MAX_CHILDREN {
            return None;
        }
        Some(DeepTreeChildren { references })
    }

    pub fn references(&self) -> &[DeepTree] {
        &self.references
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeepTree {
    blob: TreeBlob,
    children: DeepTreeChildren,
}

impl DeepTree {
    pub fn new(blob: TreeBlob, children: DeepTreeChildren) -> DeepTree {
        DeepTree { blob, children }
    }

    pub fn empty() -> DeepTree {
        DeepTree {
            blob: TreeBlob::empty(),
            children: DeepTreeChildren::empty(),
        }
    }

    pub fn try_from_string(value: &str) -> Result<DeepTree, TreeSerializationError> {
        Ok(DeepTree::new(
            TreeBlob::try_from(bytes::Bytes::copy_from_slice(value.as_bytes()))?,
            DeepTreeChildren::empty(),
        ))
    }

    pub fn blob(&self) -> &TreeBlob {
        &self.blob
    }

    pub fn children(&self) -> &DeepTreeChildren {
        &self.children
    }

    pub async fn deserialize(
        root: &BlobDigest,
        load_tree: &dyn LoadTree,
    ) -> std::result::Result<DeepTree, LoadError> {
        let tree = match load_tree.load_tree(root).await?.hash() {
            Some(hashed_tree) => hashed_tree,
            None => {
                return Err(LoadError::TreeNotFound(*root));
            }
        };
        let blob = tree.hashed_tree().tree().blob();
        let mut references = Vec::new();
        for reference in tree.hashed_tree().tree().children().references() {
            let deep_tree = Box::pin(DeepTree::deserialize(reference.digest(), load_tree)).await?;
            references.push(deep_tree);
        }
        Ok(DeepTree::new(
            blob.clone(),
            DeepTreeChildren::try_from(references)
                .expect("Max child count enforced by TreeChildren"),
        ))
    }

    pub async fn serialize(
        &self,
        store_tree: &dyn StoreTree,
    ) -> Result<StrongReference, StoreError> {
        let mut references = Vec::new();
        for reference in self.children().references() {
            references.push(Box::pin(reference.serialize(store_tree)).await?);
        }
        let tree = Arc::new(Tree::new(
            self.blob.clone(),
            crate::tree::TreeChildren::try_from(references)
                .expect("Max child count enforced by DeepTreeChildren"),
        ));
        store_tree.store_tree(&HashedTree::from(tree)).await
    }
}
