use crate::{
    storage::LoadTree,
    tree::{BlobDigest, TreeBlob},
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

    pub fn try_from_string(value: &str) -> Option<DeepTree> {
        Some(DeepTree::new(
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
}
