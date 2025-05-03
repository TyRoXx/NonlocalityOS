use astraea::tree::{BlobDigest, Tree, TreeBlob};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleOutput {
    pub message: BlobDigest,
}

impl ConsoleOutput {
    pub fn to_tree(&self) -> Tree {
        Tree::new(TreeBlob::empty(), vec![self.message])
    }

    pub fn from_tree(tree: &Tree) -> Option<ConsoleOutput> {
        if tree.blob().len() != 0 {
            return None;
        }
        if tree.references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: tree.references()[0],
        })
    }
}
