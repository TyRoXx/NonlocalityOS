use astraea::tree::{BlobDigest, Tree, TreeBlob};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleOutput {
    pub message: BlobDigest,
}

impl ConsoleOutput {
    pub fn to_value(&self) -> Tree {
        Tree::new(TreeBlob::empty(), vec![self.message])
    }

    pub fn from_value(value: &Tree) -> Option<ConsoleOutput> {
        if value.blob().len() != 0 {
            return None;
        }
        if value.references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: value.references()[0],
        })
    }
}
