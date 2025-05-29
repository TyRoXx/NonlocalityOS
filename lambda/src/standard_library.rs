use astraea::{deep_tree::DeepTree, tree::TreeBlob};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleOutput {
    pub message: DeepTree,
}

impl ConsoleOutput {
    pub fn to_tree(&self) -> DeepTree {
        DeepTree::new(TreeBlob::empty(), vec![self.message.clone()])
    }

    pub fn from_tree(tree: &DeepTree) -> Option<ConsoleOutput> {
        if !tree.blob().is_empty() {
            return None;
        }
        if tree.references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: tree.references()[0].clone(),
        })
    }
}
