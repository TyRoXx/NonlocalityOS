use astraea::{
    deep_tree::{DeepTree, DeepTreeChildren},
    tree::TreeBlob,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConsoleOutput {
    pub message: DeepTree,
}

impl ConsoleOutput {
    pub fn to_tree(&self) -> DeepTree {
        DeepTree::new(
            TreeBlob::empty(),
            DeepTreeChildren::try_from(vec![self.message.clone()]).expect("One child always fits"),
        )
    }

    pub fn from_tree(tree: &DeepTree) -> Option<ConsoleOutput> {
        if !tree.blob().is_empty() {
            return None;
        }
        if tree.children().references().len() != 1 {
            return None;
        }
        Some(ConsoleOutput {
            message: tree.children().references()[0].clone(),
        })
    }
}
