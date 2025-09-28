use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub trait NodeValue {
    type Content: Serialize + DeserializeOwned;

    fn has_child(content: &Self::Content) -> bool;
    fn from_content(content: Self::Content, child: &Option<BlobDigest>) -> Self;
    fn to_content(&self) -> Self::Content;
    fn get_reference(&self) -> Option<BlobDigest>;
}

impl<T> NodeValue for T
where
    T: Serialize + DeserializeOwned + Clone,
{
    type Content = T;

    fn has_child(_content: &Self::Content) -> bool {
        false
    }

    fn from_content(content: Self::Content, child: &Option<BlobDigest>) -> Self {
        assert!(child.is_none());
        content
    }

    fn to_content(&self) -> Self::Content {
        self.clone()
    }

    fn get_reference(&self) -> Option<BlobDigest> {
        None
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct TreeReference {
    reference: BlobDigest,
}

impl TreeReference {
    pub fn new(reference: BlobDigest) -> Self {
        Self { reference }
    }
}

impl NodeValue for TreeReference {
    type Content = ();

    fn has_child(_content: &Self::Content) -> bool {
        true
    }

    fn from_content(_content: Self::Content, child: &Option<BlobDigest>) -> Self {
        match child {
            Some(reference) => TreeReference {
                reference: *reference,
            },
            None => todo!("node claims to have a child, but no reference is available"),
        }
    }

    fn to_content(&self) -> Self::Content {}

    fn get_reference(&self) -> Option<BlobDigest> {
        Some(self.reference)
    }
}

#[derive(Serialize, Deserialize, Clone, Hash)]
pub struct Node<Key: Serialize + Ord, Value: NodeValue> {
    /// sorted by Key
    entries: Vec<(Key, Value)>,
}

impl<Key: Serialize + Ord, Value: NodeValue + Clone> Default for Node<Key, Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key: Serialize + Ord, Value: NodeValue + Clone> Node<Key, Value> {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn entries(&self) -> &Vec<(Key, Value)> {
        &self.entries
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        let partition_point = self.entries.partition_point(|element| element.0 < key);
        if partition_point < self.entries.len() && self.entries[partition_point].0 == key {
            self.entries[partition_point].1 = value;
        } else {
            self.entries.insert(partition_point, (key, value));
        }
    }

    pub fn find(&self, key: &Key) -> Option<Value> {
        match self
            .entries
            .binary_search_by_key(&key, |element| &element.0)
        {
            Ok(index) => Some(self.entries[index].1.clone()),
            Err(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Hash)]
pub struct SerializableNodeContent<Key: Serialize + Ord, Value: Serialize> {
    /// sorted by Key
    entries: Vec<(Key, Value)>,
}

impl<Key: Serialize + Ord, Value: Serialize> SerializableNodeContent<Key, Value> {
    pub fn entries(&self) -> &Vec<(Key, Value)> {
        &self.entries
    }
}

pub fn node_to_tree<Key: Serialize + Ord, Value: NodeValue>(node: &Node<Key, Value>) -> Tree {
    let serializable_node_content = SerializableNodeContent {
        entries: node
            .entries
            .iter()
            .map(|(key, value)| (key, value.to_content()))
            .collect(),
    };
    // TODO: check number of references
    let references: Vec<_> = node
        .entries
        .iter()
        .filter_map(|(_key, value)| value.get_reference())
        .collect();
    Tree::new(
        TreeBlob::try_from(bytes::Bytes::from(
            postcard::to_stdvec(&serializable_node_content)
                .expect("serializing a new tree should always succeed"),
        ))
        .expect("this should always fit"),
        references,
    )
}

pub async fn store_node<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &dyn StoreTree,
    node: &Node<Key, Value>,
) -> Result<BlobDigest, StoreError> {
    let tree = node_to_tree(node);
    store_tree
        .store_tree(&HashedTree::from(std::sync::Arc::new(tree)))
        .await
}

pub fn node_from_tree<Key: Serialize + DeserializeOwned + Ord, Value: NodeValue>(
    tree: &Tree,
) -> Node<Key, Value> {
    let node = postcard::from_bytes::<SerializableNodeContent<Key, Value::Content>>(
        tree.blob().as_slice(),
    )
    .expect("this should always work");
    if !node.entries.is_sorted_by_key(|element| &element.0) {
        todo!("loaded node is not sorted");
    }
    let mut reference_iter = tree.references().iter();
    let result = Node {
        entries: node
            .entries
            .into_iter()
            .map(|(key, content)| {
                if Value::has_child(&content) {
                    let reference = match reference_iter.next() {
                        Some(reference) => Some(*reference),
                        None => todo!("node claims to have a child, but no reference is available"),
                    };
                    (key, Value::from_content(content, &reference))
                } else {
                    (key, Value::from_content(content, &None))
                }
            })
            .collect(),
    };
    if reference_iter.next().is_some() {
        todo!("more references available than expected")
    }
    result
}

pub async fn load_node<Key: Serialize + DeserializeOwned + Ord, Value: NodeValue>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
) -> Node<Key, Value> {
    let delayed_hashed_tree = match load_tree.load_tree(root).await {
        Some(tree) => tree,
        None => todo!(),
    };
    let hashed_tree = match delayed_hashed_tree.hash() {
        Some(tree) => tree,
        None => todo!(),
    };
    node_from_tree::<Key, Value>(hashed_tree.tree())
}

pub async fn new_tree<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &dyn StoreTree,
) -> Result<BlobDigest, StoreError> {
    let root = Node::<Key, Value> {
        entries: Vec::new(),
    };
    store_node(store_tree, &root).await
}

pub async fn insert<Key: Serialize + DeserializeOwned + Ord, Value: NodeValue + Clone>(
    load_tree: &dyn LoadTree,
    store_tree: &dyn StoreTree,
    root: &BlobDigest,
    key: Key,
    value: Value,
) -> Result<BlobDigest, StoreError> {
    let mut node = load_node::<Key, Value>(load_tree, root).await;
    node.insert(key, value);
    store_node(store_tree, &node).await
}

pub async fn find<Key: Serialize + DeserializeOwned + PartialEq + Ord, Value: NodeValue + Clone>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
    key: &Key,
) -> Option<Value> {
    let node = load_node::<Key, Value>(load_tree, root).await;
    node.find(key)
}
