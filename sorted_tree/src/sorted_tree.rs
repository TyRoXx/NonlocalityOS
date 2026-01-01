use astraea::{
    storage::{LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TreeSerializationError},
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

    pub fn reference(&self) -> &BlobDigest {
        &self.reference
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

#[derive(Serialize, Deserialize, Clone, Hash, PartialEq, Debug)]
pub struct Node<Key: Serialize + Ord, Value: NodeValue> {
    /// sorted by Key
    pub entries: Vec<(Key, Value)>,
}

impl<Key: Serialize + Ord + Clone, Value: NodeValue + Clone> Default for Node<Key, Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Key: Serialize + Ord + Clone, Value: NodeValue + Clone> Node<Key, Value> {
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

    pub fn replace_chunk(&mut self, index: usize, new_chunks: &[(Key, Value)]) {
        self.entries
            .splice(index..=index, new_chunks.iter().cloned());
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

pub fn node_to_tree<Key: Serialize + Ord, Value: NodeValue>(
    node: &Node<Key, Value>,
    metadata: &bytes::Bytes,
) -> Result<Tree, TreeSerializationError> {
    let serializable_node_content = SerializableNodeContent {
        entries: node
            .entries
            .iter()
            .map(|(key, value)| (key, value.to_content()))
            .collect(),
    };
    let references: Vec<_> = node
        .entries
        .iter()
        .filter_map(|(_key, value)| value.get_reference())
        .collect();
    let children = match TreeChildren::try_from(references) {
        Some(children) => children,
        None => return Err(TreeSerializationError::TooManyChildren),
    };
    let mut buffer = Vec::from_iter(metadata.iter().cloned());
    postcard::to_io(&serializable_node_content, &mut buffer)
        .expect("serializing a node should always work");
    let blob = TreeBlob::try_from(bytes::Bytes::from(buffer))?;
    Ok(Tree::new(blob, children))
}

pub async fn store_node<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &(dyn StoreTree + Send + Sync),
    node: &Node<Key, Value>,
    metadata: &bytes::Bytes,
) -> Result<BlobDigest, StoreError> {
    let tree = match node_to_tree(node, metadata) {
        Ok(tree) => tree,
        Err(error) => return Err(StoreError::TreeSerializationError(error)),
    };
    store_tree
        .store_tree(&HashedTree::from(std::sync::Arc::new(tree)))
        .await
}

pub fn node_from_tree<Key: Serialize + DeserializeOwned + Ord, Value: NodeValue>(
    tree: &Tree,
    metadata_to_skip: usize,
) -> Node<Key, Value> {
    let node = postcard::from_bytes::<SerializableNodeContent<Key, Value::Content>>(
        tree.blob().as_slice().split_at(metadata_to_skip).1,
    )
    .expect("this should always work");
    if !node.entries.is_sorted_by_key(|element| &element.0) {
        todo!("loaded node is not sorted");
    }
    let mut reference_iter = tree.children().references().iter();
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
        Ok(tree) => tree,
        Err(_) => todo!(),
    };
    let hashed_tree = match delayed_hashed_tree.hash() {
        Some(tree) => tree,
        None => todo!(),
    };
    node_from_tree::<Key, Value>(hashed_tree.tree(), 0)
}

pub async fn new_tree<Key: Serialize + Ord, Value: NodeValue>(
    store_tree: &(dyn StoreTree + Send + Sync),
) -> Result<BlobDigest, StoreError> {
    let root = Node::<Key, Value> {
        entries: Vec::new(),
    };
    store_node(store_tree, &root,  /*this function is only used by sorted_tree_tests, so we don't need the prolly_tree metadata*/ &bytes::Bytes::new()).await
}

pub async fn insert<Key: Serialize + DeserializeOwned + Ord + Clone, Value: NodeValue + Clone>(
    load_tree: &(dyn LoadTree + Send + Sync),
    store_tree: &(dyn StoreTree + Send + Sync),
    root: &BlobDigest,
    key: Key,
    value: Value,
) -> Result<BlobDigest, StoreError> {
    let mut node = load_node::<Key, Value>(load_tree, root).await;
    node.insert(key, value);
    store_node(store_tree, &node, /*this function is only used by sorted_tree_tests, so we don't need the prolly_tree metadata*/ &bytes::Bytes::new()).await
}

pub async fn find<
    Key: Serialize + DeserializeOwned + PartialEq + Ord + Clone,
    Value: NodeValue + Clone,
>(
    load_tree: &dyn LoadTree,
    root: &BlobDigest,
    key: &Key,
) -> Option<Value> {
    let node = load_node::<Key, Value>(load_tree, root).await;
    node.find(key)
}
