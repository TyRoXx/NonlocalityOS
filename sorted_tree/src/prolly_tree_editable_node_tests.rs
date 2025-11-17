use crate::prolly_tree_editable_node::EditableNode;
use astraea::{storage::InMemoryTreeStorage, tree::BlobDigest};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[test_log::test(tokio::test)]
async fn test_insert() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    assert_eq!(None, editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(0, editable_node.size(&storage).await.unwrap());
    editable_node
        .insert(&[(1, 10), (2, 20), (3, 30)], &storage)
        .await
        .unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(3, editable_node.size(&storage).await.unwrap());
}
