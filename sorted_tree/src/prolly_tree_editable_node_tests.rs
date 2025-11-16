use crate::prolly_tree_editable_node::EditableNode;
use astraea::{storage::InMemoryTreeStorage, tree::BlobDigest};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[test_log::test(tokio::test)]
async fn test_insert() {
    let store_tree = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    editable_node
        .insert(&[(1, 10), (2, 20), (3, 30)], &store_tree)
        .await
        .unwrap();
    let digest = editable_node.save(&store_tree).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
}
