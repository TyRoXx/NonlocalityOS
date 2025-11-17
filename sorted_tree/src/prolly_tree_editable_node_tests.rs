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

    editable_node.insert(1, 10, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "f0d2a7718960d780619fe153a35b346db4ebf4dddf16cf0c6fa5b250adb9c48b120528530ddb814c68bda69ed880bce1fb29d54bb1386e00917e387ddf3497e3"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.size(&storage).await.unwrap());

    editable_node.insert(3, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "e905a3323cd8e425b4e490641fbfea34cffaa241a18f861d01affe203f721fd46ad7414c3f356d56e716585249c5964876f9d6c51aa76738d008efc8dd4cdeb8"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(2, editable_node.size(&storage).await.unwrap());

    editable_node.insert(2, 20, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(3, editable_node.size(&storage).await.unwrap());

    editable_node.insert(0, 0, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0ff5b2a71bead5718efeef5db61ecd7103056421dc962ac01af44e65696b8f3eff0c569048ebe54e2d60feefa57c3462e84336fe72b282aebd502f34f48ceb28"
        ).expect("valid digest"), digest);
    assert_eq!(Some(0), editable_node.find(&0, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(4, editable_node.size(&storage).await.unwrap());
}

#[test_log::test(tokio::test)]
async fn test_insert_overwrite() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    assert_eq!(None, editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(0, editable_node.size(&storage).await.unwrap());

    editable_node.insert(1, 10, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "f0d2a7718960d780619fe153a35b346db4ebf4dddf16cf0c6fa5b250adb9c48b120528530ddb814c68bda69ed880bce1fb29d54bb1386e00917e387ddf3497e3"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.size(&storage).await.unwrap());

    editable_node.insert(1, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "e62488a51cc8730d07ae57de8a4052bd03fac835f0a02df5cad6e0d292326b89e63740e1339cffd36cf3a2ed4789d0678ff3f39a74134934de07da4782bc129a"
        ).expect("valid digest"), digest);
    assert_eq!(Some(30), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.size(&storage).await.unwrap());
}
