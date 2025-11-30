use crate::prolly_tree_editable_node::{EditableNode, IntegrityCheckResult};
use astraea::{storage::InMemoryTreeStorage, tree::BlobDigest};
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
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

#[test_log::test(tokio::test)]
async fn test_insert_flat_values_one_at_a_time() {
    let number_of_keys = 200;
    let expected_trees_created = 8;
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<String, i64> = EditableNode::new();
    let mut all_entries = Vec::new();
    for index in 0..number_of_keys {
        let key = format!("key-{index}");
        let value = index as i64;
        all_entries.push((key, value));
    }
    {
        let mut random = SmallRng::seed_from_u64(123);
        all_entries.shuffle(&mut random);
    }
    let mut expected_entries: BTreeMap<String, i64> = BTreeMap::new();
    for (key, value) in all_entries.iter() {
        {
            let existing_entry = editable_node.find(key, &storage).await.unwrap();
            let expected_entry = expected_entries.get(key);
            assert_eq!(expected_entry.copied(), existing_entry);
        }
        let trees_before = storage.number_of_trees().await;
        editable_node
            .insert(key.clone(), *value, &storage)
            .await
            .expect("inserting key should succeed");
        let trees_after = storage.number_of_trees().await;
        assert_eq!(trees_after, trees_before);
        expected_entries.insert(key.clone(), *value);
        assert_eq!(
            expected_entries.len() as u64,
            editable_node.size(&storage).await.unwrap()
        );
        for (key, value) in expected_entries.iter() {
            let found = editable_node.find(key, &storage).await.unwrap();
            assert_eq!(Some(*value), found);
        }
        let expected_top_key = expected_entries.keys().next_back().unwrap();
        match editable_node
            .verify_integrity(expected_top_key, true, &storage)
            .await
            .unwrap()
        {
            IntegrityCheckResult::Valid { depth } => {
                assert!(depth < 10);
            }
            other => panic!("Expected valid integrity check result, got {:?}", other),
        }
    }
    let expected_top_key = expected_entries.keys().next_back().unwrap();
    assert_eq!(
        IntegrityCheckResult::Valid { depth: 1 },
        editable_node
            .verify_integrity(expected_top_key, true, &storage)
            .await
            .unwrap()
    );
    assert_eq!(0, storage.number_of_trees().await);
    editable_node.save(&storage).await.unwrap();
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(expected_trees_created, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    assert_eq!(
        IntegrityCheckResult::Valid { depth: 1 },
        editable_node
            .verify_integrity(expected_top_key, true, &storage)
            .await
            .unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn test_remove_something() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    editable_node.insert(1, 10, &storage).await.unwrap();
    editable_node.insert(2, 20, &storage).await.unwrap();
    editable_node.insert(3, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
    assert_eq!(Some(20), editable_node.remove(&2, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(2, editable_node.size(&storage).await.unwrap());
}

#[test_log::test(tokio::test)]
async fn test_remove_nothing() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    editable_node.insert(1, 10, &storage).await.unwrap();
    editable_node.insert(2, 20, &storage).await.unwrap();
    editable_node.insert(3, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
    assert_eq!(None, editable_node.remove(&4, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(3, editable_node.size(&storage).await.unwrap());
}
