use crate::{
    prolly_tree_editable_node::{
        hash_key, EditableLeafNode, EditableNode, IntegrityCheckResult, Iterator,
    },
    sorted_tree::TreeReference,
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    tree::{BlobDigest, TREE_BLOB_MAX_LENGTH},
};
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[test_log::test]
fn test_hash_key() {
    let value = 42;
    assert_eq!(hash_key(&value), 8);
    // it returns the same result every time
    assert_eq!(hash_key(&value), 8);
}

async fn test_save_load_roundtrip<
    Key: std::cmp::Ord + Clone + Serialize + DeserializeOwned + std::fmt::Debug,
    Value: Clone + Serialize + DeserializeOwned,
>(
    node: &mut EditableNode<Key, Value>,
    storage: &InMemoryTreeStorage,
    expected_digest: &BlobDigest,
) {
    let digest = node.save(storage).await.unwrap();
    let mut loaded_node: EditableNode<Key, Value> =
        EditableNode::Reference(TreeReference::new(digest));
    let saved_again = loaded_node.save(storage).await.unwrap();
    assert_eq!(digest, saved_again);
    assert_eq!(digest, *expected_digest);
}

#[test_log::test(tokio::test)]
async fn test_insert() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    assert_eq!(None, editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(0, editable_node.count(&storage).await.unwrap());

    editable_node.insert(1, 10, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "f0d2a7718960d780619fe153a35b346db4ebf4dddf16cf0c6fa5b250adb9c48b120528530ddb814c68bda69ed880bce1fb29d54bb1386e00917e387ddf3497e3"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.count(&storage).await.unwrap());

    editable_node.insert(3, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "e905a3323cd8e425b4e490641fbfea34cffaa241a18f861d01affe203f721fd46ad7414c3f356d56e716585249c5964876f9d6c51aa76738d008efc8dd4cdeb8"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(2, editable_node.count(&storage).await.unwrap());

    editable_node.insert(2, 20, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(3, editable_node.count(&storage).await.unwrap());

    editable_node.insert(0, 0, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "0ff5b2a71bead5718efeef5db61ecd7103056421dc962ac01af44e65696b8f3eff0c569048ebe54e2d60feefa57c3462e84336fe72b282aebd502f34f48ceb28"
        ).expect("valid digest"), digest);
    assert_eq!(Some(0), editable_node.find(&0, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(4, editable_node.count(&storage).await.unwrap());

    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
}

#[test_log::test(tokio::test)]
async fn test_insert_overwrite() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    assert_eq!(None, editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(0, editable_node.count(&storage).await.unwrap());

    editable_node.insert(1, 10, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "f0d2a7718960d780619fe153a35b346db4ebf4dddf16cf0c6fa5b250adb9c48b120528530ddb814c68bda69ed880bce1fb29d54bb1386e00917e387ddf3497e3"
        ).expect("valid digest"), digest);
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.count(&storage).await.unwrap());

    editable_node.insert(1, 30, &storage).await.unwrap();
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "e62488a51cc8730d07ae57de8a4052bd03fac835f0a02df5cad6e0d292326b89e63740e1339cffd36cf3a2ed4789d0678ff3f39a74134934de07da4782bc129a"
        ).expect("valid digest"), digest);
    assert_eq!(Some(30), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(1, editable_node.count(&storage).await.unwrap());

    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
}

async fn test_insert_flat_values_one_at_a_time(
    number: usize,
    seed: u64,
    expected_depth: usize,
    expected_trees_created: usize,
) -> BlobDigest {
    let number_of_keys = number;
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<String, i64> = EditableNode::new();
    let mut all_entries = Vec::new();
    for index in 0..number_of_keys {
        let key = format!("key-{index}");
        let value = index as i64;
        all_entries.push((key, value));
    }
    {
        let mut random = SmallRng::seed_from_u64(seed);
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
            editable_node.count(&storage).await.unwrap()
        );
        for (key, value) in expected_entries.iter() {
            let found = editable_node.find(key, &storage).await.unwrap();
            assert_eq!(Some(*value), found);
        }
    }
    let expected_top_key = expected_entries.keys().next_back();
    assert_eq!(
        IntegrityCheckResult::Valid {
            depth: expected_depth
        },
        editable_node
            .verify_integrity(expected_top_key, &storage)
            .await
            .unwrap()
    );
    assert_eq!(0, storage.number_of_trees().await);
    let digest = editable_node.save(&storage).await.unwrap();
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(expected_trees_created, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    assert_eq!(
        IntegrityCheckResult::Valid {
            depth: expected_depth
        },
        editable_node
            .verify_integrity(expected_top_key, &storage)
            .await
            .unwrap()
    );
    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
    digest
}

#[test_log::test(tokio::test)]
async fn test_insert_flat_values_one_at_a_time_200() {
    let first_digest = test_insert_flat_values_one_at_a_time(200, 123, 1, 3).await;
    let second_digest = test_insert_flat_values_one_at_a_time(200, 124, 1, 3).await;
    assert_eq!(first_digest, second_digest);
}

#[test_log::test(tokio::test)]
async fn test_insert_flat_values_one_at_a_time_1000() {
    let first_digest = test_insert_flat_values_one_at_a_time(1000, 123, 1, 10).await;
    let second_digest = test_insert_flat_values_one_at_a_time(1000, 124, 1, 10).await;
    assert_eq!(first_digest, second_digest);
}

#[test_log::test(tokio::test)]
async fn test_insert_large_elements() {
    // Large enough to cause frequent chunk splitting to avoid going over the TREE_BLOB_MAX_LENGTH.
    let large_value = vec![0u8; TREE_BLOB_MAX_LENGTH / 4];
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u64, Vec<u8>> = EditableNode::new();
    editable_node
        .insert(4001, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(1, storage.number_of_trees().await);
    editable_node
        .insert(4002, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(3, storage.number_of_trees().await);
    editable_node
        .insert(4003, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(5, storage.number_of_trees().await);
    editable_node
        .insert(4004, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(7, storage.number_of_trees().await);
    editable_node
        .insert(4005, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(9, storage.number_of_trees().await);
    editable_node
        .insert(4006, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(11, storage.number_of_trees().await);
    editable_node
        .insert(4007, large_value.clone(), &storage)
        .await
        .unwrap();
    editable_node.save(&storage).await.unwrap();
    assert_eq!(13, storage.number_of_trees().await);
}

#[test_log::test(tokio::test)]
async fn test_remove_something() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    editable_node.insert(1, 10, &storage).await.unwrap();
    editable_node.insert(2, 20, &storage).await.unwrap();
    editable_node.insert(3, 30, &storage).await.unwrap();
    assert_eq!(Some(20), editable_node.remove(&2, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(None, editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(2, editable_node.count(&storage).await.unwrap());
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "e905a3323cd8e425b4e490641fbfea34cffaa241a18f861d01affe203f721fd46ad7414c3f356d56e716585249c5964876f9d6c51aa76738d008efc8dd4cdeb8"
        ).expect("valid digest"), digest);

    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
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
    assert_eq!(None, editable_node.remove(&0, &storage).await.unwrap());
    assert_eq!(None, editable_node.remove(&4, &storage).await.unwrap());
    assert_eq!(Some(10), editable_node.find(&1, &storage).await.unwrap());
    assert_eq!(Some(20), editable_node.find(&2, &storage).await.unwrap());
    assert_eq!(Some(30), editable_node.find(&3, &storage).await.unwrap());
    assert_eq!(3, editable_node.count(&storage).await.unwrap());

    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
}

#[test_log::test(tokio::test)]
async fn test_remove_many() {
    let seed = 123;
    let number_of_keys = 1000;
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<String, i64> = EditableNode::new();
    let mut all_entries = Vec::new();
    for index in 0..number_of_keys {
        let key = format!("key-{index}");
        let value = index as i64;
        all_entries.push((key, value));
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
            editable_node.count(&storage).await.unwrap()
        );
        for (key, value) in expected_entries.iter() {
            let found = editable_node.find(key, &storage).await.unwrap();
            assert_eq!(Some(*value), found);
        }
    }
    let mut remove_order = all_entries
        .iter()
        .map(|(k, _v)| k.clone())
        .collect::<Vec<String>>();
    {
        let mut random = SmallRng::seed_from_u64(seed);
        remove_order.shuffle(&mut random);
    }
    for removed_key in remove_order.iter() {
        let removed_value = editable_node
            .remove(removed_key, &storage)
            .await
            .expect("removing key should succeed");
        assert_eq!(removed_value, expected_entries.remove(removed_key));
        assert_eq!(
            expected_entries.len() as u64,
            editable_node.count(&storage).await.unwrap()
        );
        let expected_top_key = expected_entries.keys().next_back();
        match editable_node
            .verify_integrity(expected_top_key, &storage)
            .await
            .unwrap()
        {
            IntegrityCheckResult::Valid { depth } => {
                assert!(depth <= 2);
            }
            IntegrityCheckResult::Corrupted(reason) => {
                panic!("Tree integrity check failed: {}", reason);
            }
        }
    }
    for (key, _value) in expected_entries.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(None, found);
    }
    assert_eq!(0, storage.number_of_trees().await);
    let digest = editable_node.save(&storage).await.unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a2701a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ).expect("valid digest"), digest);
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(1, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    assert_eq!(
        IntegrityCheckResult::Valid { depth: 0 },
        editable_node
            .verify_integrity(None, &storage)
            .await
            .unwrap()
    );
    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
}

#[test_log::test(tokio::test)]
async fn test_save_reference() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::default();
    let digest = editable_node.save(&storage).await.unwrap();
    let mut loaded_node: EditableNode<u32, u32> =
        EditableNode::Reference(TreeReference::new(digest));
    let saved_again = loaded_node.save(&storage).await.unwrap();
    assert_eq!(digest, saved_again);
    test_save_load_roundtrip(&mut editable_node, &storage, &digest).await;
}

#[test_log::test(tokio::test)]
async fn test_editable_leaf_node_create() {
    let result = EditableLeafNode::<i32, u32>::create(BTreeMap::new());
    assert!(result.is_none());
}

#[test_log::test(tokio::test)]
async fn test_iterate_empty() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    let digest_before = editable_node.save(&storage).await.unwrap();
    let mut iterator = Iterator::new(&mut editable_node, &storage);
    assert_eq!(None, iterator.next().await.unwrap());
    let digest_after = editable_node.save(&storage).await.unwrap();
    assert_eq!(digest_before, digest_after);
    assert_eq!(BlobDigest::parse_hex_string(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a2701a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ).expect("valid digest"), digest_after);
    test_save_load_roundtrip(&mut editable_node, &storage, &digest_after).await;
}

#[test_log::test(tokio::test)]
async fn test_iterate_small() {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    editable_node.insert(1, 10, &storage).await.unwrap();
    editable_node.insert(2, 20, &storage).await.unwrap();
    editable_node.insert(3, 30, &storage).await.unwrap();
    let digest_before = editable_node.save(&storage).await.unwrap();
    let mut iterator = Iterator::new(&mut editable_node, &storage);
    let mut iterated_elements = BTreeMap::new();
    while let Some((key, value)) = iterator.next().await.unwrap() {
        if let Some(last) = iterated_elements.last_entry() {
            assert!(key > *last.key());
        }
        iterated_elements.insert(key, value);
    }
    assert_eq!(
        BTreeMap::from([(1u32, 10u32), (2u32, 20u32), (3u32, 30u32)]),
        iterated_elements
    );
    let digest_after = editable_node.save(&storage).await.unwrap();
    assert_eq!(digest_before, digest_after);
    assert_eq!(BlobDigest::parse_hex_string(
            "0f0e71ebc25e8b15caa3b91d81f6d36783acd08ff84eb6312024c9f8e739157d270c51100f8610c09093a56c85948793b22217e8c705d03284dbf5e9332cd17e"
        ).expect("valid digest"), digest_after);
    test_save_load_roundtrip(&mut editable_node, &storage, &digest_after).await;
}

#[test_log::test(tokio::test)]
async fn test_iterate_large() {
    let elements: BTreeMap<u32, u32> = (0..500).map(|i| (i, i * 10)).collect();
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, u32> = EditableNode::new();
    for (key, value) in elements.iter() {
        editable_node.insert(*key, *value, &storage).await.unwrap();
    }
    let digest_before = editable_node.save(&storage).await.unwrap();
    let mut iterator = Iterator::new(&mut editable_node, &storage);
    let mut iterated_elements = BTreeMap::new();
    while let Some((key, value)) = iterator.next().await.unwrap() {
        if let Some(last) = iterated_elements.last_entry() {
            assert!(key > *last.key());
        }
        iterated_elements.insert(key, value);
    }
    assert_eq!(elements, iterated_elements);
    let digest_after = editable_node.save(&storage).await.unwrap();
    assert_eq!(digest_before, digest_after);
    assert_eq!(BlobDigest::parse_hex_string(
            "0e770e884e2a246367e71a78e3857153b7680638312717ad19a030992386e7522b0f22ad451e06e0652d6168a6620d5e5f00afda7eb0f40806e0b562db4d1255"
        ).expect("valid digest"), digest_after);
    test_save_load_roundtrip(&mut editable_node, &storage, &digest_after).await;
}
