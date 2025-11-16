use crate::{
    prolly_tree::{
        build_node_hierarchy_from_leaves, default_is_split_after_key, find, insert, insert_many,
        load_in_memory_node, load_node, new_tree, split_node_once, EitherNodeType,
    },
    sorted_tree::TreeReference,
};
use astraea::{
    storage::LoadTree,
    tree::{BlobDigest, Tree, TreeBlob},
};
use pretty_assertions::{assert_eq, assert_ne};
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[test_log::test(tokio::test)]
async fn test_split_node_once_empty() {
    assert_eq!(
        vec![crate::sorted_tree::Node::<u32, TreeReference>::new()],
        split_node_once(
            crate::sorted_tree::Node::<u32, TreeReference>::new(),
            default_is_split_after_key,
        )
    );
}

#[test_log::test(tokio::test)]
async fn test_build_node_hierarchy_from_leaves() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let expected =
        crate::prolly_tree::EitherNodeType::Leaf(crate::sorted_tree::Node::<u32, i32>::new());
    assert_eq!(
        Ok(expected),
        build_node_hierarchy_from_leaves(
            crate::sorted_tree::Node::<u32, i32>::new(),
            default_is_split_after_key,
            &storage,
        )
        .await
    );
}

#[test_log::test(tokio::test)]
async fn new_tree_serialization() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    let loaded = storage
        .load_tree(&empty)
        .await
        .expect("loading the tree should succeed");
    let hashed = loaded.hash().expect("hashing the tree should succeed");
    let tree = hashed.tree();
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from_static(&[
                /*metadata is_leaf: true*/ 1, /*empty list of entries*/ 0
            ]))
            .expect("must succeed"),
            Vec::new()
        ),
        tree.as_ref()
    );
    assert_eq!(
        0,
        crate::prolly_tree::size::<String, i64>(&storage, &empty)
            .await
            .unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn insert_flat_value() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, &empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    {
        let found = find::<String, i64>(&storage, &empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let key: String = "key".into();
    let value = 42;
    let one_element = insert::<String, i64>(
        &storage,
        &storage,
        &empty,
        key.clone(),
        value,
        default_is_split_after_key,
    )
    .await
    .expect("inserting first key should succeed");
    assert_eq!(
        Some(crate::prolly_tree::IntegrityCheckResult::Valid(Some((
            key.clone(),
            key.clone()
        )))),
        crate::prolly_tree::verify_integrity::<String, i64>(&storage, &one_element,).await
    );
    assert_ne!(empty, one_element);
    {
        let found = find::<String, i64>(&storage, &one_element, &"key".to_string()).await;
        assert_eq!(Some(value), found);
    }
    {
        let found = find::<String, i64>(&storage, &one_element, &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    let loaded_back = load_node::<String, i64>(&storage, &one_element)
        .await
        .expect("loading has to work");
    match loaded_back {
        EitherNodeType::Leaf(node) => {
            assert_eq!(&Vec::from([("key".into(), value)]), node.entries())
        }
        EitherNodeType::Internal(_) => panic!("expected a leaf node"),
    }
    assert_eq!(
        0,
        crate::prolly_tree::size::<String, i64>(&storage, &empty)
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        crate::prolly_tree::size::<String, i64>(&storage, &one_element)
            .await
            .unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn insert_tree_reference() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, TreeReference>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, TreeReference>(&storage, &empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let key: String = "key".into();
    let value = TreeReference::new(empty);
    let one_element = insert::<String, TreeReference>(
        &storage,
        &storage,
        &empty,
        key.clone(),
        value,
        default_is_split_after_key,
    )
    .await
    .expect("inserting first key should succeed");
    assert_eq!(
        Some(crate::prolly_tree::IntegrityCheckResult::Valid(Some((
            key.clone(),
            key.clone()
        )))),
        crate::prolly_tree::verify_integrity::<String, TreeReference>(&storage, &one_element).await
    );
    assert_ne!(empty, one_element);
    {
        let found = find::<String, TreeReference>(&storage, &one_element, &"key".to_string()).await;
        assert_eq!(Some(value), found);
    }
    {
        let found = find::<String, TreeReference>(&storage, &one_element, &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    let loaded_back = load_node::<String, TreeReference>(&storage, &one_element)
        .await
        .expect("loading has to work");
    match loaded_back {
        EitherNodeType::Leaf(node) => {
            assert_eq!(&Vec::from([("key".into(), value)]), node.entries())
        }
        EitherNodeType::Internal(_) => panic!("expected a leaf node"),
    }
    assert_eq!(
        0,
        crate::prolly_tree::size::<String, TreeReference>(&storage, &empty)
            .await
            .unwrap()
    );
    assert_eq!(
        1,
        crate::prolly_tree::size::<String, TreeReference>(&storage, &one_element)
            .await
            .unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn insert_flat_values_one_at_a_time() {
    let number_of_keys = 200;
    let expected_trees_created = 858;
    let expected_final_digest = BlobDigest::parse_hex_string(
            "a40af0e6ad93ed59d3554b7c00da37e9519be8bc52c15602c220cc9e03db2c01c5e5ea385af102477cf983ca98c8ade650dd604312cc08cac2906798ba85082c"
        ).expect("valid digest");
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut current_state = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
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
    let all_entries = &all_entries;
    let storage = &storage;
    let current_state = &mut current_state;
    let expected_final_digest = &expected_final_digest;
    let mut expected_entries: BTreeMap<String, i64> = BTreeMap::new();
    for (key, value) in all_entries.iter() {
        {
            let existing_entry = find::<String, i64>(storage, current_state, key).await;
            let expected_entry = expected_entries.get(key);
            assert_eq!(expected_entry.copied(), existing_entry);
        }
        let trees_before = storage.number_of_trees().await;
        *current_state = insert::<String, i64>(
            storage,
            storage,
            current_state,
            key.clone(),
            *value,
            default_is_split_after_key,
        )
        .await
        .expect("inserting key should succeed");
        let trees_after = storage.number_of_trees().await;
        assert!(trees_after > trees_before);
        let difference = trees_after - trees_before;
        assert!(difference <= 200);
        expected_entries.insert(key.clone(), *value);
        assert_eq!(
            expected_entries.len() as u64,
            crate::prolly_tree::size::<String, i64>(storage, current_state)
                .await
                .unwrap()
        );
        assert_eq!(
            Some(crate::prolly_tree::IntegrityCheckResult::Valid(
                if expected_entries.is_empty() {
                    None
                } else {
                    Some((
                        expected_entries.first_entry().unwrap().key().clone(),
                        expected_entries.last_entry().unwrap().key().clone(),
                    ))
                }
            )),
            crate::prolly_tree::verify_integrity::<String, i64>(storage, current_state).await
        );
        for (key, value) in expected_entries.iter() {
            let found = find::<String, i64>(storage, current_state, key).await;
            assert_eq!(Some(*value), found);
        }
    }
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(expected_final_digest, current_state);
    assert_eq!(expected_trees_created, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = find::<String, i64>(storage, current_state, key).await;
        assert_eq!(Some(*value), found);
    }
    let in_memory_node = load_in_memory_node::<String, i64>(storage, current_state).await;
    println!("Entire tree: {:?}", in_memory_node);
    println!("Leaf counts: {:?}", in_memory_node.count());
}

#[test_log::test(tokio::test)]
async fn insert_flat_values_many_at_a_time() {
    let number_of_keys = 1000;
    let expected_trees_created = 385;
    let expected_final_digest = BlobDigest::parse_hex_string(
            "8d5011b62b516bbdc8205c8500e4f3c4b08c7f75e94bb16127b9709f223ae32bc7980628cf3225fa8f02edaabf095494f6e10dfafc5f555500b9b3c530a03604"
        ).expect("valid digest");
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut current_state = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
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
    let storage = &storage;
    let current_state = &mut current_state;
    let expected_final_digest = &expected_final_digest;
    let mut expected_entries: BTreeMap<String, i64> = BTreeMap::new();
    for chunk in all_entries.as_slice().chunks(100) {
        for (key, value) in chunk.iter() {
            let existing_entry = find::<String, i64>(storage, current_state, key).await;
            assert_eq!(None, existing_entry);
            expected_entries.insert(key.clone(), *value);
        }
        *current_state = insert_many(
            storage,
            storage,
            current_state,
            chunk.to_vec(),
            default_is_split_after_key,
        )
        .await
        .expect("inserting many entries should succeed");
        assert_eq!(
            Some(crate::prolly_tree::IntegrityCheckResult::Valid(Some((
                expected_entries.first_entry().unwrap().key().clone(),
                expected_entries.last_entry().unwrap().key().clone()
            )))),
            crate::prolly_tree::verify_integrity::<String, i64>(storage, current_state).await
        );
        assert_eq!(
            expected_entries.len() as u64,
            crate::prolly_tree::size::<String, i64>(storage, current_state)
                .await
                .unwrap()
        );
    }
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(expected_final_digest, current_state);
    assert_eq!(expected_trees_created, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = find::<String, i64>(storage, current_state, key).await;
        assert_eq!(
            Some(*value),
            found,
            "Failed to find expected entry for key: {}",
            key
        );
    }
    let in_memory_node = load_in_memory_node::<String, i64>(storage, current_state).await;
    println!("Entire tree: {:?}", in_memory_node);
    println!("Leaf counts: {:?}", in_memory_node.count());
}

#[test_log::test(tokio::test)]
async fn insert_flat_values_all_at_once() {
    let number_of_keys = 1000;
    let expected_trees_created = 34;
    let expected_final_digest = BlobDigest::parse_hex_string(
            "06aaaa833b68e77455740308be968c9c121fe8693f73b87cf658e1cd8c8f11f1a3cc114e6f5d0278f8d965aa8d69a6a58144d0c85420ed792cf6005fd5cf677d"
        ).expect("valid digest");
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut current_state = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
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
    let storage = &storage;
    let current_state = &mut current_state;
    let expected_final_digest = &expected_final_digest;
    let mut expected_entries: BTreeMap<String, i64> = BTreeMap::new();
    for (key, value) in all_entries.iter() {
        let existing_entry = find::<String, i64>(storage, current_state, key).await;
        assert_eq!(None, existing_entry);
        expected_entries.insert(key.clone(), *value);
    }
    *current_state = insert_many(
        storage,
        storage,
        current_state,
        all_entries,
        default_is_split_after_key,
    )
    .await
    .expect("inserting many entries should succeed");
    assert_eq!(
        Some(crate::prolly_tree::IntegrityCheckResult::Valid(Some((
            expected_entries.first_entry().unwrap().key().clone(),
            expected_entries.last_entry().unwrap().key().clone()
        )))),
        crate::prolly_tree::verify_integrity::<String, i64>(storage, current_state).await
    );
    assert_eq!(
        expected_entries.len() as u64,
        crate::prolly_tree::size::<String, i64>(storage, current_state)
            .await
            .unwrap()
    );
    let trees_in_the_end = storage.number_of_trees().await;
    assert_eq!(expected_final_digest, current_state);
    assert_eq!(expected_trees_created, trees_in_the_end);
    for (key, value) in expected_entries.iter() {
        let found = find::<String, i64>(storage, current_state, key).await;
        assert_eq!(
            Some(*value),
            found,
            "Failed to find expected entry for key: {}",
            key
        );
    }
    let in_memory_node = load_in_memory_node::<String, i64>(storage, current_state).await;
    println!("Entire tree: {:?}", in_memory_node);
    println!("Leaf counts: {:?}", in_memory_node.count());
}

#[test_log::test(tokio::test)]
async fn insert_tree_references() {
    let number_of_insertions = 100;
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut current_state = new_tree::<String, TreeReference>(&storage)
        .await
        .expect("creating a new tree should succeed");
    let mut all_entries = Vec::new();
    for index in 0..number_of_insertions {
        let key = format!("key-{index}");
        let value = TreeReference::new(BlobDigest::new(&[index as u8; 64]));
        all_entries.push((key, value));
    }
    {
        let mut random = SmallRng::seed_from_u64(123);
        all_entries.shuffle(&mut random);
    }
    let mut expected_entries: BTreeMap<String, TreeReference> = BTreeMap::new();
    for (key, value) in all_entries.into_iter() {
        {
            let existing_entry =
                find::<String, TreeReference>(&storage, &current_state, &key).await;
            let expected_entry = expected_entries.get(&key);
            assert_eq!(expected_entry.copied(), existing_entry);
        }
        current_state = insert::<String, TreeReference>(
            &storage,
            &storage,
            &current_state,
            key.clone(),
            value,
            default_is_split_after_key,
        )
        .await
        .expect("inserting key should succeed");
        {
            let found = find::<String, TreeReference>(&storage, &current_state, &key).await;
            assert_eq!(Some(value), found);
        }
        expected_entries.insert(key, value);
        assert_eq!(
            Some(crate::prolly_tree::IntegrityCheckResult::Valid(Some((
                expected_entries.first_entry().unwrap().key().clone(),
                expected_entries.last_entry().unwrap().key().clone()
            )))),
            crate::prolly_tree::verify_integrity::<String, TreeReference>(&storage, &current_state)
                .await
        );
    }
    for (key, value) in expected_entries.iter() {
        let found = find::<String, TreeReference>(&storage, &current_state, key).await;
        assert_eq!(Some(*value), found);
    }
    assert_eq!(
        expected_entries.len() as u64,
        crate::prolly_tree::size::<String, TreeReference>(&storage, &current_state)
            .await
            .unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn find_in_empty_node() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let node_digest = crate::prolly_tree::store_node(
        &storage,
        &crate::sorted_tree::Node::<u32, TreeReference>::new(),
        &crate::prolly_tree::Metadata { is_leaf: false },
    )
    .await
    .expect("storing empty node should succeed");
    let found = find::<u32, TreeReference>(&storage, &node_digest, &123).await;
    assert_eq!(None, found);
}

#[test_log::test(tokio::test)]
async fn insert_many_with_zero_entries() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    assert_eq!(
        0,
        crate::prolly_tree::size::<String, i64>(&storage, &empty)
            .await
            .unwrap()
    );
    let after_insert = insert_many::<String, i64>(
        &storage,
        &storage,
        &empty,
        Vec::new(),
        default_is_split_after_key::<String>,
    )
    .await
    .expect("inserting zero entries should succeed");
    assert_eq!(empty, after_insert);
}
