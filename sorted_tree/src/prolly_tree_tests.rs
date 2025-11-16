use crate::{
    prolly_tree::{
        default_is_split_after_key, find, insert, load_in_memory_node, load_node, new_tree,
        EitherNodeType,
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
use test_case::test_case;
use tokio::{runtime::Runtime, sync::Mutex};

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
    let value = 42;
    let one_element = insert::<String, i64>(
        &storage,
        &storage,
        &empty,
        "key".into(),
        value,
        default_is_split_after_key,
    )
    .await
    .expect("inserting first key should succeed");
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
    let value = TreeReference::new(empty);
    let one_element = insert::<String, TreeReference>(
        &storage,
        &storage,
        &empty,
        "key".into(),
        value,
        default_is_split_after_key,
    )
    .await
    .expect("inserting first key should succeed");
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

#[test_case(123)]
// TODO: make final digest deterministic
//#[test_case(456)]
fn insert_many_flat_values(seed: u64) {
    Runtime::new().unwrap().block_on(async {
        let number_of_insertions = 1000;
        let expected_trees_created = 2630;
        let expected_final_digest = BlobDigest::parse_hex_string(
            "a76984443db80b8c18aa5bcd36de7e85ec48603a2ccc9a4abdf9651879bc68403130453ca771ede193055d684e413822f9ad784c31c0ef9ee1ee3aae814608c3"
        ).expect("valid digest");
        let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
        let mut current_state = new_tree::<String, i64>(&storage)
            .await
            .expect("creating a new tree should succeed");
        let mut all_entries = Vec::new();
        for index in 0..number_of_insertions {
            let key = format!("key-{index}");
            let value = index as i64;
            all_entries.push((key, value));
        }
        {
            let mut random = SmallRng::seed_from_u64(seed);
            all_entries.shuffle(&mut random);
        }
        let mut expected_entries = Vec::new();
        for (key, value) in all_entries.into_iter() {
            let trees_before = storage.number_of_trees().await;
            current_state = insert::<String, i64>(
                &storage,
                &storage,
                &current_state,
                key.clone(),
                value,
                default_is_split_after_key,
            )
            .await
            .expect("inserting key should succeed");
            let trees_after = storage.number_of_trees().await;
            assert!(trees_after > trees_before);
            let difference = trees_after - trees_before;
            assert!(difference <= 5);
            {
                let found = find::<String, i64>(&storage, &current_state, &key).await;
                assert_eq!(Some(value), found);
            }
            expected_entries.push((key, value));
        }
        let trees_in_the_end = storage.number_of_trees().await;
        assert_eq!(expected_final_digest, current_state);
        assert_eq!(expected_trees_created, trees_in_the_end);
        expected_entries.sort_by_key(|element| element.0.clone());
        for (key, value) in expected_entries.iter() {
            let found = find::<String, i64>(&storage, &current_state, key).await;
            assert_eq!(Some(*value), found);
        }
        assert_eq!(
            expected_entries.len() as u64,
            crate::prolly_tree::size::<String, i64>(&storage, &current_state)
                .await
                .unwrap()
        );
        let in_memory_node = load_in_memory_node::<String, i64>(&storage, &current_state).await;
        println!("Entire tree: {:?}", in_memory_node);
        println!("Leaf counts: {:?}", in_memory_node.count());
    });
}

#[test_log::test(tokio::test)]
async fn insert_many_tree_references() {
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
    let mut expected_entries = Vec::new();
    for (key, value) in all_entries.into_iter() {
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
        expected_entries.push((key, value));
        expected_entries.sort_by_key(|element| element.0.clone());
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
