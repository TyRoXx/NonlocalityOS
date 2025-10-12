use crate::{
    prolly_tree::{find, insert, load_in_memory_node, load_node, new_tree, EitherNodeType},
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
    let one_element = insert::<String, i64>(&storage, &storage, &empty, "key".into(), value)
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
    let one_element =
        insert::<String, TreeReference>(&storage, &storage, &empty, "key".into(), value)
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
}

#[test_log::test(tokio::test)]
async fn insert_many_flat_values() {
    let number_of_insertions = 1000;
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
        let mut random = SmallRng::seed_from_u64(123);
        all_entries.shuffle(&mut random);
    }
    let mut expected_entries = Vec::new();
    for (key, value) in all_entries.into_iter() {
        current_state =
            insert::<String, i64>(&storage, &storage, &current_state, key.clone(), value)
                .await
                .expect("inserting key should succeed");
        {
            let found = find::<String, i64>(&storage, &current_state, &key).await;
            assert_eq!(Some(value), found);
        }
        expected_entries.push((key, value));
        expected_entries.sort_by_key(|element| element.0.clone());
    }
    for (key, value) in expected_entries.iter() {
        let found = find::<String, i64>(&storage, &current_state, key).await;
        assert_eq!(Some(*value), found);
    }
    let in_memory_node = load_in_memory_node::<String, i64>(&storage, &current_state).await;
    println!("Entire tree: {:?}", in_memory_node);
    println!("Leaf counts: {:?}", in_memory_node.count());
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
        current_state =
            insert::<String, TreeReference>(&storage, &storage, &current_state, key.clone(), value)
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
}
