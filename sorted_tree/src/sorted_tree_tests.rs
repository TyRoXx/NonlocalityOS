use crate::sorted_tree::{find, insert, load_node, new_tree, node_to_tree, Node, TreeReference};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::StoreTree,
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use pretty_assertions::{assert_eq, assert_ne};
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use std::{collections::BTreeMap, sync::Arc};

#[test_log::test(tokio::test)]
async fn insert_first_key() {
    let storage = InMemoryTreeStorage::empty();
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty.digest(), &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let value = 42;
    let one_element =
        insert::<String, i64>(&storage, &storage, empty.digest(), "key".into(), value)
            .await
            .expect("inserting first key should succeed");
    assert_ne!(empty.digest(), one_element.digest());
    {
        let found = find::<String, i64>(&storage, one_element.digest(), &"key".to_string()).await;
        assert_eq!(Some(value), found);
    }
    {
        let found = find::<String, i64>(&storage, one_element.digest(), &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    let loaded_back = load_node::<String, i64>(&storage, one_element.digest())
        .await
        .unwrap();
    assert_eq!(&Vec::from([("key".into(), value)]), loaded_back.entries());
}

#[test_log::test(tokio::test)]
async fn insert_existing_key() {
    let storage = InMemoryTreeStorage::empty();
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty.digest(), &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_value = 42;
    let after_first_insert = insert::<String, i64>(
        &storage,
        &storage,
        empty.digest(),
        "key".into(),
        first_value,
    )
    .await
    .expect("inserting first key should succeed");
    assert_ne!(empty.digest(), after_first_insert.digest());
    {
        let found =
            find::<String, i64>(&storage, after_first_insert.digest(), &"key".to_string()).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found =
            find::<String, i64>(&storage, after_first_insert.digest(), &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([("key".to_string(), first_value)]),
            loaded_back.entries()
        );
    }
    let second_value = 77;
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert.digest(),
        "key".into(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty.digest(), after_second_insert.digest());
    assert_ne!(after_first_insert.digest(), after_second_insert.digest());
    {
        let found =
            find::<String, i64>(&storage, after_second_insert.digest(), &"key".to_string()).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found =
            find::<String, i64>(&storage, after_second_insert.digest(), &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([("key".to_string(), second_value)]),
            loaded_back.entries()
        );
    }
}

#[test_log::test(tokio::test)]
async fn insert_before() {
    let storage = InMemoryTreeStorage::empty();
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty.digest(), &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_key = "B".to_string();
    let first_value = 42;
    let second_key = "A".to_string();
    let second_value = 77;
    let after_first_insert = insert::<String, i64>(
        &storage,
        &storage,
        empty.digest(),
        first_key.clone(),
        first_value,
    )
    .await
    .expect("inserting first key should succeed");
    assert_ne!(empty.digest(), after_first_insert.digest());
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &second_key).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([(first_key.clone(), first_value)]),
            loaded_back.entries()
        );
    }
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert.digest(),
        second_key.clone(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty.digest(), after_second_insert.digest());
    {
        let found = find::<String, i64>(&storage, after_second_insert.digest(), &second_key).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([(second_key, second_value), (first_key, first_value)]),
            loaded_back.entries()
        );
    }
}

#[test_log::test(tokio::test)]
async fn insert_after() {
    let storage = InMemoryTreeStorage::empty();
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty.digest(), &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_key = "A".to_string();
    let first_value = 42;
    let second_key = "B".to_string();
    let second_value = 77;
    let after_first_insert = insert::<String, i64>(
        &storage,
        &storage,
        empty.digest(),
        first_key.clone(),
        first_value,
    )
    .await
    .expect("inserting first key should succeed");
    assert_ne!(empty.digest(), after_first_insert.digest());
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &second_key).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([(first_key.clone(), first_value)]),
            loaded_back.entries()
        );
    }
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert.digest(),
        second_key.clone(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty.digest(), after_second_insert.digest());
    assert_ne!(after_first_insert.digest(), after_second_insert.digest());
    {
        let found = find::<String, i64>(&storage, after_second_insert.digest(), &second_key).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert.digest(), &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert.digest())
            .await
            .unwrap();
        assert_eq!(
            &Vec::from([(first_key, first_value), (second_key, second_value)]),
            loaded_back.entries()
        );
    }
}

#[test_log::test(tokio::test)]
async fn insert_many_new_keys() {
    let number_of_insertions = 100;
    let storage = InMemoryTreeStorage::empty();
    let mut current_state = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    let mut all_entries = Vec::new();
    for index in 0..number_of_insertions {
        let key = format!("key-{index}");
        let value = index;
        all_entries.push((key, value));
    }
    {
        let mut random = SmallRng::seed_from_u64(123);
        all_entries.shuffle(&mut random);
    }
    let mut expected_entries = Vec::new();
    for (index, (key, value)) in all_entries.into_iter().enumerate() {
        current_state = insert::<String, i64>(
            &storage,
            &storage,
            current_state.digest(),
            key.clone(),
            value,
        )
        .await
        .expect("inserting key should succeed");
        {
            let found = find::<String, i64>(&storage, current_state.digest(), &key).await;
            assert_eq!(Some(value), found);
        }
        assert_eq!(2 + index as u64, storage.number_of_trees().await as u64);
        expected_entries.push((key, value));
        expected_entries.sort_by_key(|element| element.0.clone());
        {
            let loaded_back = load_node::<String, i64>(&storage, current_state.digest())
                .await
                .unwrap();
            assert_eq!(&expected_entries, loaded_back.entries());
        }
    }
    for (key, value) in expected_entries.iter() {
        let found = find::<String, i64>(&storage, current_state.digest(), key).await;
        assert_eq!(Some(*value), found);
    }
}

#[test_log::test(tokio::test)]
async fn insert_many_with_overwrites() {
    let number_of_insertions = 100;
    let storage = InMemoryTreeStorage::empty();
    let mut current_state = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    let mut oracle = BTreeMap::new();
    let mut all_insertions = Vec::new();
    for index in 0..number_of_insertions {
        let overwrite_index = index % 10;
        let key = format!("key-{overwrite_index}");
        let value = index;
        all_insertions.push((key.clone(), value));
    }
    {
        let mut random = SmallRng::seed_from_u64(123);
        all_insertions.shuffle(&mut random);
    }
    for (key, value) in all_insertions.into_iter() {
        current_state = insert::<String, i64>(
            &storage,
            &storage,
            current_state.digest(),
            key.clone(),
            value,
        )
        .await
        .expect("inserting key should succeed");
        {
            let found = find::<String, i64>(&storage, current_state.digest(), &key).await;
            assert_eq!(Some(value), found);
        }
        oracle.insert(key, value);
        {
            let loaded_back = load_node::<String, i64>(&storage, current_state.digest())
                .await
                .unwrap();
            let expected_entries = oracle
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect::<Vec<_>>();
            assert_eq!(&expected_entries, loaded_back.entries());
        }
    }
    for (key, value) in oracle.iter() {
        let found = find::<String, i64>(&storage, current_state.digest(), key).await;
        assert_eq!(Some(*value), found);
    }
}

#[test_log::test]
fn node_to_tree_without_child_references() {
    let mut node = Node::<u64, String>::new();
    node.insert(1, "A".to_string());
    node.insert(2, "B".to_string());
    let tree = node_to_tree(&node, &bytes::Bytes::new()).unwrap();
    let expected = Tree::new(
        TreeBlob::try_from(bytes::Bytes::from_static(b"\x02\x01\x01A\x02\x01B")).unwrap(),
        TreeChildren::empty(),
    );
    assert_eq!(expected, tree);
}

#[test_log::test(tokio::test)]
async fn node_to_tree_with_child_references() {
    let storage = InMemoryTreeStorage::empty();
    let mut node = Node::<u64, TreeReference>::new();
    let reference_1 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from_static(b"\x00")).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    node.insert(1, TreeReference::new(reference_1.clone()));
    let reference_2 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from_static(b"\x01")).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    node.insert(2, TreeReference::new(reference_2.clone()));
    let tree = node_to_tree(&node, &bytes::Bytes::new()).unwrap();
    let expected = Tree::new(
        TreeBlob::try_from(bytes::Bytes::from_iter([2, 1, 2])).unwrap(),
        TreeChildren::try_from(vec![reference_1, reference_2]).unwrap(),
    );
    assert_eq!(expected, tree);
}

#[test_log::test(tokio::test)]
async fn insert_reference_value() {
    let storage = InMemoryTreeStorage::empty();
    let empty = new_tree::<String, TreeReference>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found =
            find::<String, TreeReference>(&storage, empty.digest(), &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let one_element = insert::<String, TreeReference>(
        &storage,
        &storage,
        empty.digest(),
        "key".into(),
        TreeReference::new(empty.clone()),
    )
    .await
    .expect("inserting first key should succeed");
    assert_ne!(empty.digest(), one_element.digest());
    {
        let found =
            find::<String, TreeReference>(&storage, one_element.digest(), &"key".to_string()).await;
        assert_eq!(Some(TreeReference::new(empty.clone())), found);
    }
    {
        let found =
            find::<String, TreeReference>(&storage, one_element.digest(), &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    let loaded_back = load_node::<String, TreeReference>(&storage, one_element.digest())
        .await
        .unwrap();
    assert_eq!(
        &Vec::from([("key".into(), TreeReference::new(empty))]),
        loaded_back.entries()
    );
}
