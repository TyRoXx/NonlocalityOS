use crate::sorted_tree::{find, insert, load_node, new_tree};
use pretty_assertions::{assert_eq, assert_ne};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[test_log::test(tokio::test)]
async fn insert_first_key() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let value = 42;
    let one_element = insert::<String, i64>(&storage, &storage, empty, "key".into(), value)
        .await
        .expect("inserting first key should succeed");
    assert_ne!(empty, one_element);
    {
        let found = find::<String, i64>(&storage, one_element, &"key".to_string()).await;
        assert_eq!(Some(value), found);
    }
    {
        let found = find::<String, i64>(&storage, one_element, &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    let loaded_back = load_node::<String, i64>(&storage, one_element).await;
    assert_eq!(&Vec::from([("key".into(), value)]), loaded_back.entries());
}

#[test_log::test(tokio::test)]
async fn overwrite_value() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_value = 42;
    let after_first_insert =
        insert::<String, i64>(&storage, &storage, empty, "key".into(), first_value)
            .await
            .expect("inserting first key should succeed");
    assert_ne!(empty, after_first_insert);
    {
        let found = find::<String, i64>(&storage, after_first_insert, &"key".to_string()).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert, &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert).await;
        assert_eq!(
            &Vec::from([("key".to_string(), first_value)]),
            loaded_back.entries()
        );
    }
    let second_value = 77;
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert,
        "key".into(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty, after_second_insert);
    assert_ne!(after_first_insert, after_second_insert);
    {
        let found = find::<String, i64>(&storage, after_second_insert, &"key".to_string()).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_second_insert, &"xyz".to_string()).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert).await;
        assert_eq!(
            &Vec::from([("key".to_string(), second_value)]),
            loaded_back.entries()
        );
    }
}

#[test_log::test(tokio::test)]
async fn insert_before() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_key = "B".to_string();
    let first_value = 42;
    let second_key = "A".to_string();
    let second_value = 77;
    let after_first_insert =
        insert::<String, i64>(&storage, &storage, empty, first_key.clone(), first_value)
            .await
            .expect("inserting first key should succeed");
    assert_ne!(empty, after_first_insert);
    {
        let found = find::<String, i64>(&storage, after_first_insert, &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert, &second_key).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert).await;
        assert_eq!(
            &Vec::from([(first_key.clone(), first_value)]),
            loaded_back.entries()
        );
    }
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert,
        second_key.clone(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty, after_second_insert);
    {
        let found = find::<String, i64>(&storage, after_second_insert, &second_key).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert, &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert).await;
        assert_eq!(
            &Vec::from([(second_key, second_value), (first_key, first_value)]),
            loaded_back.entries()
        );
    }
}

#[test_log::test(tokio::test)]
async fn insert_after() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let empty = new_tree::<String, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    {
        let found = find::<String, i64>(&storage, empty, &"key".to_string()).await;
        assert_eq!(None, found);
    }
    let first_key = "A".to_string();
    let first_value = 42;
    let second_key = "B".to_string();
    let second_value = 77;
    let after_first_insert =
        insert::<String, i64>(&storage, &storage, empty, first_key.clone(), first_value)
            .await
            .expect("inserting first key should succeed");
    assert_ne!(empty, after_first_insert);
    {
        let found = find::<String, i64>(&storage, after_first_insert, &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert, &second_key).await;
        assert_eq!(None, found);
    }
    assert_eq!(storage.number_of_trees().await, 2);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_first_insert).await;
        assert_eq!(
            &Vec::from([(first_key.clone(), first_value)]),
            loaded_back.entries()
        );
    }
    let after_second_insert = insert::<String, i64>(
        &storage,
        &storage,
        after_first_insert,
        second_key.clone(),
        second_value,
    )
    .await
    .expect("inserting second key should succeed");
    assert_ne!(empty, after_second_insert);
    assert_ne!(after_first_insert, after_second_insert);
    {
        let found = find::<String, i64>(&storage, after_second_insert, &second_key).await;
        assert_eq!(Some(second_value), found);
    }
    {
        let found = find::<String, i64>(&storage, after_first_insert, &first_key).await;
        assert_eq!(Some(first_value), found);
    }
    assert_eq!(storage.number_of_trees().await, 3);
    {
        let loaded_back = load_node::<String, i64>(&storage, after_second_insert).await;
        assert_eq!(
            &Vec::from([(first_key, first_value), (second_key, second_value)]),
            loaded_back.entries()
        );
    }
}
