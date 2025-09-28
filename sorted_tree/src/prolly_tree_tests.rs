use crate::prolly_tree::{find, insert, new_tree};
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
    let loaded_back = crate::sorted_tree::load_node::<String, i64>(&storage, &one_element).await;
    assert_eq!(&Vec::from([("key".into(), value)]), loaded_back.entries());
}
