use arbitrary::Unstructured;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let entries: Vec<(String, i64)> = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
            let mut current_state = sorted_tree::sorted_tree::new_tree::<String, i64>(&storage)
                .await
                .expect("creating a new tree should succeed");
            let mut oracle = BTreeMap::new();
            for (key, _value) in entries.iter() {
                let found =
                    sorted_tree::sorted_tree::find::<String, i64>(&storage, current_state, key)
                        .await;
                assert_eq!(None, found);
            }
            for (key, value) in entries.iter() {
                current_state = sorted_tree::sorted_tree::insert::<String, i64>(
                    &storage,
                    &storage,
                    current_state,
                    key.clone(),
                    *value,
                )
                .await
                .expect("inserting key should succeed");
                let found =
                    sorted_tree::sorted_tree::find::<String, i64>(&storage, current_state, key)
                        .await;
                assert_eq!(Some(*value), found);
                oracle.insert(key.clone(), *value);
            }
            for (key, value) in oracle.iter() {
                let found =
                    sorted_tree::sorted_tree::find::<String, i64>(&storage, current_state, key)
                        .await;
                assert_eq!(Some(*value), found);
            }
        });
    true
}

#[test]
fn crash_0() {
    assert!(fuzz_function(&[
        33, 183, 70, 70, 70, 70, 183, 63, 37, 19, 10
    ]));
}

#[test]
fn crash_1() {
    assert!(fuzz_function(&[
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 51
    ]));
}
