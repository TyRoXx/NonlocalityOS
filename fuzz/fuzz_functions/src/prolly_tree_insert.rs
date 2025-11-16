use arbitrary::Unstructured;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

async fn insert_many_entries(entries: &[(u32, i64)]) {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut current_state = sorted_tree::prolly_tree::new_tree::<u32, i64>(&storage)
        .await
        .expect("creating a new tree should succeed");
    let mut oracle = BTreeMap::new();
    for (key, _value) in entries.iter() {
        let found = sorted_tree::prolly_tree::find::<u32, i64>(&storage, &current_state, key).await;
        assert_eq!(None, found);
    }
    for (key, value) in entries.iter() {
        {
            let number_of_trees_before = storage.number_of_trees().await;
            current_state = sorted_tree::prolly_tree::insert::<u32, i64>(
                &storage,
                &storage,
                &current_state,
                *key,
                *value,
                sorted_tree::prolly_tree::default_is_split_after_key::<u32>,
            )
            .await
            .expect("inserting key should succeed");
            let number_of_trees_after = storage.number_of_trees().await;
            assert!(number_of_trees_after >= number_of_trees_before);
            let difference = number_of_trees_after - number_of_trees_before;
            // TODO: find out why so many trees are created in some cases
            assert!(difference <= 100);
        }
        let found = sorted_tree::prolly_tree::find::<u32, i64>(&storage, &current_state, key).await;
        assert_eq!(Some(*value), found);
        oracle.insert(*key, *value);
    }
    for (key, value) in oracle.iter() {
        let found = sorted_tree::prolly_tree::find::<u32, i64>(&storage, &current_state, key).await;
        assert_eq!(Some(*value), found);
    }
    let final_size = sorted_tree::prolly_tree::size::<u32, i64>(&storage, &current_state).await;
    assert_eq!(Some(oracle.len() as u64), final_size);
    let number_of_trees = storage.number_of_trees().await;
    assert!(number_of_trees >= (1 + oracle.len()));
    // TODO: find a better upper bound
    assert!(number_of_trees <= 10000);
}

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let entries: Vec<(u32, i64)> = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            insert_many_entries(&entries).await;
        });
    true
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_zero() {
    insert_many_entries(&[]).await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_same_entry() {
    insert_many_entries(&[(10, 100), (10, 100), (10, 100), (10, 100), (10, 100)]).await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_few() {
    insert_many_entries(&[
        (10, 100),
        (20, 200),
        (15, 150),
        (25, 250),
        (5, 50),
        (30, 300),
        (12, 120),
        (10, 200),
        (15, 250),
        (12, 220),
        (18, 180),
        (22, 220),
    ])
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_lots() {
    insert_many_entries(
        &(0..200)
            .map(|i| (i, (i as i64) * 10))
            .collect::<Vec<(u32, i64)>>(),
    )
    .await;
}

#[test]
fn crash_0() {
    assert!(fuzz_function(&[
        15, 42, 255, 113, 113, 113, 169, 169, 169, 169, 169, 169, 169, 169, 255, 0, 255, 1, 3, 255,
        255, 255, 255, 35, 35, 35, 35, 35, 35, 35, 255, 255, 255, 255, 255, 255, 255, 255, 35, 35,
        222, 219, 219, 219, 219, 255, 255, 101, 255, 255, 255, 35, 35, 35, 35, 35, 35, 35, 35, 35,
        35, 166, 35, 35, 35, 35, 35, 255, 255, 35, 255, 255, 255, 3, 35, 35, 35, 35, 35, 36, 218,
        181, 186, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 255, 255, 255, 255, 113, 255, 255,
        255, 35, 35, 35, 219, 219, 219, 219, 219, 255, 255, 255, 255, 255, 35, 35, 35, 35, 35, 35,
        34, 255, 255, 255, 255, 255, 255, 255, 255, 255, 151, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 0, 6, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 255, 255, 255, 113, 255, 255, 255, 255,
        35, 35, 27, 11, 50, 50, 51, 228, 255, 124, 124, 124, 255, 255, 255, 255, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 219, 219, 255, 113, 219, 219, 219, 219, 219, 219, 219, 219,
        219, 219, 255, 255, 255, 3, 2, 2, 219, 219, 219, 219, 219, 35, 35, 255, 35, 255, 255, 255,
        255, 255, 35, 35, 35, 35, 35, 35, 255, 255, 35, 35, 35, 35, 2, 2, 255, 219, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 219, 255, 113, 219, 219, 219, 219, 219, 219, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 219, 219, 213, 219, 219, 219, 35, 35, 35, 124, 124, 255, 255,
        255, 255, 51, 255, 255, 35, 35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 255, 255, 219,
        219, 219, 219, 219, 219, 255, 35, 255, 255, 255, 255, 255, 35, 219, 219, 219, 219, 219,
        255, 11, 113, 219, 219, 219, 219, 219, 219, 219, 37, 164, 36, 253, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 219, 219, 219, 255, 113, 219, 219, 219, 219, 219, 219, 219, 255,
        51, 255, 255, 255, 51, 255, 255, 35, 35, 35, 35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255,
        255, 255, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 255, 113, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 219, 255, 255, 255, 3, 2, 2, 219, 219, 219, 219, 219, 35, 35,
        35, 255, 35, 255, 255, 255, 255, 255, 35, 35, 35, 35, 35, 35, 255, 255, 35, 35, 35, 35, 2,
        2, 255, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 255, 113, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 219, 213, 219, 219, 219,
        35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 35, 35, 35, 35, 35, 124, 124, 255, 255, 255,
        255, 51, 255, 255, 35, 35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 255, 255, 219, 219,
        219, 219, 219, 219, 255, 35, 255, 255, 255, 255, 255, 35, 219, 219, 219, 219, 219, 255, 11,
        113, 219, 219, 219, 219, 219, 219, 219, 37, 164, 36, 253, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 219, 219, 219, 255, 113, 219, 219, 219, 219, 219, 219, 219, 219, 219,
        219, 2, 2, 219, 219, 219, 219, 219, 35, 35, 255, 35, 255, 255, 255, 255, 255, 255, 51, 124,
        255, 35, 35, 35, 35, 35, 35, 255, 255, 35, 35, 51, 35, 2, 2, 255, 219, 219, 219, 219, 219,
        219, 219, 219, 219, 219, 219, 255, 113, 219, 219, 219, 219, 219, 219, 219, 219, 255, 255,
        2, 219, 219, 219, 219, 219, 35, 35, 255, 35, 255, 252, 255, 255, 255, 35, 35, 35, 35, 35,
        35, 255, 255, 35, 35, 35, 35, 166, 35, 35, 0, 0, 0, 0, 0, 0, 255, 255, 51, 255, 255, 35,
        35, 35, 35, 35, 35, 166, 35, 35, 35, 35, 35, 255, 255, 255, 255, 219, 219, 0, 0, 35, 35,
        35, 35, 219, 219, 219, 218, 219, 219, 35, 219, 35, 2
    ]));
}
