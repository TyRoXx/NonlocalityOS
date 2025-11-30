use arbitrary::{Arbitrary, Unstructured};
use astraea::tree::BlobDigest;
use pretty_assertions::assert_eq;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use sorted_tree::prolly_tree_editable_node::{EditableNode, IntegrityCheckResult};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

async fn insert_one_at_a_time(seed: u8, entries: &[(u32, i64)]) -> BlobDigest {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, i64> = EditableNode::new();
    let mut oracle = BTreeMap::new();
    for (key, _value) in entries.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(None, found);
    }
    let mut entries_in_insertion_order = entries.to_vec();
    {
        let mut random = SmallRng::seed_from_u64(seed as u64);
        entries_in_insertion_order.shuffle(&mut random);
    }
    for (key, value) in entries_in_insertion_order.iter() {
        {
            let existing_entry = editable_node.find(key, &storage).await.unwrap();
            let expected_entry = oracle.get(key);
            assert_eq!(expected_entry.copied(), existing_entry);
        }
        {
            let number_of_trees_before = storage.number_of_trees().await;
            editable_node
                .insert(*key, *value, &storage)
                .await
                .expect("inserting key should succeed");
            let number_of_trees_after = storage.number_of_trees().await;
            assert!(number_of_trees_after >= number_of_trees_before);
            let difference = number_of_trees_after - number_of_trees_before;
            // TODO: find out why so many trees are created in some cases
            assert!(difference <= 100);
        }
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
        oracle.insert(*key, *value);
        let size = editable_node.size(&storage).await.unwrap();
        assert_eq!(oracle.len() as u64, size);
        match editable_node
            .verify_integrity(oracle.keys().last().unwrap(), true, &storage)
            .await
            .unwrap()
        {
            IntegrityCheckResult::Valid { depth } => {
                assert!(depth < 10);
            }
            IntegrityCheckResult::Corrupted(reason) => {
                panic!("Tree integrity check failed: {}", reason);
            }
        }
    }
    for (key, value) in oracle.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    let final_size = editable_node.size(&storage).await.unwrap();
    assert_eq!(oracle.len() as u64, final_size);
    assert_eq!(0, storage.number_of_trees().await);
    let digest = editable_node.save(&storage).await.unwrap();
    let number_of_trees = storage.number_of_trees().await;
    assert!(number_of_trees >= 1);
    // TODO: find a better upper bound
    assert!(number_of_trees <= 1000);
    digest
}

#[derive(Arbitrary, Debug)]
struct TestCase {
    seed_a: u8,
    seed_b: u8,
    entries: Vec<(u32, i64)>,
}

async fn insert_entries(parameters: &TestCase) {
    println!("Test case: {:?}", parameters);
    let digest_a = insert_one_at_a_time(parameters.seed_a, &parameters.entries).await;
    let digest_b = insert_one_at_a_time(parameters.seed_b, &parameters.entries).await;
    assert_eq!(digest_a, digest_b);
}

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let parameters: TestCase = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            insert_entries(&parameters).await;
        });
    true
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_zero() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        entries: vec![],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_same_entry() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        entries: vec![(10, 100), (10, 100), (10, 100), (10, 100), (10, 100)],
    })
    .await;
}

/*
#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_few() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        entries: vec![
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
        ],
    })
    .await;
}
    */

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_lots() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        entries: (0..200)
            .map(|i| (i, (i as i64) * 10))
            .collect::<Vec<(u32, i64)>>(),
    })
    .await;
}

#[test]
fn crash_0() {
    assert!(fuzz_function(&[
        76, 40, 181, 181, 0, 0, 0, 10, 181, 213, 181, 181, 0, 0, 251, 255, 181
    ]));
}
