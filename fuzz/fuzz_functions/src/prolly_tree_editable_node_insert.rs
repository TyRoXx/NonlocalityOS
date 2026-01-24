use arbitrary::{Arbitrary, Unstructured};
use astraea::{in_memory_storage::InMemoryTreeStorage, tree::BlobDigest};
use pretty_assertions::assert_eq;
use rand::{rngs::SmallRng, seq::SliceRandom, SeedableRng};
use sorted_tree::{
    prolly_tree_editable_node::{EditableNode, IntegrityCheckResult},
    sorted_tree::TreeReference,
};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

type UniqueInsertions = BTreeMap<u32, i64>;
type InsertionBatches = Vec<UniqueInsertions>;

fn randomize_insertion_order(seed: u8, insertion_batches: &InsertionBatches) -> Vec<(u32, i64)> {
    let mut random = SmallRng::seed_from_u64(seed as u64);
    let mut all_insertions = Vec::new();
    for batch in insertion_batches.iter() {
        let mut batch_randomized: Vec<(u32, i64)> = batch.iter().map(|(k, v)| (*k, *v)).collect();
        batch_randomized.shuffle(&mut random);
        all_insertions.extend(batch_randomized);
    }
    all_insertions
}

async fn insert_one_at_a_time(insertions: &[(u32, i64)]) -> BlobDigest {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let mut editable_node: EditableNode<u32, i64> = EditableNode::new();
    let mut oracle = BTreeMap::new();
    for (key, _value) in insertions.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(None, found);
    }
    for (key, value) in insertions.iter() {
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
        let count = editable_node.count(&storage).await.unwrap();
        assert_eq!(oracle.len() as u64, count);
        match editable_node
            .verify_integrity(oracle.keys().last(), &storage)
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
    let final_count = editable_node.count(&storage).await.unwrap();
    assert_eq!(oracle.len() as u64, final_count);
    assert_eq!(0, storage.number_of_trees().await);
    let digest = editable_node.save(&storage).await.unwrap();
    let number_of_trees = storage.number_of_trees().await;
    assert!(number_of_trees >= 1);
    // TODO: find a better upper bound
    assert!(number_of_trees <= 1000);

    // test loading from storage
    editable_node = EditableNode::Reference(TreeReference::new(digest));
    for (key, value) in oracle.iter() {
        let found = editable_node.find(key, &storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    assert_eq!(
        oracle.len() as u64,
        editable_node.count(&storage).await.unwrap()
    );
    let saved_again = editable_node.save(&storage).await.unwrap();
    assert_eq!(saved_again, digest);

    digest
}

#[derive(Arbitrary, Debug)]
struct TestCase {
    seed_a: u8,
    seed_b: u8,
    insertion_batches: InsertionBatches,
}

async fn insert_entries(parameters: &TestCase) {
    let digest_a = insert_one_at_a_time(&randomize_insertion_order(
        parameters.seed_a,
        &parameters.insertion_batches,
    ))
    .await;
    let digest_b = insert_one_at_a_time(&randomize_insertion_order(
        parameters.seed_b,
        &parameters.insertion_batches,
    ))
    .await;
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
        insertion_batches: vec![],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_same_entry() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        insertion_batches: vec![[(10, 100), (10, 100), (10, 100), (10, 100), (10, 100)].into()],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_few() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        insertion_batches: vec![
            [
                (10, 100),
                (20, 200),
                (15, 150),
                (25, 250),
                (5, 50),
                (30, 300),
                (12, 120),
            ]
            .into(),
            [(10, 200), (15, 250), (12, 220), (18, 180), (22, 220)].into(),
        ],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_insert_many_entries_lots() {
    insert_entries(&TestCase {
        seed_a: 0,
        seed_b: 1,
        insertion_batches: vec![(0..200).map(|i| (i, (i as i64) * 10)).collect()],
    })
    .await;
}
