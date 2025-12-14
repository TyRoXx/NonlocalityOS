use arbitrary::{Arbitrary, Unstructured};
use astraea::storage::LoadTree;
use pretty_assertions::assert_eq;
use sorted_tree::prolly_tree_editable_node::{EditableLeafNode, EditableLoadedNode, SizeTracker};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Arbitrary, Debug)]
struct TestCase {
    entries: BTreeMap<u32, i64>,
}

async fn run_test_case(test_case: &TestCase) -> bool {
    let mut node = EditableLoadedNode::Leaf(
        match EditableLeafNode::<u32, i64>::create(test_case.entries.clone()) {
            Some(node) => node,
            None => return false,
        },
    );
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let digest = node.save(&storage).await.unwrap();

    let mut size_tracker = SizeTracker::new();
    for (key, value) in test_case.entries.iter() {
        size_tracker.add_entry(key, value);
    }

    let tree = storage.load_tree(&digest).await.unwrap();
    let hashed_tree = tree.hash().unwrap();
    assert_eq!(
        hashed_tree.tree().blob().len() as usize,
        size_tracker.size()
    );
    true
}

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let test_case: TestCase = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async { run_test_case(&test_case).await })
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_empty() {
    assert!(
        !run_test_case(&TestCase {
            entries: BTreeMap::new(),
        })
        .await
    );
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_one() {
    assert!(
        run_test_case(&TestCase {
            entries: BTreeMap::from([(123, 456)]),
        })
        .await
    );
}
