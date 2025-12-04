use arbitrary::{Arbitrary, Unstructured};
use astraea::{storage::InMemoryTreeStorage, tree::BlobDigest};
use pretty_assertions::assert_eq;
use sorted_tree::prolly_tree_editable_node::EditableNode;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Arbitrary, Debug)]
enum MapOperation {
    Insert(u32, i64),
    Remove(u32),
}

#[derive(Arbitrary, Debug)]
struct TestCase {
    before: BTreeMap<u32, i64>,
    after: BTreeMap<u32, i64>,
    operations: Vec<MapOperation>,
}

fn find_operations_to_transform(
    before: &BTreeMap<u32, i64>,
    after: &BTreeMap<u32, i64>,
) -> Vec<MapOperation> {
    let mut operations = Vec::new();
    for (key, value) in after.iter() {
        match before.get(key) {
            Some(existing_value) => {
                if existing_value != value {
                    operations.push(MapOperation::Insert(*key, *value));
                }
            }
            None => {
                operations.push(MapOperation::Insert(*key, *value));
            }
        }
    }
    for key in before.keys() {
        if !after.contains_key(key) {
            operations.push(MapOperation::Remove(*key));
        }
    }
    operations
}

async fn execute_operations_on_prolly_tree(
    digest: &BlobDigest,
    operations: &[MapOperation],
    storage: &InMemoryTreeStorage,
) -> BlobDigest {
    let mut editable_node: EditableNode<u32, i64> =
        EditableNode::load(digest, storage).await.unwrap();
    let mut oracle = BTreeMap::new();
    for operation in operations {
        match operation {
            MapOperation::Insert(key, value) => {
                editable_node.insert(*key, *value, storage).await.unwrap();
                oracle.insert(*key, *value);
            }
            MapOperation::Remove(key) => {
                editable_node.remove(key, storage).await.unwrap();
                oracle.remove(key);
            }
        }
    }
    editable_node.save(storage).await.unwrap()
}

fn execute_operations_on_btree_map(map: &mut BTreeMap<u32, i64>, operations: &[MapOperation]) {
    for operation in operations {
        match operation {
            MapOperation::Insert(key, value) => {
                map.insert(*key, *value);
            }
            MapOperation::Remove(key) => {
                map.remove(key);
            }
        }
    }
}

async fn verify_prolly_tree_equality_to_map(
    digest: &BlobDigest,
    map: &BTreeMap<u32, i64>,
    storage: &InMemoryTreeStorage,
) {
    let mut editable_node: EditableNode<u32, i64> =
        EditableNode::load(digest, storage).await.unwrap();
    for (key, value) in map.iter() {
        let found = editable_node.find(key, storage).await.unwrap();
        assert_eq!(Some(*value), found);
    }
    let size = editable_node.size(storage).await.unwrap();
    assert_eq!(map.len() as u64, size);
}

async fn btree_map_to_digest(
    map: &BTreeMap<u32, i64>,
    storage: &InMemoryTreeStorage,
) -> BlobDigest {
    let mut editable_node: EditableNode<u32, i64> = EditableNode::new();
    for (key, value) in map.iter() {
        editable_node.insert(*key, *value, storage).await.unwrap();
    }
    let digest = editable_node.save(storage).await.unwrap();
    verify_prolly_tree_equality_to_map(&digest, map, storage).await;
    digest
}

async fn run_test_case(test_case: &TestCase) {
    let intermediary_map = {
        let mut map = test_case.before.clone();
        execute_operations_on_btree_map(&mut map, &test_case.operations);
        map
    };
    let additional_operations = find_operations_to_transform(&intermediary_map, &test_case.after);
    let final_map = {
        let mut map = intermediary_map.clone();
        execute_operations_on_btree_map(&mut map, &additional_operations);
        map
    };
    assert_eq!(final_map, test_case.after);
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let before_digest = btree_map_to_digest(&test_case.before, &storage).await;
    let operations_executed =
        execute_operations_on_prolly_tree(&before_digest, &test_case.operations, &storage).await;
    let additional_operations_executed =
        execute_operations_on_prolly_tree(&operations_executed, &additional_operations, &storage)
            .await;
    let after_digest = btree_map_to_digest(&test_case.after, &storage).await;
    assert_eq!(after_digest, additional_operations_executed);
    verify_prolly_tree_equality_to_map(&after_digest, &final_map, &storage).await;
}

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let test_case: TestCase = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    println!("Test case: {:?}", test_case);
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            run_test_case(&test_case).await;
        });
    true
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_empty() {
    run_test_case(&TestCase {
        before: BTreeMap::new(),
        after: BTreeMap::new(),
        operations: vec![],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_no_operations() {
    run_test_case(&TestCase {
        before: BTreeMap::new(),
        after: BTreeMap::from([(10, 100), (20, 200), (30, 300)]),
        operations: vec![],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_matching_operations() {
    run_test_case(&TestCase {
        before: BTreeMap::new(),
        after: BTreeMap::from([(10, 100), (20, 200), (30, 300)]),
        operations: vec![
            MapOperation::Insert(10, 100),
            MapOperation::Insert(20, 200),
            MapOperation::Insert(30, 300),
        ],
    })
    .await;
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_mismatching_operations() {
    run_test_case(&TestCase {
        before: BTreeMap::new(),
        after: BTreeMap::from([(10, 100), (20, 200), (30, 300)]),
        operations: vec![
            MapOperation::Insert(10, 100),
            MapOperation::Insert(40, 200),
            MapOperation::Insert(30, 400),
        ],
    })
    .await;
}

#[cfg(test)]
#[test_log::test]
fn test_crash_0() {
    fuzz_function(&[
        255, 255, 255, 94, 207, 135, 196, 189, 12, 11, 158, 166, 32, 245, 148, 84, 78, 248, 53, 23,
        23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 184, 35, 156, 16, 137, 172, 105, 203, 119,
        49, 182, 132, 14, 12, 11, 158, 166, 36, 204, 151, 45, 175, 137, 136, 137, 46, 255, 255,
        255, 255, 255, 44, 217, 255, 255, 255, 197, 197, 255, 255, 94, 207, 135, 161, 196, 189, 12,
        11, 172, 171, 171, 171, 148, 84, 23, 23, 23, 23, 23, 23, 23, 184, 35, 156, 16, 137, 78,
        248, 53, 184, 35, 156, 16, 137, 172, 49, 182, 132, 14, 36, 204, 151, 45, 175, 137, 136,
        137, 46, 255, 79, 99, 99, 171, 255, 255, 255, 255, 44, 217, 13, 13,
    ]);
}
