use arbitrary::{Arbitrary, Unstructured};
use astraea::{
    storage::{InMemoryTreeStorage, LoadTree},
    tree::BlobDigest,
};
use pretty_assertions::assert_eq;
use sorted_tree::{prolly_tree_editable_node::EditableNode, sorted_tree::TreeReference};
use std::collections::BTreeMap;
use tokio::sync::Mutex;

#[derive(Arbitrary, Debug)]
enum MapOperation {
    Insert(u32, i64),
    Remove(u32),
    Save,
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
            MapOperation::Save => {
                let saved_digest = editable_node.save(storage).await.unwrap();
                let reloaded_node: EditableNode<u32, i64> =
                    EditableNode::Reference(TreeReference::new(saved_digest));
                editable_node = reloaded_node;
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
            MapOperation::Save => {}
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
    let count = editable_node.count(storage).await.unwrap();
    assert_eq!(map.len() as u64, count);
}

async fn count_tree_node_count(root: &BlobDigest, storage: &InMemoryTreeStorage) -> u64 {
    let loaded = storage.load_tree(root).await.unwrap();
    let hashed = loaded.hash().unwrap();
    let mut sum = 1;
    for child in hashed.tree().children().references() {
        let child_count = Box::pin(count_tree_node_count(child, storage)).await;
        sum += child_count;
    }
    sum
}

async fn verify_prolly_trees_equal(
    digest1: &BlobDigest,
    digest2: &BlobDigest,
    storage: &InMemoryTreeStorage,
) {
    let mut editable_node1: EditableNode<u32, i64> =
        EditableNode::load(digest1, storage).await.unwrap();
    let mut editable_node2: EditableNode<u32, i64> =
        EditableNode::load(digest2, storage).await.unwrap();
    let element_count1 = editable_node1.count(storage).await.unwrap();
    let element_count2 = editable_node2.count(storage).await.unwrap();
    assert_eq!(element_count1, element_count2);
    let node_count1 = count_tree_node_count(digest1, storage).await;
    let node_count2 = count_tree_node_count(digest2, storage).await;
    assert_eq!(node_count1, node_count2);
    assert_eq!(digest1, digest2);
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
    if test_case.operations.is_empty() {
        verify_prolly_trees_equal(&before_digest, &operations_executed, &storage).await;
    }
    verify_prolly_tree_equality_to_map(&operations_executed, &intermediary_map, &storage).await;
    let additional_operations_executed =
        execute_operations_on_prolly_tree(&operations_executed, &additional_operations, &storage)
            .await;
    if additional_operations.is_empty() {
        verify_prolly_trees_equal(
            &operations_executed,
            &additional_operations_executed,
            &storage,
        )
        .await;
    }
    let after_digest = btree_map_to_digest(&test_case.after, &storage).await;
    verify_prolly_tree_equality_to_map(&after_digest, &final_map, &storage).await;
    verify_prolly_trees_equal(&after_digest, &additional_operations_executed, &storage).await;
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
#[test_log::test(tokio::test)]
async fn test_save_operation() {
    run_test_case(&TestCase {
        before: BTreeMap::new(),
        after: BTreeMap::from([(10, 100), (20, 200), (30, 300)]),
        operations: vec![
            MapOperation::Insert(10, 100),
            MapOperation::Insert(20, 200),
            MapOperation::Save,
            MapOperation::Insert(30, 300),
        ],
    })
    .await;
}

#[cfg(test)]
#[test_log::test]
fn test_crash_0() {
    fuzz_function(&[
        201, 255, 255, 219, 89, 89, 67, 75, 73, 89, 75, 240, 67, 243, 102, 0, 219, 170, 67, 75, 89,
        32, 240, 89, 67, 75, 33, 89, 75, 240, 67, 243, 32, 191, 157, 40, 255, 0, 255, 1, 149, 25,
        255, 255, 255, 0, 0, 255, 255, 58, 255, 43, 43, 154, 202, 0, 43, 43, 43, 43, 43, 43, 43,
        43, 43, 43, 43, 43, 43, 43, 43, 43, 255, 255, 239, 32, 75, 219, 89, 89, 241, 241, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 127, 255, 255, 255, 255, 225, 255, 255, 255, 255,
        46, 255, 93, 255, 254, 59, 253, 88, 255, 255, 46, 255, 93, 241, 241, 241, 241, 241, 243,
        241, 241, 219, 89, 89, 67, 75, 89, 0, 60, 255,
    ]);
}
