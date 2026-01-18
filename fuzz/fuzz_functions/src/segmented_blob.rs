use arbitrary::{Arbitrary, Unstructured};
use astraea::{
    storage::{InMemoryTreeStorage, LoadTree, StoreTree},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use dogbox_tree::serialization::SegmentedBlob;
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Maximum number of segments to test with to keep fuzzing fast
const MAX_SEGMENTS_FOR_FUZZING: usize = 100;

/// Maximum total size to test with to keep fuzzing fast
const MAX_TOTAL_SIZE_FOR_FUZZING: u64 = 10 * 1024 * 1024; // 10 MB

#[derive(Arbitrary, Debug)]
struct TestCase {
    /// Number of segments in the blob (limited to keep fuzzing fast)
    num_segments: u8,
    /// Total size of the blob in bytes
    total_size: u32,
}

async fn run_test_case(test_case: &TestCase) -> bool {
    // Limit the number of segments to a reasonable value for fuzzing
    let num_segments = (test_case.num_segments as usize).min(MAX_SEGMENTS_FOR_FUZZING);
    if num_segments == 0 {
        return false;
    }

    // Limit the total size to keep fuzzing fast
    let total_size = (test_case.total_size as u64).min(MAX_TOTAL_SIZE_FOR_FUZZING);

    // Test serialization and deserialization of SegmentedBlob
    let info = SegmentedBlob {
        size_in_bytes: total_size,
    };

    // Serialize using postcard
    let serialized = match postcard::to_allocvec(&info) {
        Ok(data) => data,
        Err(_) => return false, // Reject invalid inputs
    };

    // Deserialize using postcard
    let deserialized: SegmentedBlob = match postcard::from_bytes(&serialized) {
        Ok(data) => data,
        Err(_) => {
            // Deserialization should not fail after successful serialization
            panic!("Deserialization failed after successful serialization");
        }
    };

    // Verify round-trip consistency
    assert_eq!(
        deserialized.size_in_bytes, info.size_in_bytes,
        "Deserialized size should match original size"
    );

    // Test with TreeBlob storage (simulating real usage)
    let storage = Arc::new(InMemoryTreeStorage::new(Mutex::new(BTreeMap::new())));

    // Create dummy segments - each segment is just a tree with some blob data
    let mut segments = Vec::new();
    for i in 0..num_segments {
        let blob_content = format!("segment_{}", i);
        let tree = Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(blob_content)).unwrap(),
            TreeChildren::empty(),
        );
        let hashed = HashedTree::from(Arc::new(tree));
        let digest = storage.store_tree(&hashed).await.unwrap();
        segments.push(digest);
    }

    // Create a tree with the serialized SegmentedBlob
    // For single segment, test with empty children; for multiple segments, test with children
    let tree = if segments.len() > 1 {
        Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(serialized.clone())).unwrap(),
            TreeChildren::try_from(segments.clone()).unwrap(),
        )
    } else {
        Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(serialized.clone())).unwrap(),
            TreeChildren::empty(),
        )
    };
    let hashed = HashedTree::from(Arc::new(tree));
    let digest = storage.store_tree(&hashed).await.unwrap();

    // Verify we can read it back
    let loaded_tree = storage.load_tree(&digest).await.unwrap();
    let hashed_loaded = loaded_tree.hash().unwrap();
    let tree_ref = hashed_loaded.tree().as_ref();

    // Deserialize the blob info
    let loaded_info: SegmentedBlob = match postcard::from_bytes(tree_ref.blob().as_slice()) {
        Ok(data) => data,
        Err(_) => {
            panic!("Failed to deserialize SegmentedBlob from stored tree");
        }
    };

    assert_eq!(
        loaded_info.size_in_bytes, total_size,
        "Loaded info size should match"
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
async fn test_single_segment() {
    assert!(
        run_test_case(&TestCase {
            num_segments: 1,
            total_size: 100,
        })
        .await
    );
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_multiple_segments_flat() {
    assert!(
        run_test_case(&TestCase {
            num_segments: 5,
            total_size: 500,
        })
        .await
    );
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_multiple_segments_hierarchical() {
    assert!(
        run_test_case(&TestCase {
            num_segments: 20,
            total_size: 2000,
        })
        .await
    );
}

#[cfg(test)]
#[test_log::test(tokio::test)]
async fn test_zero_segments() {
    assert!(
        !run_test_case(&TestCase {
            num_segments: 0,
            total_size: 0,
        })
        .await
    );
}
