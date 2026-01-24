use arbitrary::{Arbitrary, Unstructured};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{
        BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH,
        TREE_MAX_CHILDREN,
    },
};
use dogbox_tree_editor::segmented_blob::{load_segmented_blob, save_segmented_blob};
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
    total_size: u64,
    /// Maximum children per tree (must be between 2 and TREE_MAX_CHILDREN)
    max_children_per_tree: usize,
}

async fn generate_segments<'a>(
    storage: &'a (dyn Send + Sync + StoreTree),
    test_case: &'a TestCase,
) -> Option<Vec<BlobDigest>> {
    let mut segments = Vec::new();
    let mut actual_total_size = 0u64;
    let num_segments = test_case.num_segments;
    let total_size = test_case.total_size;

    for i in 0..num_segments {
        // Create blob content with varying sizes, but respecting total_size
        let remaining = total_size.saturating_sub(actual_total_size);
        if remaining == 0 && i < num_segments - 1 {
            // If we've used up total_size but need more segments, reject this case
            return None;
        }

        let segment_size = if i == num_segments - 1 {
            // Last segment gets the remaining size
            remaining
        } else {
            // Earlier segments get an equal share, but not more than remaining
            let max_per_segment = remaining / ((num_segments - i) as u64);
            std::cmp::min(max_per_segment, TREE_BLOB_MAX_LENGTH as u64)
        };

        if segment_size == 0 && num_segments > 1 {
            // Can't have zero-size segments in multi-segment case
            return None;
        }

        if segment_size > TREE_BLOB_MAX_LENGTH as u64 {
            // Segment size exceeds tree blob max length
            return None;
        }

        let blob_content = vec![i as u8; segment_size as usize];
        actual_total_size += segment_size;

        let tree = Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(blob_content)).unwrap(),
            TreeChildren::empty(),
        );
        let hashed = HashedTree::from(Arc::new(tree));
        let digest = storage.store_tree(&hashed).await.unwrap();
        segments.push(digest);
    }

    // Ensure we match the expected total size
    assert_eq!(
        actual_total_size, total_size,
        "Internal test error: segment sizes don't match total_size"
    );

    return Some(segments);
}

async fn run_test_case(test_case: &TestCase) -> bool {
    let storage = InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    if let Some(segments) = generate_segments(&storage, test_case).await {
        let size_in_bytes = test_case.total_size;
        let max_children = test_case.max_children_per_tree.clamp(2, TREE_MAX_CHILDREN);

        // Save
        let saved_result =
            save_segmented_blob(segments.as_slice(), size_in_bytes, max_children, &storage);
        let saved = saved_result.await.expect("Failed to save the blob");

        // Load saved blob
        let loaded = load_segmented_blob(&saved, &storage)
            .await
            .expect("Failed to load the blob");

        return loaded.0 == segments && loaded.1 == size_in_bytes;
    } else {
        return false;
    }
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
            max_children_per_tree: 2,
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
            max_children_per_tree: 10,
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
            max_children_per_tree: 3,
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
            max_children_per_tree: 2,
        })
        .await
    );
}

#[cfg(test)]
#[test_log::test]
fn test_crash_0() {
    assert!(fuzz_function(&[239, 93, 42, 38]));
}
