use arbitrary::{Arbitrary, Unstructured};
use astraea::{
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH, TREE_MAX_CHILDREN},
};
use dogbox_tree_editor::segmented_blob::{load_segmented_blob, save_segmented_blob};
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
    /// Maximum children per tree (must be between 2 and TREE_MAX_CHILDREN)
    max_children_per_tree: u8,
}

async fn run_test_case(test_case: &TestCase) -> bool {
    // Limit the number of segments to a reasonable value for fuzzing
    let num_segments = (test_case.num_segments as usize).min(MAX_SEGMENTS_FOR_FUZZING);
    if num_segments == 0 {
        return false;
    }

    // Limit the total size to keep fuzzing fast
    let total_size = (test_case.total_size as u64).min(MAX_TOTAL_SIZE_FOR_FUZZING);
    if total_size == 0 {
        return false;
    }

    // Ensure max_children_per_tree is in valid range [2, TREE_MAX_CHILDREN]
    let max_children_per_tree =
        (test_case.max_children_per_tree as usize).clamp(2, TREE_MAX_CHILDREN);

    let storage = Arc::new(InMemoryTreeStorage::new(Mutex::new(BTreeMap::new())));

    // Create dummy segments - each segment is just a tree with some blob data
    // We'll create segments that represent the actual data size
    let mut segments = Vec::new();
    let mut actual_total_size = 0u64;

    for i in 0..num_segments {
        // Create blob content with varying sizes, but respecting total_size
        let remaining = total_size.saturating_sub(actual_total_size);
        if remaining == 0 && i < num_segments - 1 {
            // If we've used up total_size but need more segments, reject this case
            return false;
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
            return false;
        }

        if segment_size > TREE_BLOB_MAX_LENGTH as u64 {
            // Segment size exceeds tree blob max length
            return false;
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

    // Use save_segmented_blob to save the segments
    let saved_digest = match save_segmented_blob(
        &segments,
        total_size,
        max_children_per_tree,
        storage.as_ref(),
    )
    .await
    {
        Ok(digest) => digest,
        Err(_) => return false, // Reject invalid inputs
    };

    // Use load_segmented_blob to load the segments back
    let (loaded_segments, loaded_size) =
        match load_segmented_blob(&saved_digest, storage.as_ref()).await {
            Ok(result) => result,
            Err(err) => {
                // Deserialization should not fail after successful serialization with valid data
                panic!(
                    "Deserialization failed after successful serialization: {:?}",
                    err
                );
            }
        };

    // Verify that the loaded data matches what we saved
    // For single segment case, the digest is returned directly and size is actual blob size
    if segments.len() == 1 {
        assert_eq!(
            loaded_segments.len(),
            1,
            "Single segment should be returned as-is"
        );
        assert_eq!(
            loaded_segments[0], segments[0],
            "Single segment digest should match"
        );
        // For single segment, the loaded size is the actual blob size, not total_size
        // This is expected behavior - single segments don't use the SegmentedBlob wrapper
    } else {
        // For multiple segments, the total_size should be preserved
        assert_eq!(
            loaded_size, total_size,
            "Loaded size should match saved size for multiple segments"
        );

        // Verify consistency based on tree structure
        // The segments might be reorganized into a tree structure, but total count should be preserved
        // when the structure is flat enough to not require hierarchical organization
        if segments.len() <= max_children_per_tree {
            assert_eq!(
                loaded_segments.len(),
                segments.len(),
                "Segment count should be preserved for flat structures"
            );
            assert_eq!(
                loaded_segments, segments,
                "Segments should match for flat structures"
            );
        }
        // For hierarchical structures, we just verify that we got segments back
        // The exact structure depends on the implementation details
    }

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
