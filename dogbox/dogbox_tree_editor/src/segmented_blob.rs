use astraea::{
    storage::{LoadTree, StoreError, StoreTree, StrongReference},
    tree::{
        BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH,
        TREE_MAX_CHILDREN,
    },
};
use dogbox_tree::serialization::{DeserializationError, SegmentedBlob};
use std::sync::Arc;

pub async fn save_segmented_blob(
    segments: &[StrongReference],
    total_size_in_bytes: u64,
    max_children_per_tree: usize,
    storage: &(dyn StoreTree + Send + Sync),
) -> std::result::Result<StrongReference, StoreError> {
    save_segmented_blob_impl(
        segments,
        TREE_BLOB_MAX_LENGTH as u64,
        total_size_in_bytes,
        max_children_per_tree,
        storage,
    )
    .await
}

// TODO: This function recursively builds a tree structure for large blobs, but the algorithm is not clearly documented.
// How does the tree height grow with the number of segments? Is there a maximum depth?
// What's the relationship between segment_capacity and max_children_per_tree in determining the tree structure?
// Why does segment_capacity get multiplied by max_children_per_tree in the recursive call (line 64)?
// Is this building a B-tree-like structure? Should this be explained in module-level documentation?
async fn save_segmented_blob_impl(
    segments: &[StrongReference],
    segment_capacity: u64,
    total_size_in_bytes: u64,
    max_children_per_tree: usize,
    storage: &(dyn StoreTree + Send + Sync),
) -> std::result::Result<StrongReference, StoreError> {
    assert!(max_children_per_tree >= 2);
    assert!(max_children_per_tree <= TREE_MAX_CHILDREN);
    match segments.len() {
        0 => Err(StoreError::Unrepresentable),
        1 => Ok(segments[0].clone()),
        _ => {
            if segments.len() > max_children_per_tree {
                // TODO: Why is chunking done here before recursion? Would it be clearer to have a separate function
                // for building internal tree nodes vs. leaf nodes?
                let mut chunks = Vec::new();
                let mut remaining_size = total_size_in_bytes;
                for chunk in segments.chunks(max_children_per_tree) {
                    let capacity = (chunk.len() as u64) * segment_capacity;
                    // TODO: This size calculation seems complex. What happens if remaining_size doesn't divide evenly?
                    // Could there be an off-by-one error in the last chunk?
                    let chunk_size = if remaining_size <= capacity {
                        remaining_size
                    } else {
                        capacity
                    };
                    remaining_size -= chunk_size;
                    chunks.push(
                        Box::pin(save_segmented_blob_impl(
                            chunk,
                            segment_capacity,
                            chunk_size,
                            max_children_per_tree,
                            storage,
                        ))
                        .await?,
                    );
                }
                return Box::pin(save_segmented_blob_impl(
                    &chunks,
                    segment_capacity * max_children_per_tree as u64,
                    total_size_in_bytes,
                    max_children_per_tree,
                    storage,
                ))
                .await;
            }
            let info = SegmentedBlob {
                size_in_bytes: total_size_in_bytes,
            };
            let children = TreeChildren::try_from(segments.to_vec())
                .expect("The child count was checked above.");
            let tree = Tree::new(
                // TODO: Under what conditions could postcard serialization or TreeBlob conversion fail here?
                // Should these unwraps be proper error handling to avoid panics on malformed data?
                TreeBlob::try_from(bytes::Bytes::from(postcard::to_allocvec(&info).unwrap()))
                    .unwrap(),
                children,
            );
            let reference = storage
                .store_tree(&HashedTree::from(Arc::new(tree)))
                .await?;
            Ok(reference)
        }
    }
}

pub async fn load_segmented_blob(
    digest: &BlobDigest,
    storage: &(dyn LoadTree + Send + Sync),
) -> std::result::Result<(Vec<StrongReference>, u64), DeserializationError> {
    let delayed_tree = match storage.load_tree(digest).await {
        Ok(loaded) => loaded,
        Err(error) => return Err(DeserializationError::Load(error)),
    };
    let hashed_tree = match delayed_tree.hash() {
        Some(hashed) => hashed,
        None => return Err(DeserializationError::TreeHashMismatch(*digest)),
    };
    let tree = hashed_tree.hashed_tree().tree().as_ref();
    if tree.children().references().is_empty() {
        Ok((
            vec![hashed_tree.reference().clone()],
            tree.blob().as_slice().len() as u64,
        ))
    } else {
        let info: SegmentedBlob =
            postcard::from_bytes(tree.blob().as_slice()).map_err(DeserializationError::Postcard)?;
        let capacity = (tree.children().references().len() as u64) * (TREE_BLOB_MAX_LENGTH as u64);
        if info.size_in_bytes <= capacity {
            let segments = tree.children().references().to_vec();
            return Ok((segments, info.size_in_bytes));
        }
        let mut remaining_size = info.size_in_bytes;
        let mut all_segments = Vec::new();
        for segment_reference in tree.children().references().iter() {
            if remaining_size == 0 {
                return Err(DeserializationError::Inconsistency(
                    "Segmented blob has more segments than needed for the total size.".to_string(),
                ));
            }
            if remaining_size <= TREE_BLOB_MAX_LENGTH as u64 {
                all_segments.push(segment_reference.clone());
                remaining_size = 0;
            } else {
                let (mut loaded_segments, segment_size) =
                    Box::pin(load_segmented_blob(segment_reference.digest(), storage)).await?;
                all_segments.append(&mut loaded_segments);
                remaining_size = match remaining_size.checked_sub(segment_size) {
                    Some(size) => size,
                    None => {
                        return Err(DeserializationError::Inconsistency(
                            "Segmented blob segment sizes don't add up to the total size."
                                .to_string(),
                        ))
                    }
                };
            }
        }
        if remaining_size > 0 {
            return Err(DeserializationError::Inconsistency(
                "Segmented blob has fewer segments than needed for the total size.".to_string(),
            ));
        }
        Ok((all_segments, info.size_in_bytes))
    }
}
