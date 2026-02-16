use crate::segmented_blob::{load_segmented_blob, save_segmented_blob};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadTree, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};
use dogbox_tree::serialization::SegmentedBlob;
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_0() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 2;
    let reference = save_segmented_blob(&[], 0, max_children_per_tree, &storage).await;
    assert_eq!(
        astraea::storage::StoreError::Unrepresentable,
        reference.unwrap_err()
    );
    assert_eq!(0, storage.number_of_trees().await);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_1() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 2;
    let total_size = 12;
    let segment = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; total_size])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    assert_eq!(1, storage.number_of_trees().await);
    let original_segments = [segment.clone()];
    let reference = save_segmented_blob(
        &original_segments,
        total_size as u64,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(segment.digest(), reference.digest());
    assert_eq!(1, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(reference.digest(), &storage)
        .await
        .unwrap();
    let expected_segments = original_segments.to_vec();
    assert_eq!(&expected_segments, &loaded_segments);
    assert_eq!(total_size as u64, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_2() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 2;
    let segment_0 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; TREE_BLOB_MAX_LENGTH])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let segment_1 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; 1])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    assert_eq!(2, storage.number_of_trees().await);
    let original_segments = [segment_0, segment_1];
    let total_size = TREE_BLOB_MAX_LENGTH as u64 + 1;
    let reference = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(&BlobDigest::parse_hex_string(
            "21d5cf946a7bedda1764049d28ce34c8ad8a8d02f162d12dd55442962e779beb46527b4eebd5977e990a082302e1447d489827e58e48b82f505ece30c57cb6bd"
        )
        .unwrap(), reference.digest());
    assert_eq!(3, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(reference.digest(), &storage)
        .await
        .unwrap();
    let expected_segments = original_segments.to_vec();
    assert_eq!(&expected_segments, &loaded_segments);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_5() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 5;
    let segment = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; 23])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let original_segments = (0..max_children_per_tree)
        .map(|_| segment.clone())
        .collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let reference = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(&BlobDigest::parse_hex_string(
            "849fcd5a8f0e29ae865f9f6fc37e9ef8b6b0bbfd7e5e3b5cb6b9d25e964d1f03ef9e7cab8848afd8c389c8655d26d426e0c69b640b56d35bc83fbda44aaa3ebe"
        )
        .unwrap(), reference.digest());
    assert_eq!(2, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(reference.digest(), &storage)
        .await
        .unwrap();
    let expected_segments = original_segments.to_vec();
    assert_eq!(&expected_segments, &loaded_segments);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_one_indirection() {
    let max_children_per_tree = 5;
    let number_of_segments = max_children_per_tree + 1;
    let storage = InMemoryTreeStorage::empty();
    let segment = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; 23])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let original_segments = (0..number_of_segments)
        .map(|_| segment.clone())
        .collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let reference = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(&BlobDigest::parse_hex_string(
            "759885644eac8cd3655dc3e160f9d9bf725e9d5df8d8b17c0b18bfa0475b4910d698a4c121a355c4523a16f01d80d6268a3e5485d9f86e66ab0e8ee01e54bc04"
        )
        .unwrap(), reference.digest());
    assert_eq!(3, storage.number_of_trees().await);
    let segmented_blob_loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    let inner_layer_digest = BlobDigest::parse_hex_string(
            "849fcd5a8f0e29ae865f9f6fc37e9ef8b6b0bbfd7e5e3b5cb6b9d25e964d1f03ef9e7cab8848afd8c389c8655d26d426e0c69b640b56d35bc83fbda44aaa3ebe"
        )
        .unwrap();
    assert_eq!(
        &inner_layer_digest,
        segmented_blob_loaded_back
            .hashed_tree()
            .tree()
            .children()
            .references()
            .first()
            .unwrap()
            .digest()
    );
    let inner_layer_reference = storage
        .load_tree(&inner_layer_digest)
        .await
        .unwrap()
        .hash()
        .unwrap()
        .reference()
        .clone();
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_allocvec(&SegmentedBlob {
                    size_in_bytes: total_size,
                })
                .unwrap(),
            ))
            .unwrap(),
            TreeChildren::try_from(vec![inner_layer_reference, segment.clone()]).unwrap(),
        ),
        segmented_blob_loaded_back.hashed_tree().tree().as_ref()
    );
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_allocvec(&SegmentedBlob {
                    size_in_bytes: (TREE_BLOB_MAX_LENGTH as u64) * (max_children_per_tree as u64),
                })
                .unwrap(),
            ))
            .unwrap(),
            TreeChildren::try_from(
                (0..max_children_per_tree)
                    .map(|_| segment.clone())
                    .collect::<Vec<_>>()
            )
            .unwrap(),
        ),
        storage
            .load_tree(&inner_layer_digest)
            .await
            .unwrap()
            .hash()
            .unwrap()
            .hashed_tree()
            .tree()
            .as_ref()
    );
    let (loaded_segments, loaded_size) = load_segmented_blob(reference.digest(), &storage)
        .await
        .unwrap();
    let expected_segments = original_segments.to_vec();
    assert_eq!(&expected_segments, &loaded_segments);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_two_indirections() {
    let max_children_per_tree = 5;
    let number_of_segments = (max_children_per_tree * max_children_per_tree) + 1;
    let storage = InMemoryTreeStorage::empty();
    let segment = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(vec![0u8; 23])).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let original_segments = (0..number_of_segments)
        .map(|_| segment.clone())
        .collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let reference = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(&BlobDigest::parse_hex_string(
            "c89cab53afae4969155cc532ae240ba15c111e2ef503d6c1a8cc2de75f20ac4f9e2777b7a643a2d0ee7aff9d2fb4b3b875b03f185da3f23220c19e13425a0e9e"
        )
        .unwrap(), reference.digest());
    assert_eq!(4, storage.number_of_trees().await);
    let segmented_blob_loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    let inner_layer_digest = BlobDigest::parse_hex_string(
            "b90154ae9b2cce688f1d48f8b64f2f83d62bfd0b159753f979c9d26b9dcfcecfe39f9ea2de6405aea4f445b7cb0b654873ee39ee675fb9daffd2fa41ced75805"
        )
        .unwrap();
    assert_eq!(
        &inner_layer_digest,
        segmented_blob_loaded_back
            .hashed_tree()
            .tree()
            .children()
            .references()
            .first()
            .unwrap()
            .digest()
    );
    let inner_layer_reference = storage
        .load_tree(&inner_layer_digest)
        .await
        .unwrap()
        .hash()
        .unwrap()
        .reference()
        .clone();
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_allocvec(&SegmentedBlob {
                    size_in_bytes: total_size,
                })
                .unwrap(),
            ))
            .unwrap(),
            TreeChildren::try_from(vec![inner_layer_reference, segment]).unwrap(),
        ),
        storage
            .load_tree(reference.digest())
            .await
            .unwrap()
            .hash()
            .unwrap()
            .hashed_tree()
            .tree()
            .as_ref()
    );
    let (loaded_segments, loaded_size) = load_segmented_blob(reference.digest(), &storage)
        .await
        .unwrap();
    let expected_segments = original_segments.to_vec();
    assert_eq!(&expected_segments, &loaded_segments);
    assert_eq!({ total_size }, loaded_size);
}
