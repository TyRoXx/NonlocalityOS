use dogbox_tree::serialization::SegmentedBlob;
use pretty_assertions::assert_eq;
use std::sync::Arc;

use crate::segmented_blob::{load_segmented_blob, save_segmented_blob};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadTree, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_0() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 2;
    let digest = save_segmented_blob(&[], 0, max_children_per_tree, &storage).await;
    assert_eq!(Err(astraea::storage::StoreError::Unrepresentable), digest);
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
    let original_segments = [segment];
    let digest = save_segmented_blob(
        &original_segments,
        total_size as u64,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(segment, digest);
    assert_eq!(1, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(&digest, &storage).await.unwrap();
    assert_eq!(original_segments, &loaded_segments[..]);
    assert_eq!(total_size as u64, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_2() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 2;
    let segment_0 = BlobDigest::parse_hex_string(
            "77e712cf05e19dcd622c502a3167027f9ce838094c82cef0fbf853c9b5fe2e22ce1af698fb306feb586019ddadc923f5b8f70a8c004b9f84b451be453930be14"
        )
        .unwrap();
    let segment_1 = BlobDigest::parse_hex_string(
            "12e712cf05e19dcd622c502a3167027f9ce838094c82cef0fbf853c9b5fe2e22ce1af698fb306feb586019ddadc923f5b8f70a8c004b9f84b451be453930be14"
        )
        .unwrap();
    let original_segments = [segment_0, segment_1];
    let total_size = TREE_BLOB_MAX_LENGTH as u64 + 1;
    let digest = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "5053af00af4b1c74bd569b398eb2f3195ac9145d6d94c52c5fac4bae010f88cabae1f2e32c78a6be80e1c97d42ad37b97718aaccea2e3bfe781f33ec6ad60ed0"
        )
        .unwrap(), digest);
    assert_eq!(1, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(&digest, &storage).await.unwrap();
    assert_eq!(original_segments, &loaded_segments[..]);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_5() {
    let storage = InMemoryTreeStorage::empty();
    let max_children_per_tree = 5;
    let segment = BlobDigest::parse_hex_string(
            "77e712cf05e19dcd622c502a3167027f9ce838094c82cef0fbf853c9b5fe2e22ce1af698fb306feb586019ddadc923f5b8f70a8c004b9f84b451be453930be14"
        )
        .unwrap();
    let original_segments = (0..max_children_per_tree)
        .map(|_| segment)
        .collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let digest = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "1a7c94138b50a211775a1ed2a71a122d815442bf1eecd619e00e9c336429a8afd701523c7549b8d33e3eb48d51673dd4ba1503e8e2c39d17704a15a3d2a27016"
        )
        .unwrap(), digest);
    assert_eq!(1, storage.number_of_trees().await);
    let (loaded_segments, loaded_size) = load_segmented_blob(&digest, &storage).await.unwrap();
    assert_eq!(original_segments, &loaded_segments[..]);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_one_indirection() {
    let max_children_per_tree = 5;
    let number_of_segments = max_children_per_tree + 1;
    let storage = InMemoryTreeStorage::empty();
    let segment = BlobDigest::parse_hex_string(
            "77e712cf05e19dcd622c502a3167027f9ce838094c82cef0fbf853c9b5fe2e22ce1af698fb306feb586019ddadc923f5b8f70a8c004b9f84b451be453930be14"
        )
        .unwrap();
    let original_segments = (0..number_of_segments).map(|_| segment).collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let digest = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "7f8c78d3ce7e4dac5d0fbf243c1ae56fec22d579c54c7595292efbd829688364d8890a30fd689bfa4086512d3f849cc314166039b3b8da330163baf53d8164a7"
        )
        .unwrap(), digest);
    assert_eq!(2, storage.number_of_trees().await);
    let inner_layer = BlobDigest::parse_hex_string(
            "1a7c94138b50a211775a1ed2a71a122d815442bf1eecd619e00e9c336429a8afd701523c7549b8d33e3eb48d51673dd4ba1503e8e2c39d17704a15a3d2a27016"
        )
        .unwrap();
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_allocvec(&SegmentedBlob {
                    size_in_bytes: total_size,
                })
                .unwrap(),
            ))
            .unwrap(),
            TreeChildren::try_from(vec![inner_layer, segment]).unwrap(),
        ),
        storage
            .load_tree(&digest)
            .await
            .unwrap()
            .hash()
            .unwrap()
            .tree()
            .as_ref()
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
                    .map(|_| segment)
                    .collect::<Vec<_>>()
            )
            .unwrap(),
        ),
        storage
            .load_tree(&inner_layer)
            .await
            .unwrap()
            .hash()
            .unwrap()
            .tree()
            .as_ref()
    );
    let (loaded_segments, loaded_size) = load_segmented_blob(&digest, &storage).await.unwrap();
    assert_eq!(&original_segments, &loaded_segments[..]);
    assert_eq!({ total_size }, loaded_size);
}

#[test_log::test(tokio::test)]
async fn test_save_segmented_blob_two_indirections() {
    let max_children_per_tree = 5;
    let number_of_segments = (max_children_per_tree * max_children_per_tree) + 1;
    let storage = InMemoryTreeStorage::empty();
    let segment = BlobDigest::parse_hex_string(
            "77e712cf05e19dcd622c502a3167027f9ce838094c82cef0fbf853c9b5fe2e22ce1af698fb306feb586019ddadc923f5b8f70a8c004b9f84b451be453930be14"
        )
        .unwrap();
    let original_segments = (0..number_of_segments).map(|_| segment).collect::<Vec<_>>();
    let total_size = (TREE_BLOB_MAX_LENGTH as u64) * (original_segments.len() as u64);
    let digest = save_segmented_blob(
        &original_segments,
        total_size,
        max_children_per_tree,
        &storage,
    )
    .await
    .unwrap();
    assert_eq!(BlobDigest::parse_hex_string(
            "aae782347596ab9dad0425c91c88494f8d098c6083ef5ffb9bd7b7ace5f55fea38766b425ecfe51d91e8aad301f348eb3da5c469b7ca012117d1db344c999b29"
        )
        .unwrap(), digest);
    assert_eq!(3, storage.number_of_trees().await);
    let inner_layer = BlobDigest::parse_hex_string(
            "a2a52a97c1ada926e533e5003fd1471ed73add6b66b69187b4862c6f22103379232c2429bbb28828d3ce234e867410e6c21c61e56a20c1861a61dbb1a19c98b7"
        )
        .unwrap();
    assert_eq!(
        &Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(
                postcard::to_allocvec(&SegmentedBlob {
                    size_in_bytes: total_size,
                })
                .unwrap(),
            ))
            .unwrap(),
            TreeChildren::try_from(vec![inner_layer, segment]).unwrap(),
        ),
        storage
            .load_tree(&digest)
            .await
            .unwrap()
            .hash()
            .unwrap()
            .tree()
            .as_ref()
    );
    let (loaded_segments, loaded_size) = load_segmented_blob(&digest, &storage).await.unwrap();
    assert_eq!(&original_segments, &loaded_segments[..]);
    assert_eq!({ total_size }, loaded_size);
}
