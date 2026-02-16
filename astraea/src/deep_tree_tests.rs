use crate::{
    deep_tree::{DeepTree, DeepTreeChildren},
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadError, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren},
};
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_simple_tree() {
    let storage = InMemoryTreeStorage::empty();
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(reference.digest(), &storage).await;
    assert_eq!(
        Ok(DeepTree::new(TreeBlob::empty(), DeepTreeChildren::empty())),
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_blob() {
    let storage = InMemoryTreeStorage::empty();
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(reference.digest(), &storage).await;
    assert_eq!(
        Ok(DeepTree::new(
            TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
            DeepTreeChildren::empty()
        )),
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_reference() {
    let storage = InMemoryTreeStorage::empty();
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::try_from(vec![storage
                .store_tree(&HashedTree::from(Arc::new(Tree::new(
                    TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
                    TreeChildren::empty(),
                ))))
                .await
                .unwrap()])
            .unwrap(),
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(reference.digest(), &storage).await;
    assert_eq!(
        Ok(DeepTree::new(
            TreeBlob::empty(),
            DeepTreeChildren::try_from(vec![DeepTree::new(
                TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
                DeepTreeChildren::empty()
            )])
            .unwrap()
        )),
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_not_found() {
    let digest = BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap();
    let storage = InMemoryTreeStorage::empty();
    let result = DeepTree::deserialize(&digest, &storage).await;
    assert_eq!(Err(LoadError::TreeNotFound(digest)), result);
}
