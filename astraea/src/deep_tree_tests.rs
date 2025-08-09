use crate::{
    deep_tree::DeepTree,
    storage::{InMemoryTreeStorage, StoreTree},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use pretty_assertions::assert_eq;
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_simple_tree() {
    let storage = InMemoryTreeStorage::empty();
    let digest = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            vec![],
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(&digest, &storage).await;
    assert_eq!(Some(DeepTree::new(TreeBlob::empty(), vec![])), result);
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_blob() {
    let storage = InMemoryTreeStorage::empty();
    let digest = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
            vec![],
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(&digest, &storage).await;
    assert_eq!(
        Some(DeepTree::new(
            TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
            vec![]
        )),
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_reference() {
    let storage = InMemoryTreeStorage::empty();
    let digest = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            vec![storage
                .store_tree(&HashedTree::from(Arc::new(Tree::new(
                    TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
                    vec![],
                ))))
                .await
                .unwrap()],
        ))))
        .await
        .unwrap();
    let result = DeepTree::deserialize(&digest, &storage).await;
    assert_eq!(
        Some(DeepTree::new(
            TreeBlob::empty(),
            vec![DeepTree::new(
                TreeBlob::try_from(bytes::Bytes::from("test 123")).unwrap(),
                vec![]
            )]
        )),
        result
    );
}

#[test_log::test(tokio::test)]
async fn test_deep_tree_deserialize_not_found() {
    let storage = InMemoryTreeStorage::empty();
    let result =  DeepTree::deserialize(
        &BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        &storage,
    ).await;
    assert_eq!(None, result);
}
