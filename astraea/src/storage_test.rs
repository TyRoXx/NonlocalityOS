use crate::{
    storage::{CommitChanges, LoadRoot, LoadValue, SQLiteStorage, StoreValue, UpdateRoot},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob},
};
use bytes::Bytes;
use std::sync::Arc;

#[test_log::test]
fn test_create_schema() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
}

#[test_log::test(tokio::test)]
async fn test_store_unit_first_time() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference = storage
        .store_value(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference
    );
    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_store_unit_again() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_value(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference_1
    );

    let reference_2 = storage
        .store_value(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(reference_1, reference_2);

    let loaded_back = storage
        .load_value(&reference_1)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference_1)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_store_blob() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        vec![],
    ));
    let reference = storage
        .store_value(&HashedTree::from(value.clone()))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("9be8213097a391e7b693a99d6645d11297b72113314f5e9ef98704205a7c795e41819a670fb10a60b4ca6aa92b4abd8a50932503ec843df6c40219d49f08a623").unwrap(),
        reference
    );
    let expected = HashedTree::from(value);
    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_store_reference() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let referenced_digest = BlobDigest::hash(b"ref");
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        vec![referenced_digest],
    ));
    let reference = storage
        .store_value(&HashedTree::from(value.clone()))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f9e26873d85cf34136a52d16c95dcbb557c302a60d6f2dadebea15dc769e0c8b1ca4137804bf82b4c668d65943c110db29bd6cef8493abe14b504b961e728e17").unwrap(),
        reference
    );
    let expected = HashedTree::from(value);
    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_store_two_references() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let referenced_digests = [b"a".as_slice(), b"ab"]
        .into_iter()
        .map(|element: &[u8]| BlobDigest::hash(element))
        .collect();
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        referenced_digests,
    ));
    let reference = storage
        .store_value(&HashedTree::from(value.clone()))
        .await
        .unwrap();
    assert_eq!(
            BlobDigest::parse_hex_string("ba085996952452402912ed9165e1515b30283897608e4a82d6c48740397c9cdac50321835d2749adb1f8278038dd2ab00b9a7e6a128a082e8b6ed7b0f00fd225").unwrap(),
            reference
        );
    let expected = HashedTree::from(value);
    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_store_three_references() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let referenced_digests = [b"a".as_slice(), b"ab", b"abc"]
        .into_iter()
        .map(|element: &[u8]| BlobDigest::hash(element))
        .collect();
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        referenced_digests,
    ));
    let reference = storage
        .store_value(&HashedTree::from(value.clone()))
        .await
        .unwrap();
    assert_eq!(
            BlobDigest::parse_hex_string("73dc0c58f0627b29dd0d09967e98318201504969e476b390e38e11b131faca075de24d114ba3d00524a402b88437d5b9c8ee654bbf3bb96e2ff23164a3ca4e49").unwrap(),
            reference
        );
    let expected = HashedTree::from(value);
    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_value(&reference)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_update_root() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_value(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let reference_2 = storage
        .store_value(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
            vec![],
        ))))
        .await
        .unwrap();
    let name = "test";
    assert_eq!(None, storage.load_root(name).await);
    storage.update_root(name, &reference_1).await;
    assert_eq!(Some(reference_1), storage.load_root(name).await);
    storage.update_root(name, &reference_2).await;
    assert_eq!(Some(reference_2), storage.load_root(name).await);

    storage.commit_changes().await.unwrap();
    assert_eq!(Some(reference_2), storage.load_root(name).await);
}

#[test_log::test(tokio::test)]
async fn test_roots_may_be_equal() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_value(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let name_1 = "testA";
    let name_2 = "testB";
    assert_eq!(None, storage.load_root(name_1).await);
    storage.update_root(name_1, &reference_1).await;
    assert_eq!(Some(reference_1), storage.load_root(name_1).await);
    storage.update_root(name_2, &reference_1).await;
    assert_eq!(Some(reference_1), storage.load_root(name_1).await);

    storage.commit_changes().await.unwrap();
    assert_eq!(Some(reference_1), storage.load_root(name_1).await);
    assert_eq!(Some(reference_1), storage.load_root(name_1).await);
}
