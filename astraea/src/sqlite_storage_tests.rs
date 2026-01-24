use crate::{
    sqlite_storage::SQLiteStorage,
    storage::{
        CollectGarbage, CommitChanges, GarbageCollectionStats, LoadError, LoadRoot, LoadTree,
        StoreError, StoreTree, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_MAX_CHILDREN},
};
use bytes::Bytes;
use pretty_assertions::assert_eq;
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
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference
    );
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);
}

#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_store_unit_again() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference_1
    );

    let reference_2 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(reference_1, reference_2);

    let loaded_back = storage
        .load_tree(&reference_1)
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(HashedTree::from(Arc::new(Tree::empty())), loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage
        .load_tree(&reference_1)
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
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::empty(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("9be8213097a391e7b693a99d6645d11297b72113314f5e9ef98704205a7c795e41819a670fb10a60b4ca6aa92b4abd8a50932503ec843df6c40219d49f08a623").unwrap(),
        reference
    );
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_store_reference() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let referenced_digest = BlobDigest::hash(b"ref");
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(vec![referenced_digest]).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string("f9e26873d85cf34136a52d16c95dcbb557c302a60d6f2dadebea15dc769e0c8b1ca4137804bf82b4c668d65943c110db29bd6cef8493abe14b504b961e728e17").unwrap(),
        reference
    );
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
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
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(referenced_digests).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
            BlobDigest::parse_hex_string("ba085996952452402912ed9165e1515b30283897608e4a82d6c48740397c9cdac50321835d2749adb1f8278038dd2ab00b9a7e6a128a082e8b6ed7b0f00fd225").unwrap(),
            reference
        );
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
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
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(referenced_digests).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
            BlobDigest::parse_hex_string("73dc0c58f0627b29dd0d09967e98318201504969e476b390e38e11b131faca075de24d114ba3d00524a402b88437d5b9c8ee654bbf3bb96e2ff23164a3ca4e49").unwrap(),
            reference
        );
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);
}

#[test_log::test(tokio::test)]
async fn test_load_tree_not_found() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference = BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap();
    let result = storage.load_tree(&reference).await;
    assert_eq!(Err(LoadError::TreeNotFound(reference)), result);
}

#[test_log::test(tokio::test)]
async fn test_update_root() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let reference_2 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let name = "test";
    assert_eq!(Ok(None), storage.load_root(name).await);
    storage.update_root(name, &reference_1).await.unwrap();
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name).await);
    storage.update_root(name, &reference_2).await.unwrap();
    assert_eq!(Ok(Some(reference_2)), storage.load_root(name).await);
    storage.commit_changes().await.unwrap();
    assert_eq!(Ok(Some(reference_2)), storage.load_root(name).await);
}

#[test_log::test(tokio::test)]
async fn test_roots_may_be_equal() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference_1 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    let name_1 = "testA";
    let name_2 = "testB";
    assert_eq!(Ok(None), storage.load_root(name_1).await);
    storage.update_root(name_1, &reference_1).await.unwrap();
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name_1).await);
    storage.update_root(name_2, &reference_1).await.unwrap();
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name_1).await);
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name_2).await);
    storage.commit_changes().await.unwrap();
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name_1).await);
    assert_eq!(Ok(Some(reference_1)), storage.load_root(name_2).await);
}

#[test_log::test(tokio::test)]
async fn test_compression_compressible_data() {
    // Test that compressible data works correctly with compression
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();

    // Create a highly compressible blob (repeated data)
    let compressible_data = "A".repeat(1000);
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from(compressible_data.clone())).unwrap(),
        TreeChildren::empty(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();

    // Verify we can load it back correctly
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    // Verify we can still load after commit
    let loaded_back_after_commit = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back_after_commit);
}

#[test_log::test(tokio::test)]
async fn test_compression_uncompressible_data() {
    // Test that uncompressible data is stored and retrieved correctly
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();

    // Create random-like data that won't compress well
    let uncompressible_data: Vec<u8> = (0..100).map(|i| (i * 7 + 13) as u8).collect();
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from(uncompressible_data.clone())).unwrap(),
        TreeChildren::empty(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();

    // Verify we can load it back correctly
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    // Verify we can still load after commit
    let loaded_back_after_commit = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back_after_commit);
}

#[test_log::test(tokio::test)]
async fn test_compression_large_blob() {
    // Test compression with a larger blob
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();

    // Create a large compressible blob (half repetitive, half varied)
    let mut large_data = "ABCDEFGH".repeat(500);
    large_data.push_str(
        &(0..500)
            .map(|i| ((i % 26) as u8 + b'a') as char)
            .collect::<String>(),
    );

    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from(large_data.clone())).unwrap(),
        TreeChildren::empty(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();

    // Verify we can load it back correctly
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    // Verify we can still load after commit
    let loaded_back_after_commit = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back_after_commit);
}

#[test_log::test(tokio::test)]
async fn test_compression_empty_blob() {
    // Test that empty blobs work correctly
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();

    let tree = Arc::new(Tree::empty());
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();

    // Verify we can load it back correctly
    let expected = HashedTree::from(tree);
    let loaded_back = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back);

    storage.commit_changes().await.unwrap();

    // Verify we can still load after commit
    let loaded_back_after_commit = storage.load_tree(&reference).await.unwrap().hash().unwrap();
    assert_eq!(expected, loaded_back_after_commit);
}

#[test_log::test(tokio::test)]
async fn test_compression_load_corrupted_blob() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let reference = BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap();
    let reference_digest: [u8; 64] = reference.into();
    connection
        .execute(
            "INSERT INTO tree (digest, is_compressed, tree_blob) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                reference_digest,
                1u8,
                // Insert invalid compressed data
                vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            ],
        )
        .unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let loaded_back = storage.load_tree(&reference).await;
    assert_eq!(
        Err(LoadError::Rusqlite("Query is not read-only".to_string())),
        loaded_back
    );
}

#[test_log::test(tokio::test)]
async fn test_load_too_many_children() {
    let workspace = tempfile::tempdir().unwrap();
    let database_path = workspace.path().join("database.sqlite");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference = BlobDigest::parse_hex_string(concat!(
        "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
        "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
    ))
    .unwrap();
    let children =
        TreeChildren::try_from(std::iter::repeat_n(reference, TREE_MAX_CHILDREN).collect())
            .unwrap();
    let stored = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            children,
        ))))
        .await
        .unwrap();
    storage.commit_changes().await.unwrap();
    let loaded_back = storage.load_tree(&stored).await;
    assert!(loaded_back.is_ok());
    {
        let connection2 = rusqlite::Connection::open(&database_path).unwrap();
        let tree_row_id: i64 = {
            let stored_array: [u8; 64] = stored.into();
            let mut statement = connection2
                .prepare_cached("SELECT id FROM tree WHERE digest = ?1")
                .unwrap();
            statement
                .query_row(
                    (&stored_array,),
                    |row: &rusqlite::Row<'_>| -> rusqlite::Result<_, rusqlite::Error> {
                        row.get(0)
                    },
                )
                .unwrap()
        };
        let reference_array: [u8; 64] = reference.into();
        connection2
            .execute(
                "INSERT INTO reference (origin, zero_based_index, target) VALUES (?1, ?2, ?3)",
                (tree_row_id, TREE_MAX_CHILDREN as i64, &reference_array),
            )
            .unwrap();
    }
    assert_eq!(
        Err(LoadError::Inconsistency(
            stored,
            "Tree has too many children: 1001".to_string()
        )),
        storage.load_tree(&stored).await
    );
}

#[test_log::test(tokio::test)]
async fn test_collect_garbage() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(
        GarbageCollectionStats { trees_collected: 1 },
        storage.collect_some_garbage().await.unwrap()
    );
    let digest = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    storage.update_root("test", &digest).await.unwrap();
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn test_sql_errors() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    // We have not created the schema, so any operation should fail.
    let digest = BlobDigest::hash(b"ref1");
    assert_eq!(
        Err(LoadError::Rusqlite("no such table: tree".to_string())),
        storage.load_tree(&digest).await
    );
    assert_eq!(
        Err(StoreError::Rusqlite("no such table: tree".to_string())),
        storage
            .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
            .await
    );
    assert_eq!(
        Err(LoadError::Rusqlite("no such table: root".to_string())),
        storage.load_root("test").await
    );
    assert_eq!(
        Err(StoreError::Rusqlite("no such table: root".to_string())),
        storage.update_root("test", &digest).await
    );
    assert_eq!(
        Err(StoreError::Rusqlite("no such table: tree".to_string())),
        storage.collect_some_garbage().await
    );
}
