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
        &BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference.digest()
    );
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(
        &HashedTree::from(Arc::new(Tree::empty())),
        loaded_back.hashed_tree()
    );

    assert_eq!(1, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(
        &HashedTree::from(Arc::new(Tree::empty())),
        loaded_back.hashed_tree()
    );
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
        &BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap(),
        reference_1.digest()
    );

    let reference_2 = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(reference_1.digest(), reference_2.digest());

    let loaded_back = storage
        .load_tree(reference_1.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(
        &HashedTree::from(Arc::new(Tree::empty())),
        loaded_back.hashed_tree()
    );

    assert_eq!(1, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference_1.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(
        &HashedTree::from(Arc::new(Tree::empty())),
        loaded_back.hashed_tree()
    );
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
        &BlobDigest::parse_hex_string("9be8213097a391e7b693a99d6645d11297b72113314f5e9ef98704205a7c795e41819a670fb10a60b4ca6aa92b4abd8a50932503ec843df6c40219d49f08a623").unwrap(),
        reference.digest()
    );
    let expected = HashedTree::from(tree);
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(1, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());
}

#[test_log::test(tokio::test)]
async fn test_store_reference() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let child_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::from("ref")).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(vec![child_reference]).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
        &BlobDigest::parse_hex_string("2a5e58d44738686013ea93248096f982b2ad03dfce91e5235247d5e3c3f4acc0376d2628f68b75c4afbe9484459465ccdaefe402ef3c42de270b2db096cc5c82").unwrap(),
        reference.digest()
    );
    let expected = HashedTree::from(tree);
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(3, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());
}

#[test_log::test(tokio::test)]
async fn test_store_two_references() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let mut child_references = Vec::new();
    for i in 0..2u32 {
        let child_reference = storage
            .store_tree(&HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(Bytes::from_owner(i.to_be_bytes())).unwrap(),
                TreeChildren::empty(),
            ))))
            .await
            .unwrap();
        child_references.push(child_reference);
    }
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(child_references).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
            &BlobDigest::parse_hex_string("b930b9fca665646ecf4739c0a752c8dbbfaf4846afdc9f84f7fbb287a2e5f19947e4e827d2a68bf49afb15f10e84efab05ac7b65dbfaa3965a74364ed9ac1bf6").unwrap(),
            reference.digest()
        );
    let expected = HashedTree::from(tree);
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(5, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());
}

#[test_log::test(tokio::test)]
async fn test_store_three_references() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let mut child_references = Vec::new();
    for i in 0..3u32 {
        let child_reference = storage
            .store_tree(&HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(Bytes::from_owner(i.to_be_bytes())).unwrap(),
                TreeChildren::empty(),
            ))))
            .await
            .unwrap();
        child_references.push(child_reference);
    }
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(Bytes::from("test 123")).unwrap(),
        TreeChildren::try_from(child_references).unwrap(),
    ));
    let reference = storage
        .store_tree(&HashedTree::from(tree.clone()))
        .await
        .unwrap();
    assert_eq!(
            &BlobDigest::parse_hex_string("e9d226a530b8674f757f692a5ff180082fe6feca53a2c711814a6877548b9fe78652e62a679b6b611739cfc8f6624dd6e3cd7b74e2f2be57a2f942ae4f1e1ef3").unwrap(),
            reference.digest()
        );
    let expected = HashedTree::from(tree);
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(7, storage.commit_changes().await.unwrap());

    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());
}

#[test_log::test(tokio::test)]
async fn test_load_tree_not_found() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference = BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap();
    let result = storage.load_tree(&reference).await;
    assert_eq!(LoadError::TreeNotFound(reference), result.unwrap_err());
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
    assert_eq!(Ok(Some(reference_2.clone())), storage.load_root(name).await);
    assert_eq!(4, storage.commit_changes().await.unwrap());
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
    assert_eq!(
        Ok(Some(reference_1.clone())),
        storage.load_root(name_1).await
    );
    storage.update_root(name_2, &reference_1).await.unwrap();
    assert_eq!(
        Ok(Some(reference_1.clone())),
        storage.load_root(name_1).await
    );
    assert_eq!(
        Ok(Some(reference_1.clone())),
        storage.load_root(name_2).await
    );
    assert_eq!(3, storage.commit_changes().await.unwrap());
    assert_eq!(
        Ok(Some(reference_1.clone())),
        storage.load_root(name_1).await
    );
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
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(1, storage.commit_changes().await.unwrap());

    // Verify we can still load after commit
    let loaded_back_after_commit = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back_after_commit.hashed_tree());
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
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(1, storage.commit_changes().await.unwrap());

    // Verify we can still load after commit
    let loaded_back_after_commit = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back_after_commit.hashed_tree());
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
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(1, storage.commit_changes().await.unwrap());

    // Verify we can still load after commit
    let loaded_back_after_commit = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back_after_commit.hashed_tree());
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
    let loaded_back = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back.hashed_tree());

    assert_eq!(1, storage.commit_changes().await.unwrap());

    // Verify we can still load after commit
    let loaded_back_after_commit = storage
        .load_tree(reference.digest())
        .await
        .unwrap()
        .hash()
        .unwrap();
    assert_eq!(&expected, loaded_back_after_commit.hashed_tree());
}

#[test_log::test(tokio::test)]
async fn test_compression_load_corrupted_blob() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let digest = BlobDigest::parse_hex_string("f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap();
    let digest_array: [u8; 64] = digest.into();
    connection
        .execute(
            "INSERT INTO tree (digest, is_compressed, tree_blob) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                digest_array,
                1u8,
                // Insert invalid compressed data
                vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            ],
        )
        .unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let loaded_back = storage.load_tree(&digest).await;
    assert_eq!(
        LoadError::Inconsistency(
            digest,
            "Failed to decompress tree blob using lz4: OffsetOutOfBounds".to_string()
        ),
        loaded_back.unwrap_err()
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
    let mut child_references = Vec::new();
    let child_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(Bytes::new()).unwrap(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    for _ in 0..TREE_MAX_CHILDREN {
        child_references.push(child_reference.clone());
    }
    let children = TreeChildren::try_from(child_references).unwrap();
    let stored = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            children,
        ))))
        .await
        .unwrap();
    assert_eq!(1002, storage.commit_changes().await.unwrap());
    let loaded_back = storage.load_tree(stored.digest()).await;
    assert!(loaded_back.is_ok());
    {
        let connection2 = rusqlite::Connection::open(&database_path).unwrap();
        let tree_row_id: i64 = {
            let stored_array: [u8; 64] = (*stored.digest()).into();
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
        LoadError::Inconsistency(
            *stored.digest(),
            "Tree has too many children: 1001".to_string()
        ),
        storage.load_tree(stored.digest()).await.unwrap_err()
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
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    drop(reference);
    assert_eq!(
        GarbageCollectionStats { trees_collected: 1 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(1, storage.commit_changes().await.unwrap());
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    storage.update_root("test", &reference).await.unwrap();
    drop(reference);
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
async fn test_collect_garbage_within_transaction() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    // store_tree starts a transaction, so this tests that garbage collection works correctly when there is an open transaction
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    drop(reference);
    assert_eq!(
        GarbageCollectionStats { trees_collected: 1 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(1, storage.commit_changes().await.unwrap());
}

#[test_log::test(tokio::test)]
async fn test_strong_reference() {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
    drop(reference);
    assert_eq!(
        GarbageCollectionStats { trees_collected: 1 },
        storage.collect_some_garbage().await.unwrap()
    );
    assert_eq!(
        GarbageCollectionStats { trees_collected: 0 },
        storage.collect_some_garbage().await.unwrap()
    );
}

#[test_log::test(tokio::test)]
async fn test_sql_errors() {
    let workspace = tempfile::tempdir().unwrap();
    let database_path = workspace.path().join("database.sqlite");
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
        .await
        .unwrap();
    drop(storage);
    let connection = rusqlite::Connection::open(&database_path).unwrap();
    // Delete important tables to make all operations fail with SQL errors.
    connection.execute("DROP TABLE tree", []).unwrap();
    connection.execute("DROP TABLE root", []).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    assert_eq!(
        LoadError::Rusqlite("no such table: tree".to_string()),
        storage.load_tree(reference.digest()).await.unwrap_err()
    );
    assert_eq!(
        StoreError::Rusqlite("no such table: tree".to_string()),
        storage
            .store_tree(&HashedTree::from(Arc::new(Tree::empty())))
            .await
            .unwrap_err()
    );
    assert_eq!(
        Err(LoadError::Rusqlite("no such table: root".to_string())),
        storage.load_root("test").await
    );
    assert_eq!(
        Err(StoreError::Rusqlite("no such table: tree".to_string())),
        storage.update_root("test", &reference).await
    );
    assert_eq!(
        Err(StoreError::Rusqlite("no such table: tree".to_string())),
        storage.collect_some_garbage().await
    );
}
