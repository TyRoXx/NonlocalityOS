#[cfg(test)]
mod tests {
    use crate::{
        storage::{CommitChanges, LoadRoot, LoadValue, SQLiteStorage, StoreValue, UpdateRoot},
        tree::{BlobDigest, HashedValue, Reference, TypeId, TypedReference, Value, ValueBlob},
    };
    use bytes::Bytes;
    use std::sync::Arc;

    #[test_log::test(tokio::test)]
    async fn test_sqlcipher_encryption() {
        let temporary_directory = tempfile::tempdir().unwrap();
        let database_file_name = temporary_directory.path().join("test.sqlite");
        let expected_reference = BlobDigest::new(&[
            166, 159, 115, 204, 162, 58, 154, 197, 200, 181, 103, 220, 24, 90, 117, 110, 151, 201,
            130, 22, 79, 226, 88, 89, 224, 209, 220, 193, 71, 92, 128, 166, 21, 178, 18, 58, 241,
            245, 249, 76, 17, 227, 233, 64, 44, 58, 197, 88, 245, 0, 25, 157, 149, 182, 211, 227,
            1, 117, 133, 134, 40, 29, 205, 38,
        ]);
        let correct_key = "test1234";
        {
            let connection1 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection1.pragma_update(None, "key", correct_key).unwrap();
            SQLiteStorage::create_schema(&connection1).unwrap();
            let storage = SQLiteStorage::from(connection1).unwrap();
            let reference = storage
                .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
                .await
                .unwrap();
            assert_eq!(expected_reference, reference.digest);
            storage.commit_changes().await.unwrap();
        }
        // TODO: solve OpenSSL rebuild issues on Windows
        #[cfg(target_os = "linux")]
        {
            let incorrect_key = "test12345";
            let connection2 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection2
                .pragma_update(None, "key", incorrect_key)
                .unwrap();
            let result = SQLiteStorage::from(connection2).err();
            let expected = Some(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::NotADatabase,
                    extended_code: 26,
                },
                Some("file is not a database".to_string()),
            ));
            assert_eq!(&expected, &result);
        }
        {
            let connection3 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection3.pragma_update(None, "key", correct_key).unwrap();
            let storage = SQLiteStorage::from(connection3).unwrap();
            let loaded_back = storage
                .load_value(&Reference::new(expected_reference))
                .await
                .unwrap()
                .hash()
                .unwrap();
            assert_eq!(HashedValue::from(Arc::new(Value::from_unit())), loaded_back);
        }
    }

    #[test]
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
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::new(&[
                166, 159, 115, 204, 162, 58, 154, 197, 200, 181, 103, 220, 24, 90, 117, 110, 151,
                201, 130, 22, 79, 226, 88, 89, 224, 209, 220, 193, 71, 92, 128, 166, 21, 178, 18,
                58, 241, 245, 249, 76, 17, 227, 233, 64, 44, 58, 197, 88, 245, 0, 25, 157, 149,
                182, 211, 227, 1, 117, 133, 134, 40, 29, 205, 38
            ]),
            reference.digest
        );
        let loaded_back = storage
            .load_value(&reference)
            .await
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(HashedValue::from(Arc::new(Value::from_unit())), loaded_back);

        storage.commit_changes().await.unwrap();

        let loaded_back = storage
            .load_value(&reference)
            .await
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(HashedValue::from(Arc::new(Value::from_unit())), loaded_back);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_store_unit_again() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        let reference_1 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::new(&[
                166, 159, 115, 204, 162, 58, 154, 197, 200, 181, 103, 220, 24, 90, 117, 110, 151,
                201, 130, 22, 79, 226, 88, 89, 224, 209, 220, 193, 71, 92, 128, 166, 21, 178, 18,
                58, 241, 245, 249, 76, 17, 227, 233, 64, 44, 58, 197, 88, 245, 0, 25, 157, 149,
                182, 211, 227, 1, 117, 133, 134, 40, 29, 205, 38
            ]),
            reference_1.digest
        );

        let reference_2 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .await
            .unwrap();
        assert_eq!(reference_1.digest, reference_2.digest);

        let loaded_back = storage
            .load_value(&reference_1)
            .await
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(HashedValue::from(Arc::new(Value::from_unit())), loaded_back);

        storage.commit_changes().await.unwrap();

        let loaded_back = storage
            .load_value(&reference_1)
            .await
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(HashedValue::from(Arc::new(Value::from_unit())), loaded_back);
    }

    #[test_log::test(tokio::test)]
    async fn test_store_blob() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        let value = Arc::new(Value::new(
            ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
            vec![],
        ));
        let reference = storage
            .store_value(&HashedValue::from(value.clone()))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::new(&[
                130, 115, 235, 131, 140, 52, 158, 195, 128, 151, 52, 84, 4, 23, 120, 30, 186, 184,
                216, 102, 157, 132, 234, 172, 95, 141, 225, 255, 103, 69, 15, 200, 28, 184, 128,
                242, 157, 50, 240, 255, 14, 154, 197, 128, 74, 128, 191, 86, 117, 225, 34, 104, 53,
                16, 115, 92, 235, 146, 231, 135, 79, 204, 161, 250
            ]),
            reference.digest
        );
        let expected = HashedValue::from(value);
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
        let value = Arc::new(Value::new(
            ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
            vec![TypedReference::new(
                TypeId(0),
                Reference::new(referenced_digest),
            )],
        ));
        let reference = storage
            .store_value(&HashedValue::from(value.clone()))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::new(&[
                152, 182, 130, 212, 237, 124, 174, 45, 113, 181, 43, 5, 72, 243, 126, 181, 225, 36,
                48, 119, 180, 191, 92, 196, 61, 215, 192, 223, 229, 14, 244, 98, 164, 29, 13, 112,
                236, 65, 171, 221, 49, 239, 74, 43, 206, 121, 210, 155, 155, 175, 238, 69, 255,
                222, 33, 84, 166, 21, 144, 147, 44, 156, 146, 215
            ]),
            reference.digest
        );
        let expected = HashedValue::from(value);
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
            .map(|digest| TypedReference::new(TypeId(0), Reference::new(digest)))
            .collect();
        let value = Arc::new(Value::new(
            ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
            referenced_digests,
        ));
        let reference = storage
            .store_value(&HashedValue::from(value.clone()))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::parse_hex_string("7a94d90a60e67e6f1eaa209b308250e7260824a0e1b44f28afbdec93ba48ce674ebc68535a375b63589e99c1e1333a99402f039be481163501b3ff21d6d5f095").unwrap(),
            reference.digest
        );
        let expected = HashedValue::from(value);
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
            .map(|digest| TypedReference::new(TypeId(0), Reference::new(digest)))
            .collect();
        let value = Arc::new(Value::new(
            ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
            referenced_digests,
        ));
        let reference = storage
            .store_value(&HashedValue::from(value.clone()))
            .await
            .unwrap();
        assert_eq!(
            BlobDigest::parse_hex_string("28ce0d016af6bdd104fe0f1fbc5c7a8802d3c2d4b50fee71dd3041b69ae9766dbaea94ef1e82666deece16748e1e3ad720e9b260e2a82a9836a4c05336eec93c").unwrap(),
            reference.digest
        );
        let expected = HashedValue::from(value);
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
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .await
            .unwrap();
        let reference_2 = storage
            .store_value(&HashedValue::from(Arc::new(Value::new(
                ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
                vec![],
            ))))
            .await
            .unwrap();
        let name = "test";
        assert_eq!(None, storage.load_root(name).await);
        storage.update_root(name, &reference_1.digest).await;
        assert_eq!(Some(reference_1.digest), storage.load_root(name).await);
        storage.update_root(name, &reference_2.digest).await;
        assert_eq!(Some(reference_2.digest), storage.load_root(name).await);

        storage.commit_changes().await.unwrap();
        assert_eq!(Some(reference_2.digest), storage.load_root(name).await);
    }

    #[test_log::test(tokio::test)]
    async fn test_roots_may_be_equal() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        let reference_1 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .await
            .unwrap();
        let name_1 = "testA";
        let name_2 = "testB";
        assert_eq!(None, storage.load_root(name_1).await);
        storage.update_root(name_1, &reference_1.digest).await;
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1).await);
        storage.update_root(name_2, &reference_1.digest).await;
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1).await);

        storage.commit_changes().await.unwrap();
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1).await);
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1).await);
    }
}
