#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        storage::{LoadRoot, LoadValue, SQLiteStorage, StoreValue, UpdateRoot},
        tree::{BlobDigest, HashedValue, Reference, TypeId, TypedReference, Value, ValueBlob},
    };
    use std::sync::{Arc, Mutex};

    // TODO: solve OpenSSL rebuild issues on Windows
    #[cfg(target_os = "linux")]
    #[test]
    fn test_sqlcipher_encryption() {
        use crate::storage::StoreError;
        let temporary_directory = tempfile::tempdir().unwrap();
        let database_file_name = temporary_directory.path().join("test.sqlite");
        let expected_reference = BlobDigest::new(&[
            166, 159, 115, 204, 162, 58, 154, 197, 200, 181, 103, 220, 24, 90, 117, 110, 151, 201,
            130, 22, 79, 226, 88, 89, 224, 209, 220, 193, 71, 92, 128, 166, 21, 178, 18, 58, 241,
            245, 249, 76, 17, 227, 233, 64, 44, 58, 197, 88, 245, 0, 25, 157, 149, 182, 211, 227,
            1, 117, 133, 134, 40, 29, 205, 38,
        ]);
        let correct_key = "test1234";
        let incorrect_key = "test12345";
        {
            let connection1 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection1.pragma_update(None, "key", correct_key).unwrap();
            SQLiteStorage::create_schema(&connection1).unwrap();
            let storage = SQLiteStorage::new(Mutex::new(connection1));
            let reference = storage.store_value(Arc::new(Value::from_unit())).unwrap();
            assert_eq!(expected_reference, reference.digest);
        }
        {
            let connection2 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection2
                .pragma_update(None, "key", incorrect_key)
                .unwrap();
            let storage = SQLiteStorage::new(Mutex::new(connection2));
            let result = storage.store_value(Arc::new(Value::from_unit()));
            let expected : std::result::Result<Reference, StoreError> = Err(StoreError::Rusqlite("SqliteFailure(Error { code: NotADatabase, extended_code: 26 }, Some(\"file is not a database\"))".to_string()));
            assert_eq!(&expected, &result);
        }
        {
            let connection3 = rusqlite::Connection::open(&database_file_name).unwrap();
            connection3.pragma_update(None, "key", correct_key).unwrap();
            let storage = SQLiteStorage::new(Mutex::new(connection3));
            let loaded_back = storage
                .load_value(&Reference::new(expected_reference))
                .unwrap();
            assert_eq!(Value::from_unit(), *loaded_back);
        }
    }

    #[test]
    fn test_create_schema() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
    }

    #[test]
    fn test_store_unit_first_time() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
        let reference = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
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
        let loaded_back = storage.load_value(&reference).unwrap();
        assert_eq!(Value::from_unit(), *loaded_back);
    }

    #[test]
    fn test_store_unit_again() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
        let reference_1 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
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
            .unwrap();
        assert_eq!(reference_1.digest, reference_2.digest);

        let loaded_back = storage.load_value(&reference_1).unwrap();
        assert_eq!(Value::from_unit(), *loaded_back);
    }

    #[test]
    fn test_store_blob() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
        let value = Arc::new(Value::new(
            ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
            vec![],
        ));
        let reference = storage
            .store_value(&HashedValue::from(value.clone()))
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
        let loaded_back = storage.load_value(&reference).unwrap();
        assert_eq!(*value, *loaded_back);
    }

    #[test]
    fn test_store_reference() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
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
        let loaded_back = storage.load_value(&reference).unwrap();
        assert_eq!(*value, *loaded_back);
    }

    #[test]
    fn test_update_root() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
        let reference_1 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .unwrap();
        let reference_2 = storage
            .store_value(&HashedValue::from(Arc::new(Value::new(
                ValueBlob::try_from(Bytes::from("test 123")).unwrap(),
                vec![],
            ))))
            .unwrap();
        let name = "test";
        assert_eq!(None, storage.load_root(name));
        storage.update_root(name, &reference_1.digest);
        assert_eq!(Some(reference_1.digest), storage.load_root(name));
        storage.update_root(name, &reference_2.digest);
        assert_eq!(Some(reference_2.digest), storage.load_root(name));
    }

    #[test]
    fn test_roots_may_be_equal() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::new(Mutex::new(connection));
        let reference_1 = storage
            .store_value(&HashedValue::from(Arc::new(Value::from_unit())))
            .unwrap();
        let name_1 = "testA";
        let name_2 = "testB";
        assert_eq!(None, storage.load_root(name_1));
        storage.update_root(name_1, &reference_1.digest);
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1));
        storage.update_root(name_2, &reference_1.digest);
        assert_eq!(Some(reference_1.digest), storage.load_root(name_1));
    }
}
