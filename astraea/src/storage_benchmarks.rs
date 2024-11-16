extern crate test;

#[cfg(test)]
mod tests {
    use super::test::Bencher;
    use crate::{
        storage::{LoadValue, SQLiteStorage, StoreValue},
        tree::{BlobDigest, HashedValue, Value, ValueBlob, VALUE_BLOB_MAX_LENGTH},
    };
    use std::sync::Arc;

    #[bench]
    fn sqlite_in_memory_store_value(b: &mut Bencher) {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        let stored_value = HashedValue::from(Arc::new(Value::from_unit()));
        b.iter(|| {
            let reference = storage.store_value(&stored_value).unwrap();
            assert_eq!(BlobDigest::hash(&[]), reference.digest);
            reference
        });
    }

    fn random_bytes(len: usize) -> Vec<u8> {
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut small_rng = SmallRng::seed_from_u64(123);
        (0..len).map(|_| small_rng.gen()).collect()
    }

    fn sqlite_in_memory_load_and_hash_value(b: &mut Bencher, value_count_in_database: usize) {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        for index in 0..(value_count_in_database as u64) {
            let stored_value = HashedValue::from(Arc::new(Value::new(
                ValueBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
                vec![],
            )));
            let _reference = storage.store_value(&stored_value).unwrap();
        }
        assert_eq!(
            Ok(value_count_in_database as u64),
            storage.approximate_value_count()
        );
        let stored_value = HashedValue::from(Arc::new(Value::new(
            ValueBlob::try_from(bytes::Bytes::from(random_bytes(VALUE_BLOB_MAX_LENGTH))).unwrap(),
            vec![],
        )));
        let reference = storage.store_value(&stored_value).unwrap();
        assert_eq!(BlobDigest::parse_hex_string(
            "23f3c29d5ead1d624ce6a64c730d6bb84acd6f9e6a51d411e189d396825ae4e393cdf18ddbe5a23b820c975f9efaa96d25cbfa14af369f5665fce583b44abc25").unwrap(),
            reference.digest);
        b.iter(|| {
            let loaded = storage.load_value(&reference).unwrap().hash().unwrap();
            assert_eq!(stored_value.digest(), loaded.digest());
            *loaded.digest()
        });
        assert_eq!(
            Ok(value_count_in_database as u64 + 1),
            storage.approximate_value_count()
        );
    }

    #[bench]
    fn sqlite_in_memory_load_and_hash_value_small_database(b: &mut Bencher) {
        sqlite_in_memory_load_and_hash_value(b, 0);
    }

    #[bench]
    fn sqlite_in_memory_load_and_hash_value_large_database(b: &mut Bencher) {
        sqlite_in_memory_load_and_hash_value(b, 10_000);
    }
}
