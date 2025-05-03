extern crate test;
use crate::{
    storage::{LoadValue, SQLiteStorage, StoreValue},
    tree::{BlobDigest, HashedValue, TreeBlob, Value, VALUE_BLOB_MAX_LENGTH},
};
use std::sync::Arc;
use test::Bencher;
use tokio::runtime::Runtime;

fn sqlite_in_memory_store_value_redundantly(b: &mut Bencher, value_blob_size: usize) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let stored_value = HashedValue::from(Arc::new(Value::new(
        TreeBlob::try_from(bytes::Bytes::from(random_bytes(value_blob_size))).unwrap(),
        vec![],
    )));
    let runtime = Runtime::new().unwrap();
    b.iter(|| {
        let reference = runtime
            .block_on(storage.store_value(&stored_value))
            .unwrap();
        assert_eq!(stored_value.digest(), &reference);
        reference
    });
    b.bytes = value_blob_size as u64;
}

#[bench]
fn sqlite_in_memory_store_value_redundantly_small(b: &mut Bencher) {
    sqlite_in_memory_store_value_redundantly(b, 100);
}

#[bench]
fn sqlite_in_memory_store_value_redundantly_medium(b: &mut Bencher) {
    sqlite_in_memory_store_value_redundantly(b, VALUE_BLOB_MAX_LENGTH / 2);
}

#[bench]
fn sqlite_in_memory_store_value_redundantly_large(b: &mut Bencher) {
    sqlite_in_memory_store_value_redundantly(b, VALUE_BLOB_MAX_LENGTH);
}

fn sqlite_in_memory_store_value_newly(
    b: &mut Bencher,
    value_blob_size: usize,
    reference_count: usize,
) {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    // count reduced to save time in the tests
    let store_count = 15;
    let stored_values: Vec<_> = (0..store_count)
        .map(|_| {
            HashedValue::from(Arc::new(Value::new(
                TreeBlob::try_from(bytes::Bytes::from_iter(
                    (0..value_blob_size).map(|_| small_rng.gen()),
                ))
                .unwrap(),
                (0..reference_count)
                    .map(|_| BlobDigest::new(&small_rng.gen()))
                    .collect(),
            )))
        })
        .collect();
    let runtime = Runtime::new().unwrap();
    b.iter(|| {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        for stored_value in &stored_values {
            let reference = runtime
                .block_on(storage.store_value(&stored_value))
                .unwrap();
            assert_eq!(stored_value.digest(), &reference);
        }
        storage
    });
    b.bytes = store_count as u64 * (value_blob_size as u64 + reference_count as u64 * 64);
}

#[bench]
fn sqlite_in_memory_store_value_newly_small(b: &mut Bencher) {
    sqlite_in_memory_store_value_newly(b, 100, 0);
}

#[bench]
fn sqlite_in_memory_store_value_newly_large(b: &mut Bencher) {
    sqlite_in_memory_store_value_newly(b, VALUE_BLOB_MAX_LENGTH, 0);
}

#[bench]
fn sqlite_in_memory_store_value_newly_only_refs(b: &mut Bencher) {
    sqlite_in_memory_store_value_newly(b, 0, 100);
}

async fn generate_random_values<T: StoreValue>(value_count_in_database: u64, storage: &T) {
    for index in 0..value_count_in_database {
        let stored_value = HashedValue::from(Arc::new(Value::new(
            TreeBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
            vec![],
        )));
        let _reference = storage.store_value(&stored_value).await.unwrap();
    }
}

#[bench]
fn generate_random_values_1000(b: &mut Bencher) {
    let value_count_in_database = 1000;
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    b.iter(|| {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        runtime.block_on(generate_random_values(value_count_in_database, &storage));
        assert_eq!(
            Ok(value_count_in_database),
            runtime.block_on(storage.approximate_value_count())
        );
    });
}

fn random_bytes(len: usize) -> Vec<u8> {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    (0..len).map(|_| small_rng.gen()).collect()
}

fn sqlite_in_memory_load_and_hash_value(b: &mut Bencher, value_count_in_database: u64) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    runtime.block_on(generate_random_values(value_count_in_database, &storage));
    assert_eq!(
        Ok(value_count_in_database as u64),
        runtime.block_on(storage.approximate_value_count())
    );
    let stored_value = HashedValue::from(Arc::new(Value::new(
        TreeBlob::try_from(bytes::Bytes::from(random_bytes(VALUE_BLOB_MAX_LENGTH))).unwrap(),
        vec![],
    )));
    let reference = runtime
        .block_on(storage.store_value(&stored_value))
        .unwrap();
    assert_eq!(
        BlobDigest::parse_hex_string(concat!(
            "d15454a6735a0bb995b758a221381c539eb16e7653fb6b1b4975377187cfd4f0",
            "26495f5d6ad44b93d4738210700d88da92e876049aaffac298f9b3547479818a"
        ))
        .unwrap(),
        reference
    );
    b.iter(|| {
        let loaded = runtime
            .block_on(storage.load_value(&reference))
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(stored_value.digest(), loaded.digest());
        loaded
    });
    b.bytes = stored_value.value().blob().len() as u64;
    assert_eq!(
        Ok(value_count_in_database + 1),
        runtime.block_on(storage.approximate_value_count())
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
