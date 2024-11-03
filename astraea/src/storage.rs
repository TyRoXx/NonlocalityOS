use crate::tree::{
    calculate_reference, BlobDigest, Reference, TypeId, TypedReference, Value, ValueBlob,
    VALUE_BLOB_MAX_LENGTH,
};
use rusqlite::Transaction;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tracing::{debug, info};

#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    NoSpace,
    Rusqlite(String),
}

pub trait StoreValue {
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError>;
}

pub trait LoadValue {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>>;
}

pub trait LoadStoreValue: LoadValue + StoreValue {}

impl std::fmt::Debug for dyn LoadStoreValue + Send + Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoadStoreValue")
    }
}

pub trait UpdateRoot {
    fn update_root(&self, name: &str, target: &BlobDigest);
}

pub trait LoadRoot {
    fn load_root(&self, name: &str) -> Option<BlobDigest>;
}

pub struct InMemoryValueStorage {
    reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>,
}

impl InMemoryValueStorage {
    pub fn new(reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>) -> InMemoryValueStorage {
        InMemoryValueStorage { reference_to_value }
    }

    pub fn empty() -> InMemoryValueStorage {
        Self {
            reference_to_value: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.reference_to_value.lock().unwrap().len()
    }
}

impl StoreValue for InMemoryValueStorage {
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError> {
        let mut lock = self.reference_to_value.lock().unwrap();
        let reference = calculate_reference(&value);
        if !lock.contains_key(&reference) {
            lock.insert(reference.clone(), value);
        }
        Ok(reference)
    }
}

impl LoadValue for InMemoryValueStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        let lock = self.reference_to_value.lock().unwrap();
        lock.get(reference).cloned()
    }
}

impl LoadStoreValue for InMemoryValueStorage {}

pub struct SQLiteStorage {
    connection: Mutex<rusqlite::Connection>,
}

impl SQLiteStorage {
    pub fn new(connection: Mutex<rusqlite::Connection>) -> Self {
        Self { connection }
    }

    pub fn create_schema(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        connection.pragma_update(None, "foreign_keys", "on")?;
        {
            // Why are we using format! instead of an SQL parameter here?
            // Answer is the SQLite error: "parameters prohibited in CHECK constraints" (because why should anything ever work)
            let query = format!(
                "CREATE TABLE value (
                id INTEGER PRIMARY KEY NOT NULL,
                digest BLOB UNIQUE NOT NULL,
                value_blob BLOB NOT NULL,
                CONSTRAINT digest_length_matches_sha3_512 CHECK (LENGTH(digest) == 64),
                CONSTRAINT value_blob_max_length CHECK (LENGTH(value_blob) <= {})
            ) STRICT",
                VALUE_BLOB_MAX_LENGTH
            );
            connection
                .execute(&query, ())
                .map(|size| assert_eq!(0, size))?;
        }
        connection
            .execute(
                "CREATE TABLE reference (
                id INTEGER PRIMARY KEY NOT NULL,
                origin INTEGER NOT NULL REFERENCES value,
                zero_based_index INTEGER NOT NULL,
                target BLOB NOT NULL,
                UNIQUE (origin, zero_based_index),
                CONSTRAINT digest_length_matches_sha3_512 CHECK (LENGTH(target) == 64)
            ) STRICT",
                (),
            )
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute("CREATE INDEX reference_origin ON reference (origin)", ())
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute("CREATE INDEX reference_target ON reference (target)", ())
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute(
                "CREATE TABLE root (
                id INTEGER PRIMARY KEY NOT NULL,
                name TEXT UNIQUE NOT NULL,
                target BLOB NOT NULL,
                CONSTRAINT target_length_matches_sha3_512 CHECK (LENGTH(target) == 64)
            ) STRICT",
                (),
            )
            .map(|size| assert_eq!(0, size))?;
        Ok(())
    }
}

impl StoreValue for SQLiteStorage {
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError> {
        let connection_locked = self.connection.lock().unwrap();
        let reference = calculate_reference(&value);
        debug!(
            "Store {} bytes as {}",
            value.blob().content.len(),
            &reference.digest,
        );
        let origin_digest: [u8; 64] = reference.digest.into();
        let transaction = Transaction::new_unchecked(&connection_locked, rusqlite::TransactionBehavior::Deferred).unwrap(/*TODO*/);
        let existing_count: i64 = connection_locked
            .query_row_and_then(
                "SELECT COUNT(*) FROM value WHERE digest = ?",
                (&origin_digest,),
                |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
            )
            .map_err(|error| StoreError::Rusqlite(format!("{:?}", &error)))?;
        match existing_count {
            0 => {}
            1 => return Ok(reference),
            _ => panic!(),
        }
        connection_locked.execute(
            "INSERT INTO value (digest, value_blob) VALUES (?1, ?2)",
            (&origin_digest, value.blob().as_slice()),
        ).unwrap(/*TODO*/);
        let inserted_value_rowid = connection_locked.last_insert_rowid();
        for (index, reference) in value.references().iter().enumerate() {
            let target_digest: [u8; 64] = reference.reference.digest.into();
            connection_locked.execute(
                "INSERT INTO reference (origin, zero_based_index, target) VALUES (?1, ?2, ?3)",
                (&inserted_value_rowid, &index, &target_digest),
            ).unwrap(/*TODO*/);
        }
        transaction.commit().unwrap(/*TODO*/);
        Ok(reference)
    }
}

impl LoadValue for SQLiteStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        let connection_locked = self.connection.lock().unwrap();
        let digest: [u8; 64] = reference.digest.into();
        let (id, value_blob) = connection_locked.query_row_and_then("SELECT id, value_blob FROM value WHERE digest = ?1", 
        (&digest, ),
         |row| -> rusqlite::Result<_> {
            let id : i64 = row.get(0).unwrap(/*TODO*/);
            let value_blob_raw : Vec<u8>= row.get(1).unwrap(/*TODO*/);
            let value_blob = ValueBlob::try_from(value_blob_raw.into()).unwrap(/*TODO*/);
            Ok((id, value_blob))
         } ).unwrap(/*TODO*/);
        let mut statement = connection_locked.prepare("SELECT zero_based_index, target FROM reference WHERE origin = ? ORDER BY zero_based_index ASC").unwrap(/*TODO*/);
        let results = statement.query_map([&id], |row| {
            let index : i64 = row.get(0)?;
            let target : [u8; 64] = row.get(1)?;
            Ok((index, TypedReference::new(TypeId(0), Reference::new(BlobDigest::new(&target)))))
        }).unwrap(/*TODO*/);
        let references: Vec<crate::tree::TypedReference> = results
            .enumerate()
            .map(|(expected_index, maybe_tuple)| {
                let tuple = maybe_tuple.unwrap(/*YOLO*/);
                let reference = tuple.1;
                let actual_index = tuple.0;
                // TODO: handle mismatch properly
                assert_eq!(expected_index as i64, actual_index);
                reference
            })
            .collect();
        debug!(
            "Load {} bytes as {}",
            value_blob.content.len(),
            &reference.digest,
        );
        Some(Arc::new(Value::new(value_blob, references)))
    }
}

impl LoadStoreValue for SQLiteStorage {}

impl UpdateRoot for SQLiteStorage {
    fn update_root(&self, name: &str, target: &BlobDigest) {
        info!("Update root {} to {}", name, target);
        let connection_locked = self.connection.lock().unwrap();
        let target_array: [u8; 64] = (*target).into();
        connection_locked.execute(
            "INSERT INTO root (name, target) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET target = ?2;",
            (&name, &target_array),
        ).unwrap(/*TODO*/);
    }
}

impl LoadRoot for SQLiteStorage {
    fn load_root(&self, name: &str) -> Option<BlobDigest> {
        use rusqlite::OptionalExtension;
        let connection_locked = self.connection.lock().unwrap();
        let maybe_target: Option<[u8; 64]> = connection_locked.query_row("SELECT target FROM root WHERE name = ?1", 
        (&name, ),
         |row| -> rusqlite::Result<_> {
            let target = row.get(0).unwrap(/*TODO*/);
            Ok(target)
         } ).optional().unwrap(/*TODO*/);
        let result = maybe_target.map(|target| BlobDigest::new(&target));
        match &result {
            Some(found) => info!("Loaded root {} as {}", name, found),
            None => info!("Could not find root {}", name),
        }
        result
    }
}
