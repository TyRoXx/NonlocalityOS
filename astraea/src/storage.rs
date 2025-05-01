use crate::tree::{BlobDigest, HashedValue, Value, ValueBlob, VALUE_BLOB_MAX_LENGTH};
use async_trait::async_trait;
use cached::Cached;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, info};

#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    NoSpace,
    Rusqlite(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for StoreError {}

#[async_trait::async_trait]
pub trait StoreValue {
    async fn store_value(&self, value: &HashedValue)
        -> std::result::Result<BlobDigest, StoreError>;
}

#[derive(Debug, Clone)]
enum DelayedHashedValueAlternatives {
    Delayed(Arc<Value>, BlobDigest),
    Immediate(HashedValue),
}

#[derive(Debug, Clone)]
pub struct DelayedHashedValue {
    alternatives: DelayedHashedValueAlternatives,
}

impl DelayedHashedValue {
    pub fn delayed(value: Arc<Value>, expected_digest: BlobDigest) -> Self {
        Self {
            alternatives: DelayedHashedValueAlternatives::Delayed(value, expected_digest),
        }
    }

    pub fn immediate(value: HashedValue) -> Self {
        Self {
            alternatives: DelayedHashedValueAlternatives::Immediate(value),
        }
    }

    //#[instrument(skip_all)]
    pub fn hash(self) -> Option<HashedValue> {
        match self.alternatives {
            DelayedHashedValueAlternatives::Delayed(value, expected_digest) => {
                let hashed_value = HashedValue::from(value);
                if hashed_value.digest() == &expected_digest {
                    Some(hashed_value)
                } else {
                    None
                }
            }
            DelayedHashedValueAlternatives::Immediate(hashed_value) => Some(hashed_value),
        }
    }
}

#[async_trait::async_trait]
pub trait LoadValue: std::fmt::Debug {
    async fn load_value(&self, reference: &BlobDigest) -> Option<DelayedHashedValue>;
    async fn approximate_value_count(&self) -> std::result::Result<u64, StoreError>;
}

pub trait LoadStoreValue: LoadValue + StoreValue {}

#[async_trait]
pub trait UpdateRoot {
    async fn update_root(&self, name: &str, target: &BlobDigest);
}

#[async_trait]
pub trait LoadRoot {
    async fn load_root(&self, name: &str) -> Option<BlobDigest>;
}

#[derive(Debug)]
pub struct InMemoryValueStorage {
    reference_to_value: Mutex<BTreeMap<BlobDigest, HashedValue>>,
}

impl InMemoryValueStorage {
    pub fn new(
        reference_to_value: Mutex<BTreeMap<BlobDigest, HashedValue>>,
    ) -> InMemoryValueStorage {
        InMemoryValueStorage { reference_to_value }
    }

    pub fn empty() -> InMemoryValueStorage {
        Self {
            reference_to_value: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn len(&self) -> usize {
        self.reference_to_value.lock().await.len()
    }

    pub async fn digests(&self) -> BTreeSet<BlobDigest> {
        self.reference_to_value
            .lock()
            .await
            .keys()
            .map(|v| *v)
            .collect()
    }
}

#[async_trait]
impl StoreValue for InMemoryValueStorage {
    async fn store_value(
        &self,
        value: &HashedValue,
    ) -> std::result::Result<BlobDigest, StoreError> {
        let mut lock = self.reference_to_value.lock().await;
        let reference = *value.digest();
        if !lock.contains_key(&reference) {
            lock.insert(reference.clone(), value.clone());
        }
        Ok(reference)
    }
}

#[async_trait]
impl LoadValue for InMemoryValueStorage {
    async fn load_value(&self, reference: &BlobDigest) -> Option<DelayedHashedValue> {
        let lock = self.reference_to_value.lock().await;
        lock.get(reference)
            .map(|found| DelayedHashedValue::immediate(found.clone()))
    }

    async fn approximate_value_count(&self) -> std::result::Result<u64, StoreError> {
        let lock = self.reference_to_value.lock().await;
        Ok(lock.len() as u64)
    }
}

impl LoadStoreValue for InMemoryValueStorage {}

#[derive(Debug)]
struct SQLiteState {
    connection: rusqlite::Connection,
    is_in_transaction: bool,
}

impl SQLiteState {
    fn require_transaction(&mut self) -> std::result::Result<(), rusqlite::Error> {
        match self.is_in_transaction {
            true => Ok(()),
            false => {
                debug!("BEGIN TRANSACTION");
                self.connection.execute("BEGIN TRANSACTION;", ())?;
                self.is_in_transaction = true;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct SQLiteStorage {
    state: tokio::sync::Mutex<SQLiteState>,
}

impl SQLiteStorage {
    pub fn from(connection: rusqlite::Connection) -> rusqlite::Result<Self> {
        connection.pragma_update(None, "foreign_keys", "on")?;
        // "The default suggested cache size is -2000, which means the cache size is limited to 2048000 bytes of memory."
        // https://www.sqlite.org/pragma.html#pragma_cache_size
        connection.pragma_update(None, "cache_size", "-200000")?;
        // "The WAL journaling mode uses a write-ahead log instead of a rollback journal to implement transactions. The WAL journaling mode is persistent; after being set it stays in effect across multiple database connections and after closing and reopening the database. A database in WAL journaling mode can only be accessed by SQLite version 3.7.0 (2010-07-21) or later."
        // https://www.sqlite.org/wal.html
        connection.pragma_update(None, "journal_mode", "WAL")?;
        Ok(Self {
            state: Mutex::new(SQLiteState {
                connection,
                is_in_transaction: false,
            }),
        })
    }

    pub fn create_schema(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
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

#[async_trait]
impl StoreValue for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn store_value(
        &self,
        value: &HashedValue,
    ) -> std::result::Result<BlobDigest, StoreError> {
        let mut state_locked = self.state.lock().await;
        let reference = *value.digest();
        let origin_digest: [u8; 64] = reference.into();
        state_locked.require_transaction().unwrap(/*TODO*/);
        let connection_locked = &state_locked.connection;
        {
            let mut statement = connection_locked.prepare_cached(
                "SELECT COUNT(*) FROM value WHERE digest = ?").unwrap(/*TODO*/);
            let existing_count: i64 = statement
                .query_row(
                    (&origin_digest,),
                    |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
                )
                .map_err(|error| StoreError::Rusqlite(format!("{:?}", &error)))?;
            match existing_count {
                0 => {}
                1 => return Ok(reference),
                _ => panic!(),
            }
        }

        let mut statement = connection_locked.prepare_cached(
            "INSERT INTO value (digest, value_blob) VALUES (?1, ?2)").unwrap(/*TODO*/);
        let rows_inserted = statement.execute(
            (&origin_digest, value.value().blob().as_slice()),
        ).unwrap(/*TODO*/);
        assert_eq!(1, rows_inserted);

        if !value.value().references().is_empty() {
            let inserted_value_rowid = connection_locked.last_insert_rowid();
            let mut statement = connection_locked.prepare_cached(
                "INSERT INTO reference (origin, zero_based_index, target) VALUES (?1, ?2, ?3)",).unwrap(/*TODO*/);
            for (index, reference) in value.value().references().iter().enumerate() {
                let target_digest: [u8; 64] = (*reference).into();
                let rows_inserted = statement.execute(
                    (&inserted_value_rowid, &index, &target_digest),
                ).unwrap(/*TODO*/);
                assert_eq!(1, rows_inserted);
            }
        }

        Ok(reference)
    }
}

#[async_trait]
impl LoadValue for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_value(&self, reference: &BlobDigest) -> Option<DelayedHashedValue> {
        let references: Vec<crate::tree::BlobDigest>;
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        let digest: [u8; 64] = (*reference).into();
        let mut statement = connection_locked.prepare_cached("SELECT id, value_blob FROM value WHERE digest = ?1").unwrap(/*TODO*/);
        let (id, value_blob) = statement.query_row(
            (&digest, ),
            |row| -> rusqlite::Result<_> {
                let id : i64 = row.get(0).unwrap(/*TODO*/);
                let value_blob_raw : Vec<u8> = row.get(1).unwrap(/*TODO*/);
                let value_blob = ValueBlob::try_from(value_blob_raw.into()).unwrap(/*TODO*/);
                Ok((id, value_blob))
            } ).unwrap(/*TODO*/);
        let mut statement = connection_locked.prepare_cached(concat!("SELECT zero_based_index, target FROM reference",
            " WHERE origin = ? ORDER BY zero_based_index ASC")).unwrap(/*TODO*/);
        let results = statement.query_map([&id], |row| {
            let index: i64 = row.get(0)?;
            let target: [u8; 64] = row.get(1)?;
            Ok((index, BlobDigest::new(&target)))
            }).unwrap(/*TODO*/);
        references = results
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
        Some(DelayedHashedValue::delayed(
            Arc::new(Value::new(value_blob, references)),
            *reference,
        ))
    }

    async fn approximate_value_count(&self) -> std::result::Result<u64, StoreError> {
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        connection_locked
            .query_row_and_then(
                "SELECT COUNT(*) FROM value",
                (),
                |row| -> rusqlite::Result<_> {
                    let count: u64 = row.get(0).unwrap(/*TODO*/);
                    Ok(count)
                },
            )
            .map_err(|error| StoreError::Rusqlite(format!("{:?}", &error)))
    }
}

impl LoadStoreValue for SQLiteStorage {}

#[async_trait]
impl UpdateRoot for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn update_root(&self, name: &str, target: &BlobDigest) {
        info!("Update root {} to {}", name, target);
        let mut state_locked = self.state.lock().await;
        state_locked.require_transaction().unwrap(/*TODO*/);
        let connection_locked = &state_locked.connection;
        let target_array: [u8; 64] = (*target).into();
        connection_locked.execute(
            "INSERT INTO root (name, target) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET target = ?2;",
            (&name, &target_array),
        ).unwrap(/*TODO*/);
    }
}

#[async_trait]
impl LoadRoot for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_root(&self, name: &str) -> Option<BlobDigest> {
        use rusqlite::OptionalExtension;
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
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

#[async_trait]
pub trait CommitChanges {
    async fn commit_changes(&self) -> Result<(), rusqlite::Error>;
}

#[async_trait]
impl CommitChanges for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn commit_changes(&self) -> Result<(), rusqlite::Error> {
        let mut state_locked = self.state.lock().await;
        match state_locked.is_in_transaction {
            true => {
                state_locked.is_in_transaction = false;
                info!("COMMIT");
                state_locked.connection.execute("COMMIT;", ())?;
                Ok(())
            }
            false => Ok(()),
        }
    }
}

#[derive(Debug)]
pub struct LoadCache {
    next: Arc<(dyn LoadStoreValue + Send + Sync)>,
    entries: Mutex<cached::stores::SizedCache<BlobDigest, HashedValue>>,
}

impl LoadCache {
    pub fn new(next: Arc<(dyn LoadStoreValue + Send + Sync)>, max_entries: usize) -> Self {
        Self {
            next,
            entries: Mutex::new(cached::stores::SizedCache::with_size(max_entries)),
        }
    }
}

#[async_trait]
impl LoadValue for LoadCache {
    async fn load_value(&self, reference: &BlobDigest) -> Option<DelayedHashedValue> {
        {
            let mut entries_locked = self.entries.lock().await;
            if let Some(found) = entries_locked.cache_get(reference) {
                return Some(DelayedHashedValue::immediate(found.clone()));
            }
        }
        let loaded = match self.next.load_value(reference).await {
            Some(loaded) => loaded,
            None => return None,
        };
        let hashed_value = loaded.hash();
        match hashed_value {
            Some(success) => {
                let mut entries_locked = self.entries.lock().await;
                entries_locked.cache_set(*reference, success.clone());
                Some(DelayedHashedValue::immediate(success))
            }
            None => None,
        }
    }

    async fn approximate_value_count(&self) -> std::result::Result<u64, StoreError> {
        self.next.approximate_value_count().await
    }
}

#[async_trait]
impl StoreValue for LoadCache {
    async fn store_value(
        &self,
        value: &HashedValue,
    ) -> std::result::Result<BlobDigest, StoreError> {
        self.next.store_value(value).await
    }
}

impl LoadStoreValue for LoadCache {}
