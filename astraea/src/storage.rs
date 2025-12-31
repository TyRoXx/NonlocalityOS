use crate::tree::{
    BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TreeSerializationError,
    TREE_BLOB_MAX_LENGTH,
};
use async_trait::async_trait;
use cached::Cached;
use pretty_assertions::assert_eq;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    NoSpace,
    Rusqlite(String),
    TreeSerializationError(TreeSerializationError),
    Unrepresentable,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for StoreError {}

#[async_trait::async_trait]
pub trait StoreTree {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError>;
}

#[derive(Debug, Clone)]
enum DelayedHashedTreeAlternatives {
    Delayed(Arc<Tree>, BlobDigest),
    Immediate(HashedTree),
}

#[derive(Debug, Clone)]
pub struct DelayedHashedTree {
    alternatives: DelayedHashedTreeAlternatives,
}

impl DelayedHashedTree {
    pub fn delayed(tree: Arc<Tree>, expected_digest: BlobDigest) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Delayed(tree, expected_digest),
        }
    }

    pub fn immediate(tree: HashedTree) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Immediate(tree),
        }
    }

    //#[instrument(skip_all)]
    pub fn hash(self) -> Option<HashedTree> {
        match self.alternatives {
            DelayedHashedTreeAlternatives::Delayed(tree, expected_digest) => {
                let hashed_tree = HashedTree::from(tree);
                if hashed_tree.digest() == &expected_digest {
                    Some(hashed_tree)
                } else {
                    None
                }
            }
            DelayedHashedTreeAlternatives::Immediate(hashed_tree) => Some(hashed_tree),
        }
    }
}

#[async_trait::async_trait]
pub trait LoadTree: std::fmt::Debug {
    async fn load_tree(&self, reference: &BlobDigest) -> Option<DelayedHashedTree>;
    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError>;
}

pub trait LoadStoreTree: LoadTree + StoreTree {}

#[async_trait]
pub trait UpdateRoot {
    async fn update_root(
        &self,
        name: &str,
        target: &BlobDigest,
    ) -> std::result::Result<(), StoreError>;
}

#[async_trait]
pub trait LoadRoot {
    async fn load_root(&self, name: &str) -> Option<BlobDigest>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GarbageCollectionStats {
    pub trees_collected: u64,
}

#[async_trait]
pub trait CollectGarbage {
    async fn collect_some_garbage(&self)
        -> std::result::Result<GarbageCollectionStats, StoreError>;
}

#[derive(Debug)]
pub struct InMemoryTreeStorage {
    reference_to_tree: Mutex<BTreeMap<BlobDigest, HashedTree>>,
}

impl InMemoryTreeStorage {
    pub fn new(reference_to_tree: Mutex<BTreeMap<BlobDigest, HashedTree>>) -> InMemoryTreeStorage {
        InMemoryTreeStorage { reference_to_tree }
    }

    pub fn empty() -> InMemoryTreeStorage {
        Self {
            reference_to_tree: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn number_of_trees(&self) -> usize {
        self.reference_to_tree.lock().await.len()
    }

    pub async fn digests(&self) -> BTreeSet<BlobDigest> {
        self.reference_to_tree
            .lock()
            .await
            .keys()
            .copied()
            .collect()
    }
}

#[async_trait]
impl StoreTree for InMemoryTreeStorage {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        let mut lock = self.reference_to_tree.lock().await;
        let reference = *tree.digest();
        lock.entry(reference).or_insert_with(|| tree.clone());
        Ok(reference)
    }
}

#[async_trait]
impl LoadTree for InMemoryTreeStorage {
    async fn load_tree(&self, reference: &BlobDigest) -> Option<DelayedHashedTree> {
        let lock = self.reference_to_tree.lock().await;
        lock.get(reference)
            .map(|found| DelayedHashedTree::immediate(found.clone()))
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        let lock = self.reference_to_tree.lock().await;
        Ok(lock.len() as u64)
    }
}

impl LoadStoreTree for InMemoryTreeStorage {}

#[derive(Debug)]
struct TransactionStats {
    writes: u64,
}

#[derive(Debug)]
struct SQLiteState {
    connection: rusqlite::Connection,
    transaction: Option<TransactionStats>,
    has_gc_new_tree_table: bool,
}

impl SQLiteState {
    fn require_transaction(&mut self, add_writes: u64) -> std::result::Result<(), rusqlite::Error> {
        match self.transaction {
            Some(ref mut stats) => {
                stats.writes += add_writes;
                Ok(())
            }
            None => {
                debug!("BEGIN TRANSACTION");
                self.connection.execute("BEGIN TRANSACTION;", ())?;
                self.transaction = Some(TransactionStats { writes: add_writes });
                Ok(())
            }
        }
    }

    fn require_gc_new_tree_table(&mut self) -> std::result::Result<(), rusqlite::Error> {
        if self.has_gc_new_tree_table {
            Ok(())
        } else {
            self.connection.execute(
                // unfortunately, we cannot have a foreign key in a temp table
                "CREATE TEMP TABLE gc_new_tree (
                    id INTEGER PRIMARY KEY NOT NULL,
                    tree_id INTEGER UNIQUE NOT NULL
                ) STRICT",
                (),
            )?;
            self.has_gc_new_tree_table = true;
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct SQLiteStorage {
    state: tokio::sync::Mutex<SQLiteState>,
}

impl SQLiteStorage {
    pub fn from(connection: rusqlite::Connection) -> rusqlite::Result<Self> {
        Self::configure_connection(&connection)?;
        Ok(Self {
            state: Mutex::new(SQLiteState {
                connection,
                transaction: None,
                has_gc_new_tree_table: false,
            }),
        })
    }

    pub fn configure_connection(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        connection.pragma_update(None, "foreign_keys", "on")?;
        // "The default suggested cache size is -2000, which means the cache size is limited to 2048000 bytes of memory."
        // https://www.sqlite.org/pragma.html#pragma_cache_size
        connection.pragma_update(None, "cache_size", "-200000")?;
        // "The WAL journaling mode uses a write-ahead log instead of a rollback journal to implement transactions. The WAL journaling mode is persistent; after being set it stays in effect across multiple database connections and after closing and reopening the database. A database in WAL journaling mode can only be accessed by SQLite version 3.7.0 (2010-07-21) or later."
        // https://www.sqlite.org/wal.html
        connection.pragma_update(None, "journal_mode", "WAL")?;
        // CREATE TEMP TABLE shall not create a file (https://sqlite.org/tempfiles.html)
        connection.pragma_update(None, "temp_store", "MEMORY")?;
        Ok(())
    }

    pub fn create_schema(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        {
            // Why are we using format! instead of an SQL parameter here?
            // Answer is the SQLite error: "parameters prohibited in CHECK constraints" (because why should anything ever work)
            let query = format!(
                "CREATE TABLE tree (
                    id INTEGER PRIMARY KEY NOT NULL,
                    digest BLOB UNIQUE NOT NULL,
                    tree_blob BLOB NOT NULL,
                    is_compressed INTEGER NOT NULL,
                    CONSTRAINT digest_length_matches_sha3_512 CHECK (LENGTH(digest) == 64),
                    CONSTRAINT tree_blob_max_length CHECK (LENGTH(tree_blob) <= {TREE_BLOB_MAX_LENGTH}),
                    CONSTRAINT is_compressed_boolean CHECK (is_compressed IN (0, 1))
                ) STRICT"
            );
            connection
                .execute(&query, ())
                .map(|size| assert_eq!(0, size))?;
        }
        connection
            .execute(
                "CREATE TABLE reference (
                    id INTEGER PRIMARY KEY NOT NULL,
                    origin INTEGER NOT NULL REFERENCES tree ON DELETE CASCADE,
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
impl StoreTree for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        let mut state_locked = self.state.lock().await;
        let reference = *tree.digest();
        let origin_digest: [u8; 64] = reference.into();
        {
            let connection_locked = &state_locked.connection;
            let mut statement = connection_locked.prepare_cached(
                "SELECT COUNT(*) FROM tree WHERE digest = ?").unwrap(/*TODO*/);
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

        state_locked.require_gc_new_tree_table().expect("TODO");

        state_locked.require_transaction(1 + tree.tree().children().references().len() as u64).unwrap(/*TODO*/);
        let connection_locked = &state_locked.connection;

        // Try to compress the blob, but only store compressed if it's beneficial
        let original_blob = tree.tree().blob().as_slice();
        let compressed = lz4_flex::compress_prepend_size(original_blob);

        let (blob_to_store, is_compressed): (&[u8], i32) = if compressed.len() < original_blob.len()
        {
            // Compression is beneficial, store compressed
            (&compressed, 1)
        } else {
            // Compression doesn't help, store uncompressed to save CPU time on loading
            (original_blob, 0)
        };

        {
            let mut statement = connection_locked.prepare_cached(
                "INSERT INTO tree (digest, tree_blob, is_compressed) VALUES (?1, ?2, ?3)").unwrap(/*TODO*/);
            let rows_inserted = statement.execute(
                (&origin_digest, blob_to_store, &is_compressed),
            ).unwrap(/*TODO*/);
            assert_eq!(1, rows_inserted);
        }

        let tree_id: i64 = {
            let mut statement = connection_locked
                .prepare_cached("SELECT id FROM tree WHERE digest = ?1")
                .expect("TODO");
            statement
                .query_row(
                    (&origin_digest,),
                    |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
                )
                .expect("TODO")
        };

        if !tree.tree().children().references().is_empty() {
            let inserted_tree_rowid = connection_locked.last_insert_rowid();
            let mut statement = connection_locked.prepare_cached(
                "INSERT INTO reference (origin, zero_based_index, target) VALUES (?1, ?2, ?3)",).unwrap(/*TODO*/);
            for (index, reference) in tree.tree().children().references().iter().enumerate() {
                let target_digest: [u8; 64] = (*reference).into();
                let rows_inserted = statement.execute(
                    (&inserted_tree_rowid, u32::try_from(index).expect("A child index won't be too large"), &target_digest),
                ).unwrap(/*TODO*/);
                assert_eq!(1, rows_inserted);
            }
        }

        let mut statement = connection_locked
            .prepare_cached("INSERT OR IGNORE INTO gc_new_tree (tree_id) VALUES (?1)")
            .expect("TODO");
        let rows_inserted = statement.execute((&tree_id,)).expect("TODO");
        assert!(rows_inserted <= 1);

        Ok(reference)
    }
}

#[async_trait]
impl LoadTree for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_tree(&self, reference: &BlobDigest) -> Option<DelayedHashedTree> {
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        let digest: [u8; 64] = (*reference).into();
        let mut statement = connection_locked.prepare_cached("SELECT id, tree_blob, is_compressed FROM tree WHERE digest = ?1").unwrap(/*TODO*/);
        let (id, tree_blob) = match statement.query_row((&digest,), |row| -> rusqlite::Result<_> {
            let id: i64 = row.get(0).unwrap(/*TODO*/);
            let tree_blob_raw: Vec<u8> = row.get(1).unwrap(/*TODO*/);
            let is_compressed: i32 = row.get(2).unwrap(/*TODO*/);

            // Decompress if needed
            let decompressed_data = match is_compressed {
                1 => match lz4_flex::decompress_size_prepended(&tree_blob_raw) {
                    Ok(data) => data,
                    Err(error) => {
                        error!("Failed to decompress tree blob: {error:?}");
                        return Err(rusqlite::Error::InvalidQuery);
                    }
                },
                0 => tree_blob_raw,
                _ => {
                    error!("Invalid is_compressed value: {is_compressed}, expected 0 or 1");
                    return Err(rusqlite::Error::InvalidQuery);
                }
            };

            let tree_blob = TreeBlob::try_from(decompressed_data.into()).unwrap(/*TODO*/);
            Ok((id, tree_blob))
        }) {
            Ok(tuple) => tuple,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                error!("No tree found for digest {reference} in the database.");
                return None;
            }
            Err(error) => {
                error!("Error loading tree from the database: {error:?}");
                return None;
            }
        };
        let mut statement = connection_locked.prepare_cached(concat!("SELECT zero_based_index, target FROM reference",
            " WHERE origin = ? ORDER BY zero_based_index ASC")).unwrap(/*TODO*/);
        let results = statement.query_map([&id], |row| {
            let index: i64 = row.get(0)?;
            let target: [u8; 64] = row.get(1)?;
            Ok((index, BlobDigest::new(&target)))
            }).unwrap(/*TODO*/);
        let references: Vec<crate::tree::BlobDigest> = results
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
        let children = TreeChildren::try_from(references)?;
        Some(DelayedHashedTree::delayed(
            Arc::new(Tree::new(tree_blob, children)),
            *reference,
        ))
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        match connection_locked
            .query_row_and_then(
                "SELECT COUNT(*) FROM tree",
                (),
                |row| -> rusqlite::Result<_> {
                    let count: i64 = row.get(0).unwrap(/*TODO*/);
                    Ok(count)
                },
            )
            .map_err(|error| StoreError::Rusqlite(format!("{:?}", &error)))
        {
            Ok(count) => Ok(u64::try_from(count).expect("Tree count won't be negative")),
            Err(err) => Err(err),
        }
    }
}

impl LoadStoreTree for SQLiteStorage {}

#[async_trait]
impl UpdateRoot for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn update_root(
        &self,
        name: &str,
        target: &BlobDigest,
    ) -> std::result::Result<(), StoreError> {
        info!("Update root {} to {}", name, target);
        let mut state_locked = self.state.lock().await;
        state_locked
            .require_transaction(1)
            .map_err(|err| StoreError::Rusqlite(format!("{:?}", &err)))?;
        let connection_locked = &state_locked.connection;
        let target_array: [u8; 64] = (*target).into();
        connection_locked.execute(
            "INSERT INTO root (name, target) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET target = ?2;",
            (&name, &target_array),
        )
        .map_err(|err| StoreError::Rusqlite(format!("{:?}", &err)))?;
        Ok(())
    }
}

#[instrument(skip_all)]
fn collect_garbage(connection: &rusqlite::Connection) -> rusqlite::Result<GarbageCollectionStats> {
    let deleted_trees = connection.execute(
        "DELETE FROM tree
        WHERE NOT EXISTS (
            SELECT 1 FROM reference
            WHERE reference.target = tree.digest
        )
        AND NOT EXISTS (
            SELECT 1 FROM gc_new_tree
            WHERE gc_new_tree.tree_id = tree.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM root
            WHERE root.target = tree.digest
        );",
        (),
    )?;
    let deleted_new_trees = connection.execute("DELETE FROM gc_new_tree;", ())?;
    debug!(
        "Garbage collection deleted {} unreferenced trees (using {} new tree entries)",
        deleted_trees, deleted_new_trees
    );
    Ok(GarbageCollectionStats {
        trees_collected: deleted_trees as u64,
    })
}

#[async_trait]
impl CollectGarbage for SQLiteStorage {
    async fn collect_some_garbage(
        &self,
    ) -> std::result::Result<GarbageCollectionStats, StoreError> {
        let mut state_locked = self.state.lock().await;
        match state_locked.require_gc_new_tree_table() {
            Ok(()) => {}
            Err(err) => {
                error!("Failed to require gc_new_tree table: {}", err);
                return Err(StoreError::Rusqlite(format!("{:?}", &err)));
            }
        }
        let connection_locked = &state_locked.connection;
        match collect_garbage(connection_locked) {
            Ok(stats) => {
                state_locked
                    .require_transaction(stats.trees_collected)
                    .expect("TODO");
                Ok(stats)
            }
            Err(err) => {
                error!("Failed to collect garbage: {}", err);
                Err(StoreError::Rusqlite(format!("{:?}", &err)))
            }
        }
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
    #[instrument(skip_all)]
    async fn commit_changes(&self) -> Result<(), rusqlite::Error> {
        let mut state_locked = self.state.lock().await;
        match state_locked.transaction {
            Some(ref stats) => {
                info!("COMMITting transaction with {} writes", stats.writes);
                state_locked.connection.execute("COMMIT;", ())?;
                state_locked.transaction = None;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[derive(Debug)]
pub struct LoadCache {
    next: Arc<dyn LoadStoreTree + Send + Sync>,
    entries: Mutex<cached::stores::SizedCache<BlobDigest, HashedTree>>,
}

impl LoadCache {
    pub fn new(next: Arc<dyn LoadStoreTree + Send + Sync>, max_entries: usize) -> Self {
        Self {
            next,
            entries: Mutex::new(cached::stores::SizedCache::with_size(max_entries)),
        }
    }
}

#[async_trait]
impl LoadTree for LoadCache {
    async fn load_tree(&self, reference: &BlobDigest) -> Option<DelayedHashedTree> {
        {
            let mut entries_locked = self.entries.lock().await;
            if let Some(found) = entries_locked.cache_get(reference) {
                return Some(DelayedHashedTree::immediate(found.clone()));
            }
        }
        let loaded = match self.next.load_tree(reference).await {
            Some(loaded) => loaded,
            None => return None,
        };
        let maybe_hashed_tree = loaded.hash();
        match maybe_hashed_tree {
            Some(success) => {
                let mut entries_locked = self.entries.lock().await;
                entries_locked.cache_set(*reference, success.clone());
                Some(DelayedHashedTree::immediate(success))
            }
            None => None,
        }
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        self.next.approximate_tree_count().await
    }
}

#[async_trait]
impl StoreTree for LoadCache {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        self.next.store_tree(tree).await
    }
}

impl LoadStoreTree for LoadCache {}
