use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{
        CollectGarbage, CommitChanges, GarbageCollectionStats, LoadError, LoadRoot, LoadStoreTree,
        LoadTree, StoreError, StoreTree, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};
use async_trait::async_trait;
use pretty_assertions::assert_eq;
use rusqlite::OptionalExtension;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

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
            let mut statement = connection_locked
                .prepare_cached("SELECT COUNT(*) FROM tree WHERE digest = ?")
                .map_err(|error| StoreError::Rusqlite(format!("{}", &error)))?;
            let existing_count: i64 = statement
                .query_row(
                    (&origin_digest,),
                    |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
                )
                .map_err(|error| StoreError::Rusqlite(format!("{}", &error)))?;
            match existing_count {
                0 => {}
                1 => return Ok(reference),
                _ => panic!(),
            }
        }

        state_locked
            .require_gc_new_tree_table()
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;

        state_locked
            .require_transaction(1 + tree.tree().children().references().len() as u64)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;

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
            let mut statement = connection_locked
                .prepare_cached(
                    "INSERT INTO tree (digest, tree_blob, is_compressed) VALUES (?1, ?2, ?3)",
                )
                .map_err(|error| StoreError::Rusqlite(format!("{}", &error)))?;
            let rows_inserted = statement
                .execute((&origin_digest, blob_to_store, &is_compressed))
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
            assert_eq!(1, rows_inserted);
        }

        let tree_id: i64 = {
            let mut statement = connection_locked
                .prepare_cached("SELECT id FROM tree WHERE digest = ?1")
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
            statement
                .query_row(
                    (&origin_digest,),
                    |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
                )
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?
        };

        if !tree.tree().children().references().is_empty() {
            let inserted_tree_rowid = connection_locked.last_insert_rowid();
            let mut statement = connection_locked
                .prepare_cached(
                    "INSERT INTO reference (origin, zero_based_index, target) VALUES (?1, ?2, ?3)",
                )
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
            for (index, reference) in tree.tree().children().references().iter().enumerate() {
                let target_digest: [u8; 64] = (*reference).into();
                let rows_inserted = statement
                    .execute((
                        &inserted_tree_rowid,
                        u32::try_from(index).expect("A child index won't be too large"),
                        &target_digest,
                    ))
                    .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
                assert_eq!(1, rows_inserted);
            }
        }

        let mut statement = connection_locked
            .prepare_cached("INSERT OR IGNORE INTO gc_new_tree (tree_id) VALUES (?1)")
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        let rows_inserted = statement
            .execute((&tree_id,))
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        assert!(rows_inserted <= 1);

        Ok(reference)
    }
}

#[async_trait]
impl LoadTree for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<DelayedHashedTree, LoadError> {
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        let digest: [u8; 64] = (*reference).into();
        let mut statement = connection_locked
            .prepare_cached("SELECT id, tree_blob, is_compressed FROM tree WHERE digest = ?1")
            .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let (id, decompressed_data) =
            match statement.query_row((&digest,), |row| -> rusqlite::Result<_> {
                let id: i64 = row.get(0)?;
                let tree_blob_raw: Vec<u8> = row.get(1)?;
                let is_compressed: i32 = row.get(2)?;
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
                Ok((id, decompressed_data))
            }) {
                Ok(tuple) => tuple,
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    error!("No tree found for digest {reference} in the database.");
                    return Err(LoadError::TreeNotFound(*reference));
                }
                Err(error) => {
                    error!("Error loading tree from the database: {error:?}");
                    return Err(LoadError::Rusqlite(format!("{}", &error)));
                }
            };
        let tree_blob = TreeBlob::try_from(decompressed_data.into())
            .map_err(|error| LoadError::Deserialization(*reference, error))?;
        let mut statement = connection_locked
            .prepare_cached(concat!(
                "SELECT zero_based_index, target FROM reference",
                " WHERE origin = ? ORDER BY zero_based_index ASC"
            ))
            .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let results = statement
            .query_map([&id], |row| {
                let index: i64 = row.get(0)?;
                let target: [u8; 64] = row.get(1)?;
                Ok((index, BlobDigest::new(&target)))
            })
            .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let references: Vec<crate::tree::BlobDigest> = results
            .enumerate()
            .map(|(expected_index, maybe_tuple)| {
                let tuple =
                    maybe_tuple.map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
                let target = tuple.1;
                let actual_index = tuple.0;
                if expected_index as i64 != actual_index {
                    return Err(LoadError::Inconsistency(
                        *reference,
                        format!(
                            "Expected index {}, but got {}",
                            expected_index, actual_index
                        ),
                    ));
                }
                Ok(target)
            })
            .try_collect()?;
        let child_count = references.len();
        let children = match TreeChildren::try_from(references) {
            Some(children) => children,
            None => {
                let message = format!("Tree has too many children: {}", child_count);
                error!("{}", message);
                return Err(LoadError::Inconsistency(*reference, message));
            }
        };
        Ok(DelayedHashedTree::delayed(
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
                    let count: i64 = row.get(0)?;
                    Ok(count)
                },
            )
            .map_err(|error| StoreError::Rusqlite(format!("{}", &error)))
        {
            Ok(count) => Ok(u64::try_from(count).expect("COUNT(*) won't be negative")),
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
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        let connection_locked = &state_locked.connection;
        let target_array: [u8; 64] = (*target).into();
        connection_locked.execute(
            "INSERT INTO root (name, target) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET target = ?2;",
            (&name, &target_array),
        )
        .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
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
        state_locked
            .require_gc_new_tree_table()
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        let connection_locked = &state_locked.connection;
        // TODO: rework the transaction handling here
        let stats = collect_garbage(connection_locked)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        state_locked
            .require_transaction(stats.trees_collected)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        Ok(stats)
    }
}

#[async_trait]
impl LoadRoot for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_root(&self, name: &str) -> std::result::Result<Option<BlobDigest>, LoadError> {
        let state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        let target: Option<BlobDigest> = connection_locked
            .query_row(
                "SELECT target FROM root WHERE name = ?1",
                (&name,),
                |row| -> rusqlite::Result<_> {
                    let target = row.get(0)?;
                    Ok(BlobDigest::new(&target))
                },
            )
            .optional()
            .map_err(|err| LoadError::Rusqlite(format!("{}", &err)))?;
        Ok(target)
    }
}

#[async_trait]
impl CommitChanges for SQLiteStorage {
    #[instrument(skip_all)]
    async fn commit_changes(&self) -> Result<(), StoreError> {
        let mut state_locked = self.state.lock().await;
        match state_locked.transaction {
            Some(ref stats) => {
                info!("COMMITting transaction with {} writes", stats.writes);
                state_locked
                    .connection
                    .execute("COMMIT;", ())
                    .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
                state_locked.transaction = None;
                Ok(())
            }
            None => Ok(()),
        }
    }
}
