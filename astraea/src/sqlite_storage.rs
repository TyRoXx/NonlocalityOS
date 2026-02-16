use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{
        CollectGarbage, CommitChanges, GarbageCollectionStats, LoadError, LoadRoot, LoadStoreTree,
        LoadTree, StoreError, StoreTree, StrongDelayedHashedTree, StrongReference,
        StrongReferenceTrait, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};
use async_trait::async_trait;
use pretty_assertions::assert_eq;
use rusqlite::OptionalExtension;
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

#[derive(Debug)]
struct TransactionStats {
    writes: u64,
}

#[derive(Debug)]
struct SQLiteStrongReferenceImpl {}

impl StrongReferenceTrait for SQLiteStrongReferenceImpl {}

#[derive(Debug)]
struct GarbageCollector {
    additional_roots: BTreeMap<BlobDigest, (i64, Weak<SQLiteStrongReferenceImpl>)>,
    last_gc_additional_roots_len: usize,
    has_gc_new_tree_table: bool,
}

impl GarbageCollector {
    fn new() -> Self {
        Self {
            additional_roots: BTreeMap::new(),
            last_gc_additional_roots_len: 0,
            has_gc_new_tree_table: false,
        }
    }

    fn require_additional_root(
        &mut self,
        root: &BlobDigest,
        root_tree_id: i64,
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<StrongReference> {
        let result = self.require_additional_root_entry(root, root_tree_id)?;
        self.check_automatic_collection(connection)?;
        Ok(result)
    }

    fn require_additional_root_entry(
        &mut self,
        root: &BlobDigest,
        root_tree_id: i64,
    ) -> rusqlite::Result<StrongReference> {
        match self.additional_roots.entry(*root) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let reference_counter = Arc::new(SQLiteStrongReferenceImpl {});
                vacant_entry.insert((root_tree_id, Arc::downgrade(&reference_counter)));
                Ok(StrongReference::new(Some(reference_counter), *root))
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => {
                match occupied_entry.get().1.upgrade() {
                    Some(reference_counter) => {
                        let existing_tree_id = occupied_entry.get().0;
                        if existing_tree_id != root_tree_id {
                            unreachable!("Inconsistency detected: The same root digest {} is associated with multiple tree IDs: existing tree ID {}, new tree ID {}", root, existing_tree_id, root_tree_id);
                        }
                        Ok(StrongReference::new(Some(reference_counter), *root))
                    }
                    None => {
                        let reference_counter = Arc::new(SQLiteStrongReferenceImpl {});
                        occupied_entry.insert((root_tree_id, Arc::downgrade(&reference_counter)));
                        Ok(StrongReference::new(Some(reference_counter), *root))
                    }
                }
            }
        }
    }

    fn check_automatic_collection(
        &mut self,
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<()> {
        let additional_roots_len = self.additional_roots.len();
        // Not sure what's a good minimum here.
        let minimum_additional_roots_len_for_gc = 100;
        if (additional_roots_len >= minimum_additional_roots_len_for_gc)
            && (additional_roots_len > self.last_gc_additional_roots_len * 2)
        {
            info!("Automatic garbage collection triggered because the additional root count {} exceeded a threshold", additional_roots_len);
            let stats = self.collect_garbage(connection)?;
            info!(
                "Automatic garbage collection collected {} trees",
                stats.trees_collected
            );
            self.last_gc_additional_roots_len = self.additional_roots.len();
        }
        Ok(())
    }

    fn require_gc_new_tree_table(
        &mut self,
        connection: &rusqlite::Connection,
    ) -> std::result::Result<(), rusqlite::Error> {
        if self.has_gc_new_tree_table {
            Ok(())
        } else {
            connection.execute(
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

    #[instrument(skip_all)]
    fn collect_garbage(
        &mut self,
        connection: &rusqlite::Connection,
    ) -> rusqlite::Result<GarbageCollectionStats> {
        self.require_gc_new_tree_table(connection)?;
        connection.execute("DELETE FROM gc_new_tree", ())?;
        {
            let mut statement = connection
                .prepare_cached("INSERT OR IGNORE INTO gc_new_tree (tree_id) VALUES (?1)")?;
            let mut sql_error: Option<rusqlite::Error> = None;
            self.additional_roots
                .retain(|_, (tree_id, reference_counter)| {
                    if reference_counter.upgrade().is_none() {
                        // All StrongReferences have been dropped, so we can remove this additional root
                        // and not consider the tree it pointed to as a root for GC purposes anymore
                        return false;
                    }
                    if let Err(err) = statement.execute((*tree_id,)) {
                        sql_error = Some(err);
                    }
                    true
                });
            if let Some(err) = sql_error {
                return Err(err);
            }
        }
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
        debug!(
            "Garbage collection deleted {} unreferenced trees",
            deleted_trees
        );
        self.last_gc_additional_roots_len = self.additional_roots.len();
        Ok(GarbageCollectionStats {
            trees_collected: deleted_trees as u64,
        })
    }
}

#[derive(Debug)]
struct SQLiteState {
    connection: rusqlite::Connection,
    transaction: Option<TransactionStats>,
    garbage_collector: GarbageCollector,
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
                garbage_collector: GarbageCollector::new(),
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
    async fn store_tree(
        &self,
        tree: &HashedTree,
    ) -> std::result::Result<StrongReference, StoreError> {
        let mut state_locked = self.state.lock().await;
        let digest = *tree.digest();
        let origin_digest: [u8; 64] = digest.into();
        {
            let tree_id: Option<i64> = {
                let connection_locked = &state_locked.connection;
                let mut statement = connection_locked
                    .prepare_cached("SELECT id FROM tree WHERE digest = ?")
                    .map_err(|error| StoreError::Rusqlite(format!("{}", &error)))?;
                match statement.query_row(
                    (&origin_digest,),
                    |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
                ) {
                    Ok(id) => Some(id),
                    Err(rusqlite::Error::QueryReturnedNoRows) => None,
                    Err(error) => {
                        return Err(StoreError::Rusqlite(format!("{}", &error)));
                    }
                }
            };
            if let Some(id) = tree_id {
                let (connection_locked, garbage_collector) = {
                    let state = &mut *state_locked;
                    (&state.connection, &mut state.garbage_collector)
                };
                return garbage_collector
                    .require_additional_root(tree.digest(), id, connection_locked)
                    .map_err(|error| StoreError::Rusqlite(error.to_string()));
            }
        }

        state_locked
            .require_transaction(1 + tree.tree().children().references().len() as u64)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;

        let connection_locked = &mut state_locked.connection;

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

        let tree_id: i64 = {
            // The SAVEPOINT ensures that the trees and references stay consistent even if something fails here.
            let save_point = connection_locked
                .savepoint()
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;

            {
                let mut statement = save_point
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
                let mut statement = save_point
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
                let inserted_tree_rowid = save_point.last_insert_rowid();
                let mut statement = save_point
                    .prepare_cached(
                        "INSERT INTO reference (origin, zero_based_index, target) SELECT ?1, ?2, ?3 FROM tree WHERE tree.digest = ?3",
                    )
                    .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
                for (index, reference) in tree.tree().children().references().iter().enumerate() {
                    let target_digest: [u8; 64] = (*reference.digest()).into();
                    let rows_inserted = statement
                        .execute((
                            &inserted_tree_rowid,
                            u32::try_from(index).expect("A child index won't be too large"),
                            &target_digest,
                        ))
                        .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
                    match rows_inserted {
                        0 => {
                            return Err(StoreError::TreeMissing(LoadError::TreeNotFound(
                                *reference.digest(),
                            )))
                        }
                        1 => {}
                        _ =>
                            return Err(StoreError::CorruptedStorage(
                                "Multiple rows inserted into reference table for a single child reference, which should be impossible due to the UNIQUE constraint".to_string())),
                    }
                }
            }

            save_point
                .commit()
                .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
            tree_id
        };

        let (connection_locked, garbage_collector) = {
            let state = &mut *state_locked;
            (&state.connection, &mut state.garbage_collector)
        };
        garbage_collector
            .require_additional_root(&digest, tree_id, connection_locked)
            .map_err(|error| StoreError::Rusqlite(error.to_string()))
    }
}

async fn load_tree_impl(
    state: &tokio::sync::Mutex<SQLiteState>,
    reference: &BlobDigest,
) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
    let mut state_locked = state.lock().await;
    let (tree_blob, child_digests, root_reference) = {
        let (connection_locked, garbage_collector) = {
            let state = &mut *state_locked;
            (&state.connection, &mut state.garbage_collector)
        };
        let digest: [u8; 64] = (*reference).into();
        let mut statement = connection_locked
            .prepare_cached("SELECT id, tree_blob, is_compressed FROM tree WHERE digest = ?1")
            .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let (tree_id, decompressed_data) =
            match statement.query_row((&digest,), |row| -> rusqlite::Result<_> {
                let id: i64 = row.get(0)?;
                let tree_blob_raw: Vec<u8> = row.get(1)?;
                let is_compressed: i32 = row.get(2)?;
                // Decompress if needed
                let decompressed_data = match is_compressed {
                    1 => match lz4_flex::decompress_size_prepended(&tree_blob_raw) {
                        Ok(data) => data,
                        Err(error) => {
                            let message =
                                format!("Failed to decompress tree blob using lz4: {error:?}");
                            return Ok(Err(LoadError::Inconsistency(*reference, message)));
                        }
                    },
                    0 => tree_blob_raw,
                    _ => {
                        let message = format!(
                            "Invalid is_compressed value: {is_compressed}, expected 0 or 1"
                        );
                        return Ok(Err(LoadError::Inconsistency(*reference, message)));
                    }
                };
                Ok(Ok((id, decompressed_data)))
            }) {
                Ok(maybe_tuple) => maybe_tuple?,
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    error!("No tree found for digest {reference} in the database.");
                    return Err(LoadError::TreeNotFound(*reference));
                }
                Err(sql_error) => {
                    error!("Error loading tree from the database: {sql_error:?}");
                    return Err(LoadError::Rusqlite(format!("{}", &sql_error)));
                }
            };
        // Keep the parent alive while we load the children to prevent it from being garbage collected in the middle of loading.
        let root_reference = garbage_collector
            .require_additional_root(reference, tree_id, connection_locked)
            .map_err(|error| LoadError::Rusqlite(error.to_string()))?;
        let tree_blob = TreeBlob::try_from(decompressed_data.into())
            .map_err(|error| LoadError::Deserialization(*reference, error))?;
        let mut statement = connection_locked
        .prepare_cached(concat!(
            "SELECT reference.zero_based_index, reference.target, tree.id FROM reference, tree",
            " WHERE reference.origin = ? AND reference.target = tree.digest ORDER BY reference.zero_based_index ASC"
        ))
        .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let child_results = statement
            .query_map([&tree_id], |row| {
                let index: i64 = row.get(0)?;
                let target: [u8; 64] = row.get(1)?;
                let child_tree_id: i64 = row.get(2)?;
                Ok((index, BlobDigest::new(&target), child_tree_id))
            })
            .map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
        let child_digests: Vec<(BlobDigest, i64)> = child_results
            .enumerate()
            .map(|(expected_index, maybe_tuple)| {
                let tuple =
                    maybe_tuple.map_err(|error| LoadError::Rusqlite(format!("{}", &error)))?;
                let target = tuple.1;
                let actual_index = tuple.0;
                let child_tree_id = tuple.2;
                if expected_index as i64 != actual_index {
                    return Err(LoadError::Inconsistency(
                        *reference,
                        format!(
                            "Expected index {}, but got {}",
                            expected_index, actual_index
                        ),
                    ));
                }
                Ok((target, child_tree_id))
            })
            .try_collect()?;
        (tree_blob, child_digests, root_reference)
    };
    let mut child_references = Vec::new();
    for (child_digest, child_tree_id) in child_digests {
        let (connection_locked, garbage_collector) = {
            let state = &mut *state_locked;
            (&state.connection, &mut state.garbage_collector)
        };
        let child_reference = garbage_collector
            .require_additional_root(&child_digest, child_tree_id, connection_locked)
            .map_err(|error| LoadError::Rusqlite(error.to_string()))?;
        child_references.push(child_reference);
    }
    let child_count = child_references.len();
    let children = match TreeChildren::try_from(child_references) {
        Some(children) => children,
        None => {
            let message = format!("Tree has too many children: {}", child_count);
            error!("{}", message);
            return Err(LoadError::Inconsistency(*reference, message));
        }
    };
    let tree = DelayedHashedTree::delayed(Arc::new(Tree::new(tree_blob, children)), *reference);
    Ok(StrongDelayedHashedTree::new(root_reference, tree))
}

#[async_trait]
impl LoadTree for SQLiteStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        load_tree_impl(&self.state, reference).await
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
        target: &StrongReference,
    ) -> std::result::Result<(), StoreError> {
        info!("Update root {} to {}", name, target);
        let mut state_locked = self.state.lock().await;
        state_locked
            .require_transaction(1)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        let connection_locked = &state_locked.connection;
        let target_array: [u8; 64] = (*target.digest()).into();
        let _tree_id = match connection_locked.query_row(
            "SELECT id FROM tree WHERE digest = ?1",
            (&target_array,),
            |row| -> rusqlite::Result<i64> { row.get(0) },
        ) {
            Ok(id) => id,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                return Err(StoreError::TreeMissing(LoadError::TreeNotFound(
                    *target.digest(),
                )))
            }
            Err(err) => return Err(StoreError::Rusqlite(format!("{}", &err))),
        };
        // TODO: use tree_id as target in the query
        connection_locked.execute(
            "INSERT INTO root (name, target) VALUES (?1, ?2) ON CONFLICT(name) DO UPDATE SET target = ?2;",
            (&name, &target_array),
        )
        .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        Ok(())
    }
}

#[async_trait]
impl CollectGarbage for SQLiteStorage {
    async fn collect_some_garbage(
        &self,
    ) -> std::result::Result<GarbageCollectionStats, StoreError> {
        let mut state_locked = self.state.lock().await;
        let state_borrowed: &mut SQLiteState = &mut state_locked;
        let stats = state_borrowed
            .garbage_collector
            .collect_garbage(&state_borrowed.connection)
            .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
        Ok(stats)
    }
}

#[async_trait]
impl LoadRoot for SQLiteStorage {
    //#[instrument(skip_all)]
    async fn load_root(
        &self,
        name: &str,
    ) -> std::result::Result<Option<StrongReference>, LoadError> {
        let mut state_locked = self.state.lock().await;
        let connection_locked = &state_locked.connection;
        let target: Option<(BlobDigest, i64)> = connection_locked
            .query_row(
                "SELECT root.target, tree.id FROM root, tree WHERE root.name = ?1 AND root.target = tree.digest",
                (&name,),
                |row| -> rusqlite::Result<_> {
                    let target = row.get(0)?;
                    let tree_id: i64 = row.get(1)?;
                    Ok((BlobDigest::new(&target), tree_id))
                },
            )
            .optional()
            .map_err(|err| LoadError::Rusqlite(format!("{}", &err)))?;
        match target {
            Some((digest, tree_id)) => {
                let (connection_locked, garbage_collector) = {
                    let state = &mut *state_locked;
                    (&state.connection, &mut state.garbage_collector)
                };
                let reference = garbage_collector
                    .require_additional_root(&digest, tree_id, connection_locked)
                    .map_err(|error| LoadError::Rusqlite(error.to_string()))?;
                Ok(Some(reference))
            }
            None => Ok(None),
        }
    }
}

#[async_trait]
impl CommitChanges for SQLiteStorage {
    #[instrument(skip_all)]
    async fn commit_changes(&self) -> Result<u64, StoreError> {
        let mut state_locked = self.state.lock().await;
        match state_locked.transaction {
            Some(ref stats) => {
                info!("COMMITting transaction with {} writes", stats.writes);
                state_locked
                    .connection
                    .execute("COMMIT;", ())
                    .map_err(|err| StoreError::Rusqlite(format!("{}", &err)))?;
                let writes = stats.writes;
                state_locked.transaction = None;
                Ok(writes)
            }
            None => Ok(0),
        }
    }
}
