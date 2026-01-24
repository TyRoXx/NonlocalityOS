use crate::{
    sqlite::{register_vfs, PagesVfs, SyncDirectoryFunction},
    CacheDropStats, FileCreationMode, NormalizedPath, OpenDirectory, OpenFileStats, TreeEditor,
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{DelayedHashedTree, LoadError, LoadStoreTree, LoadTree, StoreError, StoreTree},
    tree::{BlobDigest, HashedTree},
};
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use futures::StreamExt;
use pretty_assertions::assert_eq;
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use relative_path::RelativePath;
use sqlite_vfs::Vfs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::runtime::Handle;
use tracing::info;

static VFS_NAME_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}_{n}")
}

fn register_unique_vfs(
    editor: TreeEditor,
    runtime: Handle,
    random_number_generator: Box<dyn RngCore + Send>,
    sync_directory: SyncDirectoryFunction,
) -> String {
    // Cargo runs tests in parallel in the same process, so we can't reuse VFS names.
    let name = unique_vfs_name("test_vfs");
    info!("Allocated VFS name: {}", &name);
    register_vfs(
        &name,
        editor,
        runtime,
        random_number_generator,
        sync_directory,
    )
    .unwrap();
    // Unfortunately, sqlite_vfs doesn't provide a way to unregister VFS instances (2026-01-11).
    name
}

async fn expect_directory_entries(
    directory: &OpenDirectory,
    expectation: &BTreeMap<FileName, DirectoryEntryKind>,
) {
    let mut entries = BTreeMap::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        entries.insert(entry.name, entry.kind);
    }
    assert_eq!(expectation, &entries);
}

#[test_log::test(tokio::test)]
async fn test_vfs_delete_invalid_file_name() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        TreeEditor::new(directory.clone(), None),
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || match vfs.delete("\\") {
        Ok(_) => panic!("Expected error"),
        Err(e) => {
            assert_eq!(
                "Invalid database file path `\\`: WindowsSpecialCharacter",
                e.to_string()
            );
        }
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_delete_not_found() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        TreeEditor::new(directory.clone(), None),
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || match vfs.delete("test.db") {
        Ok(_) => panic!("Expected error"),
        Err(e) => {
            assert_eq!(
                "Failed to delete database file `test.db`: NotFound(FileName { content: FileNameContent(\"test.db\") })",
                e.to_string()
            );
        }
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_exists_invalid_file_name() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        TreeEditor::new(directory.clone(), None),
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || match vfs.exists("\\") {
        Ok(_) => panic!("Expected error"),
        Err(e) => {
            assert_eq!(
                "Invalid database file path `\\`: WindowsSpecialCharacter",
                e.to_string()
            );
        }
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_exists_cannot_open_file_as_directory() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let editor = TreeEditor::new(directory.clone(), None);
    editor
        .open_file(
            NormalizedPath::try_from(RelativePath::new("/test")).unwrap(),
            FileCreationMode::create_new(),
        )
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        editor,
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || match vfs.exists("/test/file.db") {
        Ok(_) => panic!("Expected error"),
        Err(e) => {
            assert_eq!(
                "Failed to check existence of database file `/test/file.db`: CannotOpenRegularFileAsDirectory(FileName { content: FileNameContent(\"test\") })",
                e.to_string()
            );
        }
    });
    thread.await.unwrap();
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test".to_string()).unwrap(),
            DirectoryEntryKind::File(0),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_temporary_name() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let editor = TreeEditor::new(directory.clone(), None);
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        editor,
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        assert_eq!("sqlite-temp-a5565735f810987a", &vfs.temporary_name());
        assert_eq!("sqlite-temp-d6914642e58d662e", &vfs.temporary_name());
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_random() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let editor = TreeEditor::new(directory.clone(), None);
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        editor,
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        let mut buffer = [0i8; 16];
        vfs.random(&mut buffer);
        info!("Random bytes: {:?}", &buffer);
        assert_eq!(
            &buffer,
            &[122, -104, 16, -8, 53, 87, 86, -91, 46, 102, -115, -27, 66, 70, -111, -42]
        );
        // test empty buffer as well
        vfs.random(&mut []);
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_vfs_sleep() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let editor = TreeEditor::new(directory.clone(), None);
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(
        editor,
        runtime,
        random_number_generator,
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        let duration = Duration::from_micros(1);
        let elapsed = vfs.sleep(duration);
        assert!(elapsed >= duration);
    });
    thread.await.unwrap();
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_open_database() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name.as_str(),
        )
        .unwrap();
        connection
            .execute(
                "CREATE TABLE t (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                ) STRICT",
                (),
            )
            .unwrap();
        {
            let transaction = connection.transaction().unwrap();
            for i in 0..100 {
                transaction
                    .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                    .unwrap();
            }
            transaction.commit().unwrap();
        }
        connection.close().unwrap();
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 1,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_open_invalid_file_name() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        match rusqlite::Connection::open_with_flags_and_vfs(
            "\\",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name.as_str(),
        ) {
            Ok(_) => panic!("Expected error"),
            Err(e) => {
                assert_eq!("unable to open database file: \\", e.to_string());
            }
        }
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 0,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_open_failure() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let editor = TreeEditor::new(directory.clone(), None);
    editor
        .create_directory(NormalizedPath::try_from(RelativePath::new("/dir")).unwrap())
        .await
        .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        match rusqlite::Connection::open_with_flags_and_vfs(
            "dir",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name.as_str(),
        ) {
            Ok(_) => panic!("Expected error"),
            Err(e) => {
                assert_eq!("unable to open database file: dir", e.to_string());
            }
        }
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 0,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 2);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("dir".to_string()).unwrap(),
            DirectoryEntryKind::Directory,
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_open_no_create() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        match rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            // no SQLITE_OPEN_CREATE
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            vfs_name.as_str(),
        ) {
            Ok(_) => panic!("Expected error"),
            Err(e) => {
                assert_eq!("unable to open database file: test.db", e.to_string());
            }
        }
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 0,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}

#[test_log::test(tokio::test)]
async fn test_reopen_database() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let insert_count = 100;
    {
        let vfs_name = vfs_name.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection
                .execute(
                    "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                    (),
                )
                .unwrap();
            {
                let transaction = connection.transaction().unwrap();
                for i in 0..insert_count {
                    transaction
                        .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                        .unwrap();
                }
                transaction.commit().unwrap();
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 1,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 1,
                files_open_for_writing_count: 1,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
    {
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                // test read-only access
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                vfs_name.as_str(),
            )
            .unwrap();
            {
                let mut select_statement = connection.prepare("SELECT id, name FROM t").unwrap();
                let mut rows = select_statement.query([]).unwrap();
                let mut count = 0;
                while let Some(row) = rows.next().unwrap() {
                    let id: i32 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();
                    assert_eq!(id, count + 1);
                    assert_eq!(name, format!("Name {}", count));
                    count += 1;
                }
                assert_eq!(count, insert_count);
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_sync_directory() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(
            std::path::PathBuf::from(""),
            storage.clone(),
            clock.clone(),
            1,
        )
        .await
        .unwrap(),
    );
    let handle = Handle::current();
    let last_synced_digest = Arc::new(tokio::sync::Mutex::new(None));
    let sync_directory_function = Arc::new({
        let directory = directory.clone();
        let handle = handle.clone();
        let last_synced_digest = last_synced_digest.clone();
        move || {
            let directory = directory.clone();
            handle.block_on(async {
                let status = directory.request_save().await.unwrap();
                assert_eq!(0, status.directories_unsaved_count);
                assert!(status.digest.is_digest_up_to_date);
                info!(
                    "Synced directory at digest {}",
                    &status.digest.last_known_digest
                );
                *last_synced_digest.lock().await = Some(status.digest);
            });
            Ok(())
        }
    });
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        handle.clone(),
        Box::new(SmallRng::seed_from_u64(123)),
        sync_directory_function.clone(),
    );
    let insert_count = 100;
    let digest_after_commit = {
        let handle = handle.clone();
        let last_synced_digest = last_synced_digest.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection
                .execute(
                    "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                    (),
                )
                .unwrap();
            let digest_before_insert = handle
                .block_on(async { *last_synced_digest.lock().await })
                .expect("Expected at least one directory sync");
            assert!(digest_before_insert.is_digest_up_to_date);
            {
                let transaction = connection.transaction().unwrap();
                for i in 0..insert_count {
                    transaction
                        .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                        .unwrap();
                }
                transaction.commit().unwrap();
            }
            let digest_after_insert = handle
                .block_on(async { *last_synced_digest.lock().await })
                .expect("Last synced digest cannot disappear");
            assert!(digest_after_insert.is_digest_up_to_date);
            assert_ne!(digest_before_insert, digest_after_insert);
            {
                let transaction = connection.transaction().unwrap();
                transaction.commit().unwrap();
            }
            let digest_after_redundant_commit = handle
                .block_on(async { *last_synced_digest.lock().await })
                .expect("Last synced digest cannot disappear");
            assert_eq!(digest_after_redundant_commit, digest_after_insert);
            connection.close().unwrap();
            let digest_after_close = handle
                .block_on(async { *last_synced_digest.lock().await })
                .expect("Last synced digest cannot disappear");
            assert_eq!(digest_after_close, digest_after_insert);
            digest_after_insert
        });
        thread.await.unwrap()
    };
    let final_commit_digest = {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 1,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 1,
                files_open_for_writing_count: 1,
                files_unflushed_count: 0,
            }
        );
        // We are not sure why, but we have to explicitly save again after closing the database connection.
        //SQLite does not appear to sync all data when you commit a transaction.
        assert_ne!(&directory_status.digest, &digest_after_commit);
        assert!(directory_status.digest.is_digest_up_to_date);
        directory_status.digest.last_known_digest
    };
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
    let directory = OpenDirectory::load_directory(
        PathBuf::from(""),
        storage,
        &final_commit_digest,
        clock(),
        clock,
        1,
    )
    .await
    .unwrap();
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
    let sync_directory_function = Arc::new({
        let directory = directory.clone();
        let handle = handle.clone();
        move || {
            let directory = directory.clone();
            handle.block_on(async {
                let status = directory.request_save().await.unwrap();
                info!(
                    "After loading: Syncing directory at digest {}",
                    &status.digest.last_known_digest
                );
                assert_eq!(0, status.directories_unsaved_count);
                assert!(status.digest.is_digest_up_to_date);
            });
            Ok(())
        }
    });
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        handle.clone(),
        Box::new(SmallRng::seed_from_u64(123)),
        sync_directory_function,
    );
    {
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                vfs_name.as_str(),
            )
            .unwrap();
            {
                let mut select_statement = connection.prepare("SELECT id, name FROM t").unwrap();
                let mut rows = select_statement.query([]).unwrap();
                let mut count = 0;
                while let Some(row) = rows.next().unwrap() {
                    let id: i32 = row.get(0).unwrap();
                    let name: String = row.get(1).unwrap();
                    assert_eq!(id, count + 1);
                    assert_eq!(name, format!("Name {}", count));
                    count += 1;
                }
                assert_eq!(count, insert_count);
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_temp_table() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let thread = tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name.as_str(),
        )
        .unwrap();
        connection
            .execute(
                "CREATE TEMP TABLE t (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                ) STRICT",
                (),
            )
            .unwrap();
        {
            let transaction = connection.transaction().unwrap();
            // Force temp table to spill over into a temporary file by inserting many rows.
            let insert_count = 100_000;
            for i in 0..insert_count {
                transaction
                    .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                    .unwrap();
            }
            transaction.commit().unwrap();
        }
        connection.close().unwrap();
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 1,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(0),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_open_database_with_wal() {
    // WAL is not supported by sqlite-vfs yet: https://github.com/rkusa/sqlite-vfs?tab=readme-ov-file#limitations
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    {
        let vfs_name = vfs_name.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .unwrap();
            match connection.execute(
                "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                (),
            ) {
                Ok(_) => {
                    panic!("Currently the query fails for an unknown reason");
                }
                Err(e) => {
                    assert_eq!("disk I/O error", e.to_string());
                }
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 2,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 1,
                files_open_for_writing_count: 1,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([
            (
                FileName::try_from("test.db".to_string()).unwrap(),
                DirectoryEntryKind::File(4096),
            ),
            (
                FileName::try_from("test.db-wal".to_string()).unwrap(),
                DirectoryEntryKind::File(0),
            ),
        ]),
    )
    .await;
    // reopen
    {
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([
            (
                FileName::try_from("test.db".to_string()).unwrap(),
                DirectoryEntryKind::File(4096),
            ),
            (
                FileName::try_from("test.db-wal".to_string()).unwrap(),
                DirectoryEntryKind::File(0),
            ),
        ]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_change_page_size() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let page_size = 16384;
    let thread = tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name.as_str(),
        )
        .unwrap();
        connection
            .pragma_update(None, "page_size", page_size.to_string())
            .unwrap();
        connection
            .execute(
                "CREATE TABLE t (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                ) STRICT",
                (),
            )
            .unwrap();
        {
            let transaction = connection.transaction().unwrap();
            for i in 0..100 {
                transaction
                    .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                    .unwrap();
            }
            transaction.commit().unwrap();
        }
        connection.close().unwrap();
    });
    thread.await.unwrap();
    {
        let cache_drop_stats = directory.drop_all_read_caches().await;
        assert_eq!(
            &cache_drop_stats,
            &CacheDropStats {
                files_and_directories_remaining_open: 1,
                hashed_trees_dropped: 0,
                open_directories_closed: 0,
                open_files_closed: 1,
            }
        );
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(page_size * 2),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_storage_read_error() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    let insert_count = 100;
    {
        let vfs_name = vfs_name.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection
                .execute(
                    "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                    (),
                )
                .unwrap();
            {
                let transaction = connection.transaction().unwrap();
                for i in 0..insert_count {
                    transaction
                        .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                        .unwrap();
                }
                transaction.commit().unwrap();
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    {
        // close the database file to force read from storage
        directory.drop_all_read_caches().await;
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    // make it impossible to read the file from storage
    storage.clear().await;
    {
        let thread = tokio::task::spawn_blocking(move || {
            match rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                // test read-only access
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                vfs_name.as_str(),
            ) {
                Ok(_) => panic!("Expected error"),
                Err(e) => {
                    assert_eq!("disk I/O error", e.to_string());
                }
            }
        });
        thread.await.unwrap();
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
}

#[derive(Debug)]
struct TestStorage {
    inner: Arc<InMemoryTreeStorage>,
    fail_on_write: tokio::sync::Mutex<bool>,
}

impl TestStorage {
    fn new(inner: Arc<InMemoryTreeStorage>, fail_on_write: bool) -> Self {
        Self {
            inner,
            fail_on_write: tokio::sync::Mutex::new(fail_on_write),
        }
    }

    async fn set_fail_on_write(&self, fail: bool) {
        let mut lock = self.fail_on_write.lock().await;
        *lock = fail;
    }
}

#[async_trait::async_trait]
impl StoreTree for TestStorage {
    async fn store_tree(&self, tree: &HashedTree) -> std::result::Result<BlobDigest, StoreError> {
        if *self.fail_on_write.lock().await {
            Err(StoreError::NoSpace)
        } else {
            self.inner.store_tree(tree).await
        }
    }
}

#[async_trait::async_trait]
impl LoadTree for TestStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<DelayedHashedTree, LoadError> {
        self.inner.load_tree(reference).await
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        self.inner.approximate_tree_count().await
    }
}

impl LoadStoreTree for TestStorage {}

#[test_log::test(tokio::test)]
async fn test_storage_write_error_in_sync() {
    // covers DatabaseHandle::sync
    let storage = Arc::new(TestStorage::new(
        Arc::new(InMemoryTreeStorage::empty()),
        false,
    ));
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    {
        let storage = storage.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            Handle::current().block_on(storage.set_fail_on_write(true));
            match connection.execute(
                "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                (),
            ) {
                Ok(_) => panic!("Expected error"),
                Err(e) => {
                    assert_eq!("disk I/O error", e.to_string());
                }
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    storage.set_fail_on_write(false).await;
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 1,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(0),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_storage_write_error_in_write_all_at() {
    // covers DatabaseHandle::write_all_at
    let storage = Arc::new(TestStorage::new(
        Arc::new(InMemoryTreeStorage::empty()),
        false,
    ));
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    {
        let storage = storage.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            connection
                .execute(
                    "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                    (),
                )
                .unwrap();
            Handle::current().block_on(storage.set_fail_on_write(true));
            {
                let transaction = connection.transaction().unwrap();
                let insert_count = 10000;
                for i in 0..insert_count {
                    match transaction
                        .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                    {
                        Ok(_) => {}
                        Err(e) => {
                            assert_eq!("disk I/O error", e.to_string());
                            assert_eq!(0, i);
                            break;
                        }
                    }
                }
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    storage.set_fail_on_write(false).await;
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 1,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 1,
                files_open_for_writing_count: 1,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(
        &directory,
        &BTreeMap::from([(
            FileName::try_from("test.db".to_string()).unwrap(),
            DirectoryEntryKind::File(8192),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_deleting_open_database_file() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = register_unique_vfs(
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
        Arc::new(|| Ok(())),
    );
    {
        let directory = directory.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let file_name = FileName::try_from("test.db").unwrap();
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                file_name.as_str(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name.as_str(),
            )
            .unwrap();
            Handle::current().block_on(async {
                directory.remove(&file_name).await.unwrap();
            });
            match connection.execute(
                "CREATE TABLE t (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    ) STRICT",
                (),
            ) {
                Ok(_) => panic!("Expected error"),
                Err(e) => {
                    assert_eq!("disk I/O error", e.to_string());
                }
            }
            connection.close().unwrap();
        });
        thread.await.unwrap();
    }
    {
        let directory_status = directory.request_save().await.unwrap();
        assert_eq!(directory_status.directories_open_count, 1);
        assert_eq!(directory_status.directories_unsaved_count, 0);
        assert_eq!(
            &directory_status.open_files,
            &OpenFileStats {
                files_open_count: 0,
                bytes_unflushed_count: 0,
                files_open_for_reading_count: 0,
                files_open_for_writing_count: 0,
                files_unflushed_count: 0,
            }
        );
    }
    expect_directory_entries(&directory, &BTreeMap::from([])).await;
}
