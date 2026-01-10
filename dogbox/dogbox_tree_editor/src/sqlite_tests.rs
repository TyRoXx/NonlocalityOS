use crate::{
    sqlite::register_vfs, CacheDropStats, NormalizedPath, OpenDirectory, OpenFileStats, TreeEditor,
};
use astraea::storage::InMemoryTreeStorage;
use dogbox_tree::serialization::FileName;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use rand::{rngs::SmallRng, SeedableRng};
use relative_path::RelativePath;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[test_log::test(tokio::test)]
async fn test_open_database() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage, clock, 1)
            .await
            .unwrap(),
    );
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name,
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
    let mut entries = BTreeMap::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        match entry.kind {
            crate::DirectoryEntryKind::File(size) => {
                entries.insert(entry.name.clone(), size);
            }
            crate::DirectoryEntryKind::Directory => {
                panic!("Unexpected directory");
            }
        }
    }
    assert_eq!(
        &entries,
        &BTreeMap::from([(FileName::try_from("test.db".to_string()).unwrap(), 8192)])
    );
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        match rusqlite::Connection::open_with_flags_and_vfs(
            "\\",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name,
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
    let mut entries = BTreeMap::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        match entry.kind {
            crate::DirectoryEntryKind::File(size) => {
                entries.insert(entry.name.clone(), size);
            }
            crate::DirectoryEntryKind::Directory => {
                panic!("Unexpected directory");
            }
        }
    }
    assert_eq!(&entries, &BTreeMap::from([]));
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    let editor = TreeEditor::new(directory.clone(), None);
    editor
        .create_directory(NormalizedPath::try_from(RelativePath::new("/dir")).unwrap())
        .await
        .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        match rusqlite::Connection::open_with_flags_and_vfs(
            "dir",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name,
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
    let mut entries = BTreeSet::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        match entry.kind {
            crate::DirectoryEntryKind::File(_size) => {
                panic!("Unexpected file");
            }
            crate::DirectoryEntryKind::Directory => {
                entries.insert(entry.name.clone());
            }
        }
    }
    assert_eq!(
        &entries,
        &BTreeSet::from([FileName::try_from("dir".to_string()).unwrap()])
    );
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    let insert_count = 100;
    {
        let thread = tokio::task::spawn_blocking(move || {
            let mut connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name,
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
    let mut entries = BTreeMap::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        match entry.kind {
            crate::DirectoryEntryKind::File(size) => {
                entries.insert(entry.name.clone(), size);
            }
            crate::DirectoryEntryKind::Directory => {
                panic!("Unexpected directory");
            }
        }
    }
    assert_eq!(
        &entries,
        &BTreeMap::from([(FileName::try_from("test.db".to_string()).unwrap(), 8192)])
    );
    {
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                // test read-only access
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
                vfs_name,
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        let connection = rusqlite::Connection::open_with_flags_and_vfs(
            "test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            vfs_name,
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
        for i in 0..100 {
            connection
                .execute("INSERT INTO t (name) VALUES (?1)", [format!("Name {}", i)])
                .unwrap();
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
    let mut entries = BTreeMap::new();
    let mut entry_stream = directory.read().await;
    while let Some(entry) = entry_stream.next().await {
        match entry.kind {
            crate::DirectoryEntryKind::File(size) => {
                entries.insert(entry.name.clone(), size);
            }
            crate::DirectoryEntryKind::Directory => {
                panic!("Unexpected directory");
            }
        }
    }
    assert_eq!(
        &entries,
        &BTreeMap::from([(FileName::try_from("test.db".to_string()).unwrap(), 0)])
    );
}
