use crate::{
    sqlite::{register_vfs, PagesVfs},
    CacheDropStats, NormalizedPath, OpenDirectory, OpenFileStats, TreeEditor,
};
use astraea::{
    storage::{
        DelayedHashedTree, InMemoryTreeStorage, LoadError, LoadStoreTree, LoadTree, StoreError,
        StoreTree,
    },
    tree::{BlobDigest, HashedTree},
};
use dogbox_tree::serialization::FileName;
use futures::StreamExt;
use pretty_assertions::assert_eq;
use rand::{rngs::SmallRng, SeedableRng};
use relative_path::RelativePath;
use sqlite_vfs::Vfs;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Handle;
use tracing::info;

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
        .open_file(NormalizedPath::try_from(RelativePath::new("/test")).unwrap())
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();
    let random_number_generator = Box::new(SmallRng::seed_from_u64(123));
    let vfs: PagesVfs<4096> = PagesVfs::new(editor, runtime, random_number_generator);
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
        &BTreeMap::from([(FileName::try_from("test".to_string()).unwrap(), 0)])
    );
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
    let vfs: PagesVfs<4096> = PagesVfs::new(editor, runtime, random_number_generator);
    let thread = tokio::task::spawn_blocking(move || {
        assert_eq!("", &vfs.temporary_name());
    });
    thread.await.unwrap();
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
    let vfs: PagesVfs<4096> = PagesVfs::new(editor, runtime, random_number_generator);
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
    let vfs: PagesVfs<4096> = PagesVfs::new(editor, runtime, random_number_generator);
    let thread = tokio::task::spawn_blocking(move || {
        let duration = Duration::from_micros(1);
        let elapsed = vfs.sleep(duration);
        assert!(elapsed >= duration);
    });
    thread.await.unwrap();
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
async fn test_open_no_create() {
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
            // no SQLITE_OPEN_CREATE
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            vfs_name,
        )
        // TODO: fix the implementation to make this fail
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

#[test_log::test(tokio::test)]
async fn test_storage_read_error() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
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
                vfs_name,
            ) {
                Ok(_) => panic!("Expected error"),
                Err(e) => {
                    assert_eq!("disk I/O error", e.to_string());
                }
            }
        });
        thread.await.unwrap();
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    {
        let storage = storage.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                "test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name,
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
    let vfs_name = "test_vfs";
    register_vfs(
        vfs_name,
        TreeEditor::new(directory.clone(), None),
        tokio::runtime::Handle::current(),
        Box::new(SmallRng::seed_from_u64(123)),
    )
    .unwrap();
    {
        let storage = storage.clone();
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
async fn test_deleting_open_database_file() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from(""), storage.clone(), clock, 1)
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
    {
        let directory = directory.clone();
        let thread = tokio::task::spawn_blocking(move || {
            let file_name = FileName::try_from("test.db").unwrap();
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                file_name.as_str(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                vfs_name,
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
