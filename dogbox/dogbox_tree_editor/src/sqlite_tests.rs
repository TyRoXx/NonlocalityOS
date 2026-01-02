use crate::{sqlite::register_vfs, OpenDirectory, TreeEditor};
use astraea::storage::InMemoryTreeStorage;
use std::sync::Arc;

#[test_log::test(tokio::test)]
async fn test_open_database() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = || std::time::SystemTime::UNIX_EPOCH;
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
    )
    .unwrap();
    let thread = tokio::task::spawn_blocking(move || {
        let connection = rusqlite::Connection::open_with_flags_and_vfs(
            "main.db",
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
    });
    thread.await.unwrap();
}
