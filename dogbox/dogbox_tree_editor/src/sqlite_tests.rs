use crate::sqlite::register_vfs;

#[test_log::test]
fn test_register_vfs() {
    let result = register_vfs("test_vfs");
    assert!(result.is_ok());
}

#[test_log::test(tokio::test)]
async fn test_open_database() {
    let vfs_name = "test_vfs";
    register_vfs(vfs_name).unwrap();
    let connection = rusqlite::Connection::open_with_flags_and_vfs(
        "main.db",
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        vfs_name,
    )
    .unwrap();
}
