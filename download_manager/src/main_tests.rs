use crate::{is_file_located_in_directory, upgrade_schema};

#[test]
fn test_is_file_located_in_directory() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let directory_a = temp_dir.path().join("/home/user/a");
    std::fs::create_dir_all(&directory_a).expect("Failed to create test directory");
    let directory_b = temp_dir.path().join("/home/user/b");
    std::fs::create_dir_all(&directory_b).expect("Failed to create test directory");
    let file_in_directory_a = directory_a.join("/home/user/a/file.txt");
    std::fs::write(&file_in_directory_a, "Test content").expect("Failed to create test file");
    let file_in_directory_b = directory_b.join("/home/user/b/file.txt");
    std::fs::write(&file_in_directory_b, "Test content").expect("Failed to create test file");
    assert!(is_file_located_in_directory(&file_in_directory_a, &directory_a).unwrap());
    assert!(is_file_located_in_directory(&file_in_directory_b, &directory_b).unwrap());
    assert!(!is_file_located_in_directory(&file_in_directory_b, &directory_a).unwrap());
    assert!(!is_file_located_in_directory(&file_in_directory_a, &directory_b).unwrap());
}

#[test]
fn test_is_file_located_in_directory_file_not_found() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let directory_a = temp_dir.path().join("/home/user/a");
    std::fs::create_dir_all(&directory_a).expect("Failed to create test directory");
    let file_in_directory_a = directory_a.join("/home/user/a/file.txt");
    assert!(is_file_located_in_directory(&file_in_directory_a, &directory_a).unwrap());
}

#[test]
fn test_is_file_located_in_directory_directory_not_found() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let directory_a = temp_dir.path().join("/home/user/a");
    let file_in_directory_a = directory_a.join("/home/user/a/file.txt");
    assert!(is_file_located_in_directory(&file_in_directory_a, &directory_a).unwrap());
}

#[test_log::test]
fn test_is_file_located_in_directory_file_found_but_not_directory() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let directory_a = temp_dir.path().join("/home/user/a");
    std::fs::create_dir_all(&directory_a).expect("Failed to create test directory");
    let directory_b = temp_dir.path().join("/home/user/b");
    let file_in_directory_a = directory_a.join("/home/user/a/file.txt");
    std::fs::write(&file_in_directory_a, "Test content").expect("Failed to create test file");
    assert!(!is_file_located_in_directory(&file_in_directory_a, &directory_b).unwrap());
}

#[test_log::test]
fn test_upgrade_schema_on_new_database() {
    let connection =
        rusqlite::Connection::open_in_memory().expect("Failed to open in-memory database");
    upgrade_schema(&connection).expect("Failed to upgrade schema on new database");
}

#[test_log::test]
fn test_upgrade_schema_on_existing_database() {
    let connection =
        rusqlite::Connection::open_in_memory().expect("Failed to open in-memory database");
    upgrade_schema(&connection).expect("Failed to upgrade schema on new database");
    upgrade_schema(&connection).expect("Failed to upgrade schema on existing database");
}
