use crate::{is_file_located_in_directory, start_watching_url_input_file, upgrade_schema};
use tracing::info;

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

#[test_log::test]
fn test_store_urls_in_database() {
    let mut connection =
        rusqlite::Connection::open_in_memory().expect("Failed to open in-memory database");
    upgrade_schema(&connection).expect("Failed to upgrade schema on new database");
    let urls = vec![
        "http://example.com/file1".to_string(),
        "http://example.com/file2".to_string(),
    ];
    crate::store_urls_in_database(urls.clone(), &mut connection)
        .expect("Failed to store URLs in database");
    let mut statement = connection
        .prepare("SELECT id, url, sha3_512_digest FROM download_job")
        .expect("Failed to prepare statement");
    let stored_urls: Vec<(i64, String)> = statement
        .query_map([], |row| {
            let id: i64 = row.get(0)?;
            let url: String = row.get(1)?;
            let digest: Option<String> = row.get(2)?;
            assert_eq!(digest, None);
            Ok((id, url))
        })
        .expect("Failed to query URLs")
        .map(|result| result.expect("Failed to get URL"))
        .collect();
    let expected = vec![(1, urls[0].clone()), (2, urls[1].clone())];
    assert_eq!(stored_urls, expected);
}

#[test_log::test(tokio::test)]
async fn test_start_watching_url_input_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let watched_directory = temp_dir.path().join("watched");
    std::fs::create_dir_all(&watched_directory).expect("Failed to create watched directory");
    let url_input_file_path = watched_directory.join("urls.txt");
    std::fs::write(&url_input_file_path, "").expect("Failed to create test file");
    let (url_input_file_watcher, watcher_thread, mut event_receiver) =
        start_watching_url_input_file(url_input_file_path.clone())
            .expect("Failed to start watching URL input file");
    std::fs::write(&url_input_file_path, "http://example.com")
        .expect("Failed to overwrite test file");
    let mut event_received = false;
    for _ in 0..3 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.recv()).await {
            Ok(event) => match event {
                Some(_) => {
                    event_received = true;
                    break;
                }
                None => {
                    panic!("Watcher channel closed unexpectedly");
                }
            },
            Err(e) => {
                info!("Timeout waiting for event: {:?}", e);
                info!("Overwriting watched file again to trigger another event");
                std::fs::write(&url_input_file_path, "http://example.com")
                    .expect("Failed to overwrite test file");
            }
        }
    }
    assert!(event_received);
    info!("Stopping file watcher");
    drop(url_input_file_watcher);
    info!("Joining watcher thread");
    watcher_thread.join().expect("Watcher thread panicked");
}

#[test_log::test]
fn test_parse_url_input_file() {
    assert_eq!(Vec::<String>::new(), crate::parse_url_input_file(""));
    assert_eq!(
        Vec::<String>::new(),
        crate::parse_url_input_file("\n\n\n\n")
    );
    assert_eq!(vec!["a"], crate::parse_url_input_file("a"));
    assert_eq!(vec!["a"], crate::parse_url_input_file("a\n"));
    assert_eq!(vec!["a"], crate::parse_url_input_file("a\r\n"));
    assert_eq!(vec!["a"], crate::parse_url_input_file(" a "));
    assert_eq!(vec!["a", "b", "c"], crate::parse_url_input_file("a\nb\nc"));
    assert_eq!(
        vec!["a", "b", "c"],
        crate::parse_url_input_file("a\r\nb\r\nc")
    );
    assert_eq!(vec!["a"], crate::parse_url_input_file("\n\na\n\n"));
}
