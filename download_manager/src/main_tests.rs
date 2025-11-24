use tracing::info;

use crate::{is_file_located_in_directory, start_watching_url_input_file, upgrade_schema};

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

#[test_log::test(tokio::test)]
async fn test_start_watching_url_input_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let watched_directory = temp_dir.path().join("watched");
    std::fs::create_dir_all(&watched_directory).expect("Failed to create watched directory");
    let url_input_file_path = watched_directory.join("urls.txt");
    std::fs::write(&url_input_file_path, "").expect("Failed to create test file");
    let (url_input_file_watcher, watcher_thread, mut event_receiver) =
        start_watching_url_input_file(&url_input_file_path)
            .expect("Failed to start watching URL input file");
    std::fs::write(&url_input_file_path, "http://example.com")
        .expect("Failed to overwrite test file");
    let mut event_received = false;
    for _ in 0..3 {
        match tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.recv()).await {
            Ok(event) => match event {
                Some(Ok(notify::Event {
                    paths,
                    kind,
                    attrs: _,
                })) => {
                    if paths.contains(&url_input_file_path) {
                        match kind {
                            notify::EventKind::Modify(modify_kind) => match modify_kind {
                                notify::event::ModifyKind::Data(_)
                                | notify::event::ModifyKind::Any => {
                                    info!("Received expected modify data event");
                                    event_received = true;
                                    break;
                                }
                                _ => {
                                    panic!("Unexpected modify kind received: {:?}", modify_kind);
                                }
                            },
                            _ => {
                                panic!("Unexpected event kind received: {:?}", kind);
                            }
                        }
                    } else {
                        info!("Ignoring event for other paths: {:?}", paths);
                    }
                }
                Some(Err(e)) => {
                    panic!("Error received from watcher: {:?}", e);
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
