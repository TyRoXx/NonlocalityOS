use crate::{
    is_relevant_change_to_url_input_file, keep_reading_url_input_file,
    load_downloaded_urls_from_database, load_undownloaded_urls_from_database,
    make_database_file_name, make_url_input_file_path, prepare_database, run_application,
    run_download_job, run_main_loop, set_download_job_digests, start_watching_url_input_file,
    store_urls_in_database, upgrade_schema, Download, SetDownloadJobDigestOutcome,
};
use astraea::tree::BlobDigest;
use pretty_assertions::assert_eq;
use tracing::info;

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
fn test_upgrade_schema_for_unknown_user_version() {
    let unsupported_version = 23;
    let connection =
        rusqlite::Connection::open_in_memory().expect("Failed to open in-memory database");
    connection
        .execute(
            &format!("PRAGMA user_version = {};", unsupported_version),
            (),
        )
        .unwrap();
    match upgrade_schema(&connection) {
        Ok(_) => panic!("Expected error for unknown user version"),
        Err(e) => {
            let error_message = e.to_string();
            assert_eq!(
                format!(
                    "Unsupported database schema version: {}",
                    unsupported_version
                ),
                error_message
            );
        }
    }
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
    assert_eq!(
        2,
        crate::store_urls_in_database(urls.clone(), &mut connection)
            .expect("Failed to store URLs in database")
    );
    let stored_urls = load_undownloaded_urls_from_database(&mut connection).unwrap();
    assert_eq!(stored_urls, urls);
}

#[test_log::test]
fn test_load_undownloaded_urls_from_database() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let mut connection = prepare_database(temp_dir.path()).expect("Failed to prepare database");
    assert_eq!(
        Vec::<String>::new(),
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
    let url = "http://example.com/file1";
    assert_eq!(
        1,
        store_urls_in_database(vec![url.to_string()], &mut connection).unwrap()
    );
    assert_eq!(
        vec![url.to_string()],
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
}

#[test_log::test]
fn test_set_download_job_digests_1() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let mut connection = prepare_database(temp_dir.path()).expect("Failed to prepare database");
    let url = "http://example.com/file1";
    let digest = BlobDigest::hash(b"test data");
    assert_eq!(
        SetDownloadJobDigestOutcome::UrlNotFound,
        set_download_job_digests(&mut connection, url, &[digest]).unwrap()
    );
    assert_eq!(
        1,
        store_urls_in_database(vec![url.to_string()], &mut connection).unwrap()
    );
    assert_eq!(
        vec![url.to_string()],
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
    assert_eq!(
        SetDownloadJobDigestOutcome::Success,
        set_download_job_digests(&mut connection, url, &[digest]).unwrap()
    );
    assert_eq!(
        Vec::<String>::new(),
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
}

#[test_log::test]
fn test_set_download_job_digests_2() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let mut connection = prepare_database(temp_dir.path()).expect("Failed to prepare database");
    let url = "http://example.com/file1";
    let digests = [
        BlobDigest::hash(b"test data 1"),
        BlobDigest::hash(b"test data 2"),
    ];
    assert_eq!(
        SetDownloadJobDigestOutcome::UrlNotFound,
        set_download_job_digests(&mut connection, url, &digests).unwrap()
    );
    assert_eq!(
        1,
        store_urls_in_database(vec![url.to_string()], &mut connection).unwrap()
    );
    assert_eq!(
        vec![url.to_string()],
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
    assert_eq!(
        SetDownloadJobDigestOutcome::Success,
        set_download_job_digests(&mut connection, url, &digests).unwrap()
    );
    assert_eq!(
        Vec::<String>::new(),
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
}

#[test_log::test]
fn test_set_download_job_digests_repeat() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let mut connection = prepare_database(temp_dir.path()).expect("Failed to prepare database");
    let url = "http://example.com/file1";
    let digest = BlobDigest::hash(b"test data");
    assert_eq!(
        1,
        store_urls_in_database(vec![url.to_string()], &mut connection).unwrap()
    );
    assert_eq!(
        vec![url.to_string()],
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
    assert_eq!(
        SetDownloadJobDigestOutcome::Success,
        set_download_job_digests(&mut connection, url, &[digest]).unwrap()
    );
    assert_eq!(
        Vec::<String>::new(),
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
    assert_eq!(
        SetDownloadJobDigestOutcome::Success,
        set_download_job_digests(&mut connection, url, &[digest]).unwrap()
    );
    assert_eq!(
        Vec::<String>::new(),
        load_undownloaded_urls_from_database(&mut connection).unwrap()
    );
}

#[test_log::test]
fn test_prepare_database() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let mut connection = prepare_database(temp_dir.path()).expect("Failed to prepare database");
    assert_eq!(
        1,
        store_urls_in_database(vec!["http://example.com".into()], &mut connection).unwrap()
    );
}

#[test_log::test]
fn test_prepare_database_directory_not_found() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let non_existent_dir = temp_dir.path().join("non_existent");
    match prepare_database(&non_existent_dir) {
        Ok(_) => panic!("Expected error for non-existent directory"),
        Err(e) => {
            let error_message = e.to_string();
            assert_eq!("Failed to open or create database file", error_message);
        }
    }
}

#[test_log::test]
fn test_prepare_database_upgrade_fails() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    {
        let database_path = make_database_file_name(temp_dir.path());
        let unsupported_version = 23;
        let connection = rusqlite::Connection::open_with_flags(
            &database_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
        )
        .unwrap();
        connection
            .execute(
                &format!("PRAGMA user_version = {};", unsupported_version),
                (),
            )
            .unwrap();
    }
    match prepare_database(temp_dir.path()) {
        Ok(_) => {
            panic!("Expected error for unsupported database schema version");
        }
        Err(e) => {
            assert_eq!("Failed to upgrade database schema", e.to_string());
        }
    }
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_positive() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    for event_paths in [
        vec![url_input_file_path.into()],
        vec![
            url_input_file_path.into(),
            std::path::PathBuf::from("/other/path.txt"),
        ],
        vec![
            std::path::PathBuf::from("/other/path.txt"),
            url_input_file_path.into(),
        ],
    ] {
        for data_change in [
            notify::event::DataChange::Any,
            notify::event::DataChange::Content,
            notify::event::DataChange::Size,
            notify::event::DataChange::Other,
        ] {
            assert!(is_relevant_change_to_url_input_file(
                &notify::Event {
                    kind: notify::EventKind::Modify(notify::event::ModifyKind::Data(data_change,)),
                    paths: event_paths.clone(),
                    attrs: Default::default(),
                },
                url_input_file_path
            ));
        }
        assert!(is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Any),
                paths: event_paths.clone(),
                attrs: Default::default(),
            },
            url_input_file_path
        ));
        assert!(is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Name(
                    notify::event::RenameMode::To
                )),
                paths: event_paths,
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_negative_empty_paths() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    let event_paths = Vec::new();
    for data_change in [
        notify::event::DataChange::Any,
        notify::event::DataChange::Content,
        notify::event::DataChange::Size,
        notify::event::DataChange::Other,
    ] {
        assert!(!is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Data(data_change,)),
                paths: event_paths.clone(),
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
    assert!(!is_relevant_change_to_url_input_file(
        &notify::Event {
            kind: notify::EventKind::Modify(notify::event::ModifyKind::Any),
            paths: event_paths,
            attrs: Default::default(),
        },
        url_input_file_path
    ));
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_negative_wrong_event_kind() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    for event_kind in [
        notify::EventKind::Create(notify::event::CreateKind::Any),
        notify::EventKind::Remove(notify::event::RemoveKind::Any),
        notify::EventKind::Access(notify::event::AccessKind::Any),
        notify::EventKind::Other,
        notify::EventKind::Any,
    ] {
        assert!(!is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: event_kind,
                paths: vec![url_input_file_path.into()],
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_negative_wrong_modify_kind() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    for modify_kind in [
        notify::event::ModifyKind::Metadata(notify::event::MetadataKind::Any),
        notify::event::ModifyKind::Other,
    ] {
        assert!(!is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(modify_kind),
                paths: vec![url_input_file_path.into()],
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_negative_wrong_rename_kind() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    for rename_kind in [
        notify::event::RenameMode::Any,
        notify::event::RenameMode::From,
        notify::event::RenameMode::Both,
        notify::event::RenameMode::Other,
    ] {
        assert!(!is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Name(rename_kind)),
                paths: vec![url_input_file_path.into()],
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
}

#[test_log::test]
fn test_is_relevant_change_to_url_input_file_negative_wrong_path() {
    let url_input_file_path = std::path::Path::new("/path/to/urls.txt");
    for event_path in [
        std::path::Path::new("/path/to/other.txt"),
        std::path::Path::new("/path/to/urls.txt.bak"),
        std::path::Path::new("/path/to"),
        std::path::Path::new("urls.txt"),
        std::path::Path::new("/path/urls.txt"),
    ] {
        for data_change in [
            notify::event::DataChange::Any,
            notify::event::DataChange::Content,
            notify::event::DataChange::Size,
            notify::event::DataChange::Other,
        ] {
            assert!(!is_relevant_change_to_url_input_file(
                &notify::Event {
                    kind: notify::EventKind::Modify(notify::event::ModifyKind::Data(data_change,)),
                    paths: vec![event_path.into()],
                    attrs: Default::default(),
                },
                url_input_file_path
            ));
        }
        assert!(!is_relevant_change_to_url_input_file(
            &notify::Event {
                kind: notify::EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![event_path.into()],
                attrs: Default::default(),
            },
            url_input_file_path
        ));
    }
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

    // The first change event is emitted regardless of any changes.
    tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.changed())
        .await
        .unwrap()
        .unwrap();

    // The second change event is emitted after the file is overwritten.
    info!("Overwriting watched file again to trigger another event");
    std::fs::write(&url_input_file_path, "http://example.com\n")
        .expect("Failed to overwrite test file");
    tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.changed())
        .await
        .unwrap()
        .unwrap();

    // The next change event is emitted after the file is renamed.
    let other_file_path = watched_directory.join("urls.txt.temp");
    std::fs::write(&other_file_path, "http://example.com\nhttp://example.org\n")
        .expect("Failed to create the other test file");
    std::fs::rename(&other_file_path, &url_input_file_path)
        .expect("Failed to rename the other test file to watched file");
    tokio::time::timeout(std::time::Duration::from_secs(1), event_receiver.changed())
        .await
        .unwrap()
        .unwrap();

    info!("Stopping file watcher");
    drop(url_input_file_watcher);
    info!("Joining watcher thread");
    watcher_thread.join().expect("Watcher thread panicked");
}

#[test_log::test(tokio::test)]
async fn test_start_watching_url_input_file_with_invalid_path() {
    let result = start_watching_url_input_file("/".into());
    match result {
        Ok(_) => {
            panic!("Expected error for invalid URL input file path");
        }
        Err(e) => {
            assert_eq!("Failed to get parent directory", e.to_string());
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_read_file_tolerantly() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, "http://example.com\n").expect("Failed to write test file");
    let (urls, attempts) =
        crate::read_file_tolerantly(&file_path, &std::time::Duration::from_secs(1)).await;
    assert_eq!("http://example.com\n", urls);
    assert_eq!(1, attempts);
}

#[test_log::test(tokio::test)]
async fn test_read_file_tolerantly_not_found() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let file_path = temp_dir.path().join("test.txt");
    tokio::join!(
        {
            let file_path = file_path.clone();
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                std::fs::write(&file_path, "http://example.com\n")
                    .expect("Failed to write test file");
            }
        },
        async move {
            let (urls, attempts) =
                crate::read_file_tolerantly(&file_path, &std::time::Duration::from_millis(1)).await;
            assert_eq!("http://example.com\n", urls);
            assert!(attempts >= 2);
            assert!(attempts <= 5);
        }
    );
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

#[test_log::test(tokio::test)]
async fn test_keep_reading_url_input_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let database_dir = temp_dir.path().join("database");
    std::fs::create_dir_all(&database_dir).expect("Failed to create database directory");
    let mut connection1 = prepare_database(&database_dir).expect("Failed to prepare database");
    let mut connection2 = prepare_database(&database_dir).expect("Failed to prepare database");
    let input_dir = temp_dir.path().join("input");
    std::fs::create_dir_all(&input_dir).expect("Failed to create input directory");
    let input_file_path = input_dir.join("urls.txt");
    let (sender, receiver) = tokio::sync::watch::channel(());
    let file_writer = async {
        std::fs::write(&input_file_path, "http://example.com\n")
            .expect("Failed to write test file");
        sender.send(()).expect("Failed to send initial signal");
        loop {
            let urls = load_undownloaded_urls_from_database(&mut connection1).unwrap();
            if !urls.is_empty() {
                assert_eq!(urls, vec!["http://example.com".to_string()]);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    };
    let (database_change_sender, mut database_change_receiver) =
        tokio::sync::watch::channel::<()>(());
    let retry_delay = std::time::Duration::from_millis(1);
    tokio::select! {
        _ = keep_reading_url_input_file(&input_file_path, receiver, database_change_sender, &mut connection2, &retry_delay) => {
            panic!("keep_reading_url_input_file should not return");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Timeout");
        }
        _ = file_writer => {
            // success
        }
    }
    // there must a database change event now
    database_change_receiver.changed().await.unwrap();
}

struct FakeDownload {
    result_digests: Vec<BlobDigest>,
}

#[async_trait::async_trait]
impl Download for FakeDownload {
    async fn download(&self, url: &str) -> Result<Vec<BlobDigest>, Box<dyn std::error::Error>> {
        assert_eq!("http://example.com", url);
        Ok(self.result_digests.clone())
    }
}

#[test_log::test(tokio::test)]
async fn test_run_download_job_url_not_found_in_database() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let database_dir = temp_dir.path().join("database");
    std::fs::create_dir_all(&database_dir).expect("Failed to create database directory");
    let mut connection = prepare_database(&database_dir).expect("Failed to prepare database");
    let download = FakeDownload {
        result_digests: vec![BlobDigest::hash(b"test data")],
    };
    match run_download_job(&mut connection, &download, "http://example.com").await {
        Ok(_) => {
            panic!("Expected error for URL not found in database");
        }
        Err(err) => {
            assert_eq!(err.to_string(), "URL not found in database");
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_run_main_loop() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let working_directory = temp_dir.path().join("working");
    std::fs::create_dir_all(&working_directory).expect("Failed to create working directory");
    let url_input_file_path = working_directory.join("urls.txt");
    let (url_input_file_event_sender, url_input_file_event_receiver) =
        tokio::sync::watch::channel(());
    let digest = BlobDigest::hash(b"test data");
    let download = FakeDownload {
        result_digests: vec![digest],
    };
    let retry_delay = std::time::Duration::from_millis(1);
    tokio::select! {
        _ = {
            let url_input_file_path = url_input_file_path.clone();
            let working_directory = working_directory.clone();
            async move {
                std::fs::write(&url_input_file_path, "http://example.com\n").expect("Failed to write test file");
                url_input_file_event_sender.send(()).unwrap();
                let mut connection = prepare_database(&working_directory).expect("Failed to prepare database");
                loop {
                    let urls = load_downloaded_urls_from_database(&mut connection).unwrap();
                    if !urls.is_empty() {
                        assert_eq!(vec![("http://example.com".to_string(), digest)], urls);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        } => {
            // success
        }
        _ = run_main_loop(
            &working_directory,
            &url_input_file_path,
            url_input_file_event_receiver,
            &download,
            &retry_delay
        ) => {
            panic!("run_application should not return");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Timeout");
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_run_application() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let working_directory = temp_dir.path().join("working");
    std::fs::create_dir_all(&working_directory).expect("Failed to create working directory");
    let digest = BlobDigest::hash(b"test data");
    let download = FakeDownload {
        result_digests: vec![digest],
    };
    let retry_delay = std::time::Duration::from_millis(1);
    let wait_for_completion = {
        let working_directory = working_directory.clone();
        async move {
            let url_input_file_path = make_url_input_file_path(&working_directory);
            std::fs::write(&url_input_file_path, "http://example.com\n")
                .expect("Failed to write test file");
            let mut connection =
                prepare_database(&working_directory).expect("Failed to prepare database");
            loop {
                let urls = load_downloaded_urls_from_database(&mut connection).unwrap();
                if !urls.is_empty() {
                    assert_eq!(vec![("http://example.com".to_string(), digest)], urls);
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    };
    tokio::select! {
        _ = run_application(&working_directory, &download, &retry_delay) => {
            panic!("run_application should not return");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            panic!("Timeout");
        }
        _ = wait_for_completion => {
            // success
        }
    }
}
