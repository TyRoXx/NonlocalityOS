use astraea::{
    storage::{CommitChanges, LoadRoot, SQLiteStorage, UpdateRoot},
    tree::VALUE_BLOB_MAX_LENGTH,
};
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{OpenDirectory, OpenDirectoryStatus, WallClock};
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, net::SocketAddr, path::Path, pin::Pin, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};
use tracing_subscriber::fmt::format::FmtSpan;
mod file_system;
mod file_system_test;
use file_system::DogBoxFileSystem;

async fn serve_connection(stream: TcpStream, dav_server: Arc<DavHandler>) {
    let make_service = move |request: Request<body::Incoming>| {
        debug!("Request: {:?}", &request);
        let dav_server = dav_server.clone();
        async move {
            let response = dav_server.handle(request).await;
            Ok::<_, Infallible>(response)
        }
    };
    let io = TokioIo::new(stream);
    if let Err(err) = http1::Builder::new()
        .max_buf_size(VALUE_BLOB_MAX_LENGTH * 200)
        .serve_connection(io, hyper::service::service_fn(make_service))
        .await
    {
        error!("Error serving connection: {:?}", err);
    }
}

async fn handle_tcp_connections(
    listener: TcpListener,
    dav_server: Arc<DavHandler>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
        tokio::task::spawn(async move { serve_connection(stream, dav_server).await });
    }
}

#[derive(Debug, PartialEq)]
enum SaveStatus {
    Saved { files_open_for_writing_count: usize },
    Saving,
}

async fn save_root_regularly(root: Arc<OpenDirectory>, minimum_delay: std::time::Duration) {
    let maximum_delay = std::time::Duration::from_secs(10) + (2 * minimum_delay);
    let mut previous_status = None;
    let mut next_wait_time = minimum_delay;
    loop {
        let save_result = root.request_save().await;
        match save_result {
            Ok(status) => {
                let is_same_as_before = Some(&status) == previous_status.as_ref();
                previous_status = Some(status);
                if is_same_as_before {
                    next_wait_time = std::cmp::min(
                        maximum_delay,
                        next_wait_time + std::time::Duration::from_millis(20),
                    );
                } else {
                    next_wait_time = minimum_delay;
                }
                tokio::time::sleep(next_wait_time).await;
            }
            Err(error_) => {
                error!("request_save failed with {:?}", &error_);
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            }
        }
    }
}

async fn persist_root_on_change(
    root: Arc<OpenDirectory>,
    root_name: &str,
    blob_storage_update: &(dyn UpdateRoot + Sync),
    blob_storage_commit: &(dyn CommitChanges + Sync),
    save_status_sender: tokio::sync::mpsc::Sender<SaveStatus>,
) {
    let mut number_of_no_changes_in_a_row: u64 = 0;
    let mut receiver = root.watch().await;
    let mut previous_root_status: OpenDirectoryStatus = *receiver.borrow();
    loop {
        let root_status = *receiver.borrow();
        if previous_root_status == root_status {
            info!("Root didn't change");
            number_of_no_changes_in_a_row += 1;
            assert_ne!(10, number_of_no_changes_in_a_row);
        } else {
            info!("Root changed: {:?}", &root_status);
            number_of_no_changes_in_a_row = 0;
            if previous_root_status.digest.last_known_digest == root_status.digest.last_known_digest
            {
                info!("Root status changed, but the last known digest stays the same.");
            } else {
                blob_storage_update.update_root(root_name, &root_status.digest.last_known_digest);
                blob_storage_commit.commit_changes().unwrap(/*TODO*/);
            }
            let save_status = if root_status.digest.is_digest_up_to_date {
                assert!(root_status.bytes_unflushed_count == 0);
                assert!(root_status.files_unflushed_count == 0);
                assert!(root_status.directories_unsaved_count == 0);
                info!("Root digest is up to date.");
                SaveStatus::Saved {
                    files_open_for_writing_count: root_status.files_open_for_writing_count,
                }
            } else {
                assert!(root_status.directories_unsaved_count != 0);
                debug!("Root digest is not up to date.");
                SaveStatus::Saving
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                save_status_sender.send(save_status),
            )
            .await
            .unwrap()
            .unwrap();
            previous_root_status = root_status;
        }
        debug!("Waiting for root to change.");
        let maybe_changed = receiver.changed().await;
        match maybe_changed {
            Ok(_) => {
                debug!("changed() event!");
            }
            Err(error_) => {
                error!("Could not wait for change event: {:?}", &error_);
                return;
            }
        }
    }
}

async fn run_dav_server(
    listener: TcpListener,
    database_file_name: &Path,
    modified_default: std::time::SystemTime,
    clock: WallClock,
    minimum_save_delay: std::time::Duration,
) -> Result<
    (
        tokio::sync::mpsc::Receiver<SaveStatus>,
        Pin<
            Box<
                dyn std::future::Future<
                    Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
                >,
            >,
        >,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let database_existed = std::fs::exists(&database_file_name).unwrap();
    let sqlite_connection = rusqlite::Connection::open(&database_file_name)?;
    if !database_existed {
        match SQLiteStorage::create_schema(&sqlite_connection) {
            Ok(_) => {}
            Err(error) => {
                println!(
                    "Could not create SQL schema in {}: {:?}",
                    &database_file_name.display(),
                    &error
                );
                println!("Deleting {}", &database_file_name.display());
                std::fs::remove_file(&database_file_name).unwrap();
                panic!();
            }
        }
    }
    let blob_storage = Arc::new(SQLiteStorage::new(sqlite_connection));
    let root_name = "latest";
    let root = match blob_storage.load_root(&root_name) {
        Some(found) => {
            OpenDirectory::load_directory(blob_storage.clone(), &found, modified_default, clock).await.unwrap(/*TODO*/)
        }
        None => {
            let dir = Arc::new(
                OpenDirectory::create_directory(blob_storage.clone(), clock)
                .await
                .unwrap(/*TODO*/),
            );
            let status = dir.request_save().await.unwrap();
            assert!(status.digest.is_digest_up_to_date);
            blob_storage.update_root(root_name, &status.digest.last_known_digest);
            blob_storage.commit_changes().unwrap();
            dir
        }
    };
    let tree_editor = dogbox_tree_editor::TreeEditor::new(root.clone(), None);
    let dav_server = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(tree_editor)))
            .locksystem(FakeLs::new())
            .build_handler(),
    );
    let (save_status_sender, save_status_receiver) = tokio::sync::mpsc::channel(6);
    let result = async move {
        let root_cloned = root.clone();
        let join_result = tokio::try_join!(
            async move {
                save_root_regularly(root, minimum_save_delay).await;
                Ok(())
            },
            async move {
                persist_root_on_change(
                    root_cloned,
                    &root_name,
                    &*blob_storage,
                    &*blob_storage,
                    save_status_sender,
                )
                .await;
                Ok(())
            },
            async move {
                handle_tcp_connections(listener, dav_server).await.unwrap();
                Ok(())
            }
        );
        join_result.map(|_| ())
    };
    Ok((save_status_receiver, Box::pin(result)))
}

#[cfg(test)]
mod tests {
    use crate::run_dav_server;
    use astraea::tree::VALUE_BLOB_MAX_LENGTH;
    use dogbox_tree_editor::WallClock;
    use reqwest_dav::{list_cmd::ListEntity, Auth, Client, ClientBuilder, Depth};
    use std::{future::Future, net::SocketAddr, pin::Pin};
    use tokio::net::TcpListener;
    use tracing::info;

    async fn run_dav_server_instance<'t>(
        database_file_name: &std::path::Path,
        change_files: Option<Box<dyn FnOnce(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>>>,
        is_saving_expected: bool,
        verify_changes: &impl Fn(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
        modified_default: std::time::SystemTime,
        clock: WallClock,
    ) {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(address).await.unwrap();
        let actual_address = listener.local_addr().unwrap();
        let server_url = format!("http://{}", actual_address);
        let (mut save_status_receiver, server) = run_dav_server(
            listener,
            &database_file_name,
            modified_default,
            clock,
            std::time::Duration::ZERO,
        )
        .await
        .unwrap();
        let client_side_testing = async move {
            if let Some(change_files2) = change_files {
                change_files2(create_client(server_url.clone())).await;
            }
            verify_changes(create_client(server_url.clone())).await;
            // verify again to be extra sure this is deterministic
            verify_changes(create_client(server_url)).await;
        };
        let waiting_for_saved = async {
            if is_saving_expected {
                info!("Waiting for the save status to become saved.");
                loop {
                    let mut events = Vec::new();
                    save_status_receiver.recv_many(&mut events, 100).await;
                    info!("Receive save status: {:?}", &events);
                    match events.last().unwrap() {
                        crate::SaveStatus::Saved {
                            files_open_for_writing_count,
                        } => {
                            info!(
                                "The save status became saved with {} files open for writing.",
                                *files_open_for_writing_count
                            );
                            if *files_open_for_writing_count == 0 {
                                break;
                            }
                            info!("Waiting for remaining files to be closed.");
                        }
                        crate::SaveStatus::Saving => info!("Still saving"),
                    }
                }
            } else {
                match tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    save_status_receiver.recv(),
                )
                .await
                {
                    Ok(status) => assert_eq!(
                        Some(crate::SaveStatus::Saved {
                            files_open_for_writing_count: 0
                        }),
                        status
                    ),
                    Err(_elapsed) => {}
                }
            }
        };
        let testing = async {
            client_side_testing.await;
            waiting_for_saved.await;
        };
        tokio::select! {
            result = server => {
                panic!("Server isn't expected to exit: {:?}", result);
            }
            _ = testing => {
            }
        };
    }

    async fn test_fresh_dav_server<'t>(
        change_files: Option<Box<dyn FnOnce(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>>>,
        verify_changes: &impl Fn(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
    ) {
        let clock = || {
            std::time::SystemTime::UNIX_EPOCH
                .checked_add(std::time::Duration::from_secs(13))
                .unwrap()
        };
        let modified_default = clock();
        let temporary_directory = tempfile::tempdir().unwrap();
        let database_file_name = temporary_directory.path().join("dogbox_dav_server.sqlite");
        assert!(!std::fs::exists(&database_file_name).unwrap());
        info!("First test server instance");
        {
            let is_saving_expected = change_files.is_some();
            run_dav_server_instance(
                &database_file_name,
                change_files,
                is_saving_expected,
                &verify_changes,
                modified_default,
                clock,
            )
            .await;
        }

        // Start a new instance with the database from the first instance to check if the data was persisted correctly.
        info!("Second test server instance");
        assert!(std::fs::exists(&database_file_name).unwrap());
        run_dav_server_instance(
            &database_file_name,
            None,
            false,
            &verify_changes,
            modified_default,
            clock,
        )
        .await;
        assert!(std::fs::exists(&database_file_name).unwrap());
    }

    async fn list_directory(client: &Client, directory: &str) -> Vec<ListEntity> {
        return client.list(directory, Depth::Number(1)).await.unwrap();
    }

    fn create_client(server_url: String) -> Client {
        let client = ClientBuilder::new()
            .set_host(server_url)
            .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
            .build()
            .unwrap();
        client
    }

    fn expect_directory(entity: &ListEntity, name: &str) {
        match entity {
            reqwest_dav::list_cmd::ListEntity::File(_) => panic!(),
            reqwest_dav::list_cmd::ListEntity::Folder(folder) => {
                assert_eq!(name, folder.href);
                assert_eq!(None, folder.quota_used_bytes);
                assert_eq!(None, folder.quota_available_bytes);
                //TODO: check tag value
                assert_eq!(true, folder.tag.is_some());
                //TODO: check last modified
            }
        }
    }

    async fn expect_directory_empty(client: &Client, directory: &str) {
        let subdir_listed = list_directory(client, directory).await;
        assert_eq!(1, subdir_listed.len());
        expect_directory(&subdir_listed[0], directory)
    }

    async fn expect_file(
        client: &Client,
        entity: &ListEntity,
        name: &str,
        content: &[u8],
        content_type: &str,
    ) {
        match entity {
            reqwest_dav::list_cmd::ListEntity::File(file) => {
                assert_eq!(name, file.href, "File names do not match");
                assert_eq!(
                    content.len() as i64,
                    file.content_length,
                    "File content length does not match"
                );
                assert_eq!(content_type, file.content_type, "File type does not match");
                //TODO: check tag value
                assert_eq!(true, file.tag.is_some(), "File has no tags");
                //TODO: check last modified

                let response = client.get(&name).await.unwrap();
                let response_content = response.bytes().await.unwrap().to_vec();
                assert_eq!(*content, response_content, "File content is wrong");
            }
            reqwest_dav::list_cmd::ListEntity::Folder(_folder) => {
                panic!("Asserting that a folder is a file")
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_file_not_found() {
        let verify_changes = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                {
                    let error = client.get("/test.txt").await.unwrap_err();
                    match error {
                    reqwest_dav::Error::Reqwest(_) | reqwest_dav::Error::ReqwestDecode(_) | reqwest_dav::Error::MissingAuthContext => panic!("Unexpected error: {:?}", &error),
                    reqwest_dav::Error::Decode(decode) => match decode {
                        reqwest_dav::DecodeError::DigestAuth(_) => panic!(),
                        reqwest_dav::DecodeError::NoAuthHeaderInResponse => panic!(),
                        reqwest_dav::DecodeError::SerdeXml(_) => panic!(),
                        reqwest_dav::DecodeError::FieldNotSupported(_) => panic!(),
                        reqwest_dav::DecodeError::FieldNotFound(_) => panic!(),
                        reqwest_dav::DecodeError::StatusMismatched(_) => panic!(),
                        reqwest_dav::DecodeError::Server(server_error) =>
                            assert_eq!("ServerError { response_code: 404, exception: \"server exception and parse error\", message: \"\" }",
                                format!("{:?}", server_error)),
                    },
                };
                }
                let listed = list_directory(&client, "/").await;
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], "/");
            })
        };
        test_fresh_dav_server(None, &verify_changes).await
    }

    async fn test_create_file(content: Vec<u8>) {
        let file_name = "test.txt";
        let content_cloned = content.clone();
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put(file_name, content_cloned).await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            let content_cloned = content.clone();
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_file(
                    &client,
                    &listed[1],
                    &format!("/{}", file_name),
                    &content_cloned,
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_empty() {
        test_create_file(vec![]).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_with_small_content() {
        test_create_file(vec![b'a']).await
    }

    fn random_bytes(len: usize) -> Vec<u8> {
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut small_rng = SmallRng::from_entropy();
        (0..len).map(|_| small_rng.gen()).collect()
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_random_tiny() {
        test_create_file(random_bytes(42)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_random_one_block() {
        test_create_file(random_bytes(VALUE_BLOB_MAX_LENGTH)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_random_two_blocks() {
        test_create_file(random_bytes(VALUE_BLOB_MAX_LENGTH * 2)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_random_ten_blocks() {
        test_create_file(random_bytes(VALUE_BLOB_MAX_LENGTH * 10)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_value_blob_max_length_minus_one() {
        test_create_file(random_bytes(VALUE_BLOB_MAX_LENGTH - 1)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_value_blob_max_length_plus_one() {
        test_create_file(random_bytes(VALUE_BLOB_MAX_LENGTH + 1)).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_200k() {
        test_create_file(std::iter::repeat_n(0u8, 200_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_1_mb() {
        test_create_file(std::iter::repeat_n(0u8, 1_000_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_2_mb() {
        test_create_file(std::iter::repeat_n(0u8, 2_000_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_4_mb() {
        test_create_file(std::iter::repeat_n(0u8, 4_000_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_directory() {
        let dir_name = "Dir4";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol(&dir_name).await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                {
                    let listed = list_directory(&client, "/").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/");
                    expect_directory(&listed[1], &format!("/{}/", dir_name));
                }
                {
                    let listed = list_directory(&client, &format!("/{}/", dir_name)).await;
                    assert_eq!(1, listed.len());
                    expect_directory(&listed[0], &format!("/{}/", dir_name));
                }
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_two_nested_directories() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("a").await.unwrap();
                client.mkcol("a/b").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                {
                    let listed = list_directory(&client, "/").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/");
                    expect_directory(&listed[1], "/a/");
                }
                {
                    let listed = list_directory(&client, "/a").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/a/");
                    expect_directory(&listed[1], "/a/b/");
                }
                {
                    let listed = list_directory(&client, "/a/b").await;
                    assert_eq!(1, listed.len());
                    expect_directory(&listed[0], "/a/b/");
                }
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_three_nested_directories() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("a").await.unwrap();
                client.mkcol("a/b").await.unwrap();
                client.mkcol("a/b/c").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                {
                    let listed = list_directory(&client, "/").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/");
                    expect_directory(&listed[1], "/a/");
                }
                {
                    let listed = list_directory(&client, "/a").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/a/");
                    expect_directory(&listed[1], "/a/b/");
                }
                {
                    let listed = list_directory(&client, "/a/b").await;
                    assert_eq!(2, listed.len());
                    expect_directory(&listed[0], "/a/b/");
                    expect_directory(&listed[1], "/a/b/c/");
                }
                {
                    let listed = list_directory(&client, "/a/b/c").await;
                    assert_eq!(1, listed.len());
                    expect_directory(&listed[0], "/a/b/c/");
                }
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_list_infinity() {
        // WebDAV servers sometimes refuse "depth: infinity" PROPFIND requests. The library we use does this as well.
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                assert_eq!(
                    "reqwest_dav::Error { kind: \"Decode\", source: StatusMismatched(StatusMismatchedError { response_code: 403, expected_code: 207 }) }",
                    format!(
                        "{:?}",
                        &client.list("/", Depth::Infinity).await.unwrap_err()
                    )
                );
            })
        };
        test_fresh_dav_server(None, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_root() {
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                assert_eq!(
                    "reqwest_dav::Error { kind: \"Decode\", source: Server(ServerError { response_code: 403, exception: \"server exception and parse error\", message: \"\" }) }",
                    format!(
                        "{:?}",
                        &client.mv("/", "/test/").await.unwrap_err()));
                let listed = list_directory(&client, "/").await;
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], "/");
            })
        };
        test_fresh_dav_server(None, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_file_to_already_existing_path() {
        let content_a = "test";
        let content_b = "foo";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A", content_a).await.unwrap();
                client.put("B", content_b).await.unwrap();
                client.mv("A", "B").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_file(
                    &client,
                    &listed[1],
                    "/B",
                    content_a.as_bytes(),
                    "application/octet-stream",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_file() {
        let content = "test";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A", content).await.unwrap();
                client.mv("A", "B").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_file(
                    &client,
                    &listed[1],
                    "/B",
                    content.as_bytes(),
                    "application/octet-stream",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_directory() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("A").await.unwrap();
                client.mv("A", "B").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_directory(&listed[1], "/B/");
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_with_different_directories() {
        let content = "test";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("A").await.unwrap();
                client.put("A/foo.txt", content).await.unwrap();
                client.mv("A/foo.txt", "B.txt").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = list_directory(&client, "/").await;
                assert_eq!(3, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_directory(&root_listed[1], "/A/");
                expect_file(
                    &client,
                    &root_listed[2],
                    "/B.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;

                expect_directory_empty(&client, "/A/").await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_with_different_directories_locking() {
        let content = "test";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("A").await.unwrap();
                client.put("A/foo.txt", content).await.unwrap();
                client.mv("A/foo.txt", "B.txt").await.unwrap();
                client.mv("B.txt", "A/foo.txt").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = list_directory(&client, "/").await;
                assert_eq!(2, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_directory(&root_listed[1], "/A/");

                let a_listed = list_directory(&client, "/A/").await;
                assert_eq!(2, a_listed.len());
                expect_directory(&a_listed[0], "/A/");
                expect_file(
                    &client,
                    &a_listed[1],
                    "/A/foo.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_remove_file() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A", "content").await.unwrap();
                client.delete("A").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                expect_directory_empty(&client, "/").await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_remove_directory() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("A").await.unwrap();
                client.delete("A").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                expect_directory_empty(&client, "/").await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_file() {
        let content = "content";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A.txt", content).await.unwrap();
                client.cp("A.txt", "B.txt").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(3, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_file(
                    &client,
                    &root_listed[1],
                    "/A.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
                expect_file(
                    &client,
                    &root_listed[2],
                    "/B.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_file_independent_content() {
        let content_1 = "1";
        let content_2 = "2";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A.txt", content_1).await.unwrap();
                client.cp("A.txt", "B.txt").await.unwrap();
                client.put("A.txt", content_2).await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(3, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_file(
                    &client,
                    &root_listed[1],
                    "/A.txt",
                    content_2.as_bytes(),
                    "text/plain",
                )
                .await;
                expect_file(
                    &client,
                    &root_listed[2],
                    "/B.txt",
                    content_1.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_file_into_different_folder() {
        let content = "content";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A.txt", content).await.unwrap();
                client.mkcol("/foo").await.unwrap();
                client.cp("A.txt", "/foo/B.txt").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(3, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_file(
                    &client,
                    &root_listed[1],
                    "/A.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;

                let foo_listed = client.list("/foo", Depth::Number(1)).await.unwrap();
                expect_directory(&foo_listed[0], "/foo/");
                expect_file(
                    &client,
                    &foo_listed[1],
                    "/foo/B.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_file_to_already_existing_target() {
        let content = "content";
        let other_content = "some other content";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A.txt", content).await.unwrap();
                client.mkcol("/foo").await.unwrap();
                client.put("/foo/B.txt", other_content).await.unwrap();

                client.cp("A.txt", "/foo/B.txt").await.unwrap();
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(3, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_file(
                    &client,
                    &root_listed[1],
                    "/A.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;

                let foo_listed = client.list("/foo", Depth::Number(1)).await.unwrap();
                expect_directory(&foo_listed[0], "/foo/");
                expect_file(
                    &client,
                    &foo_listed[1],
                    "/foo/B.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_file_to_itself() {
        let content = "content";
        let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.put("A.txt", content).await.unwrap();
                match client.cp("A.txt", "A.txt").await {
                    Ok(_) => {
                        panic!("The request should have failed");
                    }
                    Err(err) => match err {
                        reqwest_dav::Error::Reqwest(_error) => {
                            panic!("Expecting a different error")
                        }
                        reqwest_dav::Error::ReqwestDecode(_reqwest_decode_error) => {
                            panic!("The request failed decoding")
                        }
                        reqwest_dav::Error::Decode(decode_error) => {
                            match decode_error {
                                reqwest_dav::DecodeError::DigestAuth(_error) => {
                                    panic!("DigestAuth error")
                                }
                                reqwest_dav::DecodeError::NoAuthHeaderInResponse => {
                                    panic!("No auth header in response")
                                }
                                reqwest_dav::DecodeError::SerdeXml(_error) => {
                                    panic!("XML decoding error")
                                }
                                reqwest_dav::DecodeError::FieldNotSupported(field_error) => {
                                    panic!("{:?}", field_error)
                                }
                                reqwest_dav::DecodeError::FieldNotFound(field_error) => {
                                    panic!("{:?}", field_error)
                                }
                                reqwest_dav::DecodeError::StatusMismatched(
                                    status_mismatched_error,
                                ) => panic!("{:?}", status_mismatched_error),
                                reqwest_dav::DecodeError::Server(_server_error) => {}
                            };
                        }
                        reqwest_dav::Error::MissingAuthContext => {
                            panic!("The request failed decoding")
                        }
                    },
                };
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let root_listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(2, root_listed.len());
                expect_directory(&root_listed[0], "/");
                expect_file(
                    &client,
                    &root_listed[1],
                    "/A.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_copy_non_existing_file() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                client.mkcol("/foo").await.unwrap();
                match client.cp("A.txt", "/foo/B.txt").await {
                    Ok(_) => {
                        panic!("The request should have failed");
                    }
                    Err(err) => match err {
                        reqwest_dav::Error::Reqwest(error) => {
                            assert_eq!(error.status().unwrap(), 404)
                        }
                        reqwest_dav::Error::ReqwestDecode(_reqwest_decode_error) => {
                            panic!("The request failed decoding")
                        }
                        reqwest_dav::Error::Decode(_decode_error) => {
                            print!("{:?}", _decode_error)
                        }
                        reqwest_dav::Error::MissingAuthContext => {
                            panic!("The request failed decoding")
                        }
                    },
                };
            })
        };
        let verify_changes =
            move |_client: Client| -> Pin<Box<dyn Future<Output = ()>>> { Box::pin(async move {}) };
        test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let address = SocketAddr::from(([0, 0, 0, 0], 4918));
    let database_file_name = std::env::current_dir()
        .unwrap()
        .join("dogbox_dav_server.sqlite");
    let listener = TcpListener::bind(address).await?;
    info!("Serving on http://{}", address);
    let clock = std::time::SystemTime::now;
    let modified_default = clock();
    info!(
        "Last modification time defaults to {:#?}",
        &modified_default
    );
    let (mut save_status_receiver, server) = run_dav_server(
        listener,
        &database_file_name,
        modified_default,
        clock,
        std::time::Duration::from_secs(10),
    )
    .await?;
    tokio::try_join!(server, async move {
        while let Some(status) = save_status_receiver.recv().await {
            info!("Save status: {:?}", status);
        }
        Ok(())
    })?;
    Ok(())
}
