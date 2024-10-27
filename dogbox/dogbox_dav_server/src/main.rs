use astraea::storage::{LoadRoot, SQLiteStorage, UpdateRoot};
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{OpenDirectory, OpenDirectoryStatus, WallClock};
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    net::SocketAddr,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};
use tracing::info;
mod file_system;
mod file_system_test;
use file_system::DogBoxFileSystem;

async fn serve_connection(stream: TcpStream, dav_server: Arc<DavHandler>) {
    let make_service = move |req: Request<body::Incoming>| {
        info!("Request: {:?}", &req);
        let dav_server = dav_server.clone();
        async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
    };
    let io = TokioIo::new(stream);
    if let Err(err) = http1::Builder::new()
        .serve_connection(io, hyper::service::service_fn(make_service))
        .await
    {
        eprintln!("Error serving connection: {:?}", err);
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

#[derive(Debug)]
enum SaveStatus {
    Saved,
    Saving,
}

async fn save_tree_regularly(
    root: Arc<OpenDirectory>,
    root_name: &str,
    blob_storage: &(dyn UpdateRoot + Sync),
    save_status_sender: tokio::sync::watch::Sender<SaveStatus>,
) {
    let mut previous_root_status: Option<OpenDirectoryStatus> = None;
    let mut number_of_no_changes_in_a_row: u64 = 0;
    loop {
        let (maybe_status, change_event_future) = root.wait_for_next_change().await;
        match maybe_status {
            Ok(root_status) => {
                if previous_root_status.as_ref() == Some(&root_status) {
                    println!("Root didn't change");
                    number_of_no_changes_in_a_row += 1;
                    assert_ne!(10, number_of_no_changes_in_a_row);
                } else {
                    println!("Root changed: {:?}", &root_status);
                    number_of_no_changes_in_a_row = 0;
                    blob_storage.update_root(root_name, &root_status.digest);
                    save_status_sender
                        .send(match root_status.files_unflushed_count {
                            0 => SaveStatus::Saved,
                            _ => SaveStatus::Saving,
                        })
                        .unwrap();
                    previous_root_status = Some(root_status);
                }
            }
            Err(error) => {
                println!("Could not poll root status: {:?}", &error);
            }
        }
        match change_event_future.await {
            Ok(_) => {
                println!("Detected a change event!");
            }
            Err(error) => {
                println!("Could not wait for change event: {:?}", &error);
            }
        }
    }
}

async fn run_dav_server(
    listener: TcpListener,
    database_file_name: &Path,
    modified_default: std::time::SystemTime,
    clock: WallClock,
) -> Result<
    (
        tokio::sync::watch::Receiver<SaveStatus>,
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
    let blob_storage = Arc::new(SQLiteStorage::new(Mutex::new(sqlite_connection)));
    let root_name = "latest";
    let root = match blob_storage.load_root(&root_name) {
        Some(found) => {
            OpenDirectory::load_directory(blob_storage.clone(), &found, modified_default, clock).await.unwrap(/*TODO*/)
        }
        None => Arc::new(OpenDirectory::from_entries(
            vec![],
            blob_storage.clone(),
            modified_default,
            clock,
        )),
    };
    let dav_server = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(
                dogbox_tree_editor::TreeEditor::new(root.clone()),
            )))
            .locksystem(FakeLs::new())
            .build_handler(),
    );
    let (save_status_sender, save_status_receiver) = tokio::sync::watch::channel(SaveStatus::Saved);
    let result = async move {
        tokio::join!(
            async move {
                save_tree_regularly(root, &root_name, &*blob_storage, save_status_sender).await;
            },
            handle_tcp_connections(listener, dav_server)
        )
        .1
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
        change_files: impl FnOnce(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
        verify_changes: &impl Fn(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
        modified_default: std::time::SystemTime,
        clock: WallClock,
    ) {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(address).await.unwrap();
        let actual_address = listener.local_addr().unwrap();
        let server_url = format!("http://{}", actual_address);
        let (mut save_status_receiver, server) =
            run_dav_server(listener, &database_file_name, modified_default, clock)
                .await
                .unwrap();
        let client_side_testing = async move {
            change_files(create_client(server_url.clone())).await;
            verify_changes(create_client(server_url.clone())).await;
            // verify again to be extra sure this is deterministic
            verify_changes(create_client(server_url)).await;
        };
        let waiting_for_saved = async {
            info!("Waiting for the save status to become saved.");
            save_status_receiver
                .wait_for(|status| match status {
                    crate::SaveStatus::Saved => true,
                    crate::SaveStatus::Saving => false,
                })
                .await
                .unwrap();
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
        change_files: impl FnOnce(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
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
        run_dav_server_instance(
            &database_file_name,
            change_files,
            &verify_changes,
            modified_default,
            clock,
        )
        .await;

        // Start a new instance with the database from the first instance to check if the data was persisted correctly.
        info!("Second test server instance");
        assert!(std::fs::exists(&database_file_name).unwrap());
        run_dav_server_instance(
            &database_file_name,
            |_client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
                Box::pin(async move { /* no changes */ })
            },
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
        client: Client,
        entity: &ListEntity,
        name: &str,
        content: &[u8],
        content_type: &str,
    ) {
        match entity {
            reqwest_dav::list_cmd::ListEntity::File(file) => {
                assert_eq!(name, file.href);
                assert_eq!(content.len() as i64, file.content_length);
                assert_eq!(content_type, file.content_type);
                //TODO: check tag value
                assert_eq!(true, file.tag.is_some());
                //TODO: check last modified

                let response = client.get(&name).await.unwrap();
                let response_content = response.bytes().await.unwrap().to_vec();
                assert_eq!(*content, response_content);
            }
            reqwest_dav::list_cmd::ListEntity::Folder(_folder) => panic!(),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_file_not_found() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
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
            })
        };
        let verify_changes = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], "/");
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    async fn test_create_file(content: Vec<u8>) {
        let file_name = "test.txt";
        let content_cloned = content.clone();
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
                    client,
                    &listed[1],
                    &format!("/{}", file_name),
                    &content_cloned,
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
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
    async fn test_create_file_1_mb() {
        test_create_file(std::iter::repeat_n(0u8, 1_000_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_2_mb() {
        test_create_file(std::iter::repeat_n(0u8, 2_000_000).collect()).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_directory() {
        let dir_name = "Dir4";
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_directories() {
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
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_list_infinity() {
        // WebDAV servers sometimes refuse "depth: infinity" PROPFIND requests. The library we use does this as well.
        let change_files =
            |_client: Client| -> Pin<Box<dyn Future<Output = ()>>> { Box::pin(async move {}) };
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
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_root() {
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                assert_eq!(
                    "reqwest_dav::Error { kind: \"Decode\", source: Server(ServerError { response_code: 403, exception: \"server exception and parse error\", message: \"\" }) }",
                    format!(
                        "{:?}",
                        &client.mv("/", "/test/").await.unwrap_err()));
            })
        };
        let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let listed = list_directory(&client, "/").await;
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], "/");
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_file_to_already_existing_path() {
        let content_a = "test";
        let content_b = "foo";
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
                    client,
                    &listed[1],
                    "/B",
                    content_a.as_bytes(),
                    "application/octet-stream",
                )
                .await;
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_file() {
        let content = "test";
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
                    client,
                    &listed[1],
                    "/B",
                    content.as_bytes(),
                    "application/octet-stream",
                )
                .await;
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
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
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_with_different_directories() {
        let content = "test";
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
                    client.clone(),
                    &root_listed[2],
                    "/B.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;

                expect_directory_empty(&client, "/A/").await;
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
    }

    #[test_log::test(tokio::test)]
    async fn test_rename_with_different_directories_locking() {
        let content = "test";
        let change_files = |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
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
                    client,
                    &a_listed[1],
                    "/A/foo.txt",
                    content.as_bytes(),
                    "text/plain",
                )
                .await;
            })
        };
        test_fresh_dav_server(change_files, &verify_changes).await
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
        test_fresh_dav_server(change_files, &verify_changes).await
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
        test_fresh_dav_server(change_files, &verify_changes).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let address = SocketAddr::from(([127, 0, 0, 1], 4918));
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
    let (_save_status_receiver, server) =
        run_dav_server(listener, &database_file_name, modified_default, clock).await?;
    server.await?;
    Ok(())
}
