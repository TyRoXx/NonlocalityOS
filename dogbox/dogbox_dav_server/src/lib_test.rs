use crate::run_dav_server;
use astraea::tree::TREE_BLOB_MAX_LENGTH;
use dogbox_tree_editor::WallClock;
use pretty_assertions::assert_eq;
use reqwest_dav::{list_cmd::ListEntity, Auth, Client, ClientBuilder, Depth};
use std::{future::Future, net::SocketAddr, pin::Pin};
use tokio::net::TcpListener;
use tracing::info;

type UnitFuture<'t> = Pin<Box<dyn Future<Output = ()> + 't>>;
type ChangeFilesFunction<'t> = Box<dyn FnOnce(Client) -> UnitFuture<'t>>;

async fn run_dav_server_instance<'t>(
    database_file_name: &std::path::Path,
    change_files: Option<ChangeFilesFunction<'t>>,
    is_saving_expected: bool,
    verify_changes: &impl Fn(Client) -> Pin<Box<dyn Future<Output = ()> + 't>>,
    modified_default: std::time::SystemTime,
    clock: WallClock,
) {
    let address = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = TcpListener::bind(address).await.unwrap();
    let actual_address = listener.local_addr().unwrap();
    let server_url = format!("http://{actual_address}");
    let (mut save_status_receiver, server, root_directory) = run_dav_server(
        listener,
        database_file_name,
        modified_default,
        clock,
        // don't waste time with the tests (more than 0 seconds to avoid wasting too many CPU cycles)
        std::time::Duration::from_millis(1),
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
    let waiting_for_saved_status = async {
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
                            let root_status = root_directory.latest_status();
                            assert!(root_status.digest.is_digest_up_to_date);
                            assert_eq!(0, root_status.bytes_unflushed_count);
                            assert_eq!(0, root_status.files_unflushed_count);
                            assert!(root_status.directories_open_count >= 1);
                            assert_eq!(0, root_status.directories_unsaved_count);
                            //TODO: can we somehow wait for files to be closed?
                            //assert_eq!(0, root_status.files_open_count);
                            assert_eq!(0, root_status.files_open_for_writing_count);
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
            let root_status = root_directory.latest_status();
            assert!(root_status.digest.is_digest_up_to_date);
            assert_eq!(0, root_status.bytes_unflushed_count);
            assert_eq!(0, root_status.files_unflushed_count);
            assert!(root_status.directories_open_count >= 1);
            assert_eq!(0, root_status.directories_unsaved_count);
            //TODO: can we somehow wait for files to be closed?
            //assert_eq!(0, root_status.files_open_count);
            assert_eq!(0, root_status.files_open_for_writing_count);
        }
    };
    let testing = async {
        client_side_testing.await;
        waiting_for_saved_status.await;
    };
    tokio::select! {
        result = server => {
            panic!("Server isn't expected to exit: {result:?}");
        }
        _ = testing => {
        }
    };
}

async fn test_fresh_dav_server<'t>(
    change_files: Option<ChangeFilesFunction<'t>>,
    verify_changes: &impl Fn(Client) -> UnitFuture<'t>,
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
    ClientBuilder::new()
        .set_host(server_url)
        .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
        .build()
        .unwrap()
}

fn expect_directory(entity: &ListEntity, name: &str) {
    match entity {
        reqwest_dav::list_cmd::ListEntity::File(_) => panic!(),
        reqwest_dav::list_cmd::ListEntity::Folder(folder) => {
            assert_eq!(name, folder.href);
            assert_eq!(None, folder.quota_used_bytes);
            assert_eq!(None, folder.quota_available_bytes);
            //TODO: check tag value
            assert!(folder.tag.is_some());
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
            assert!(file.tag.is_some(), "File has no tags");
            //TODO: check last modified

            let response = client.get(name).await.unwrap();
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
                                format!("{server_error:?}")),
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
                &format!("/{file_name}"),
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
    let mut small_rng = SmallRng::seed_from_u64(123);
    (0..len).map(|_| small_rng.random()).collect()
}

#[test_log::test(tokio::test)]
async fn test_create_file_random_tiny() {
    test_create_file(random_bytes(42)).await
}

#[test_log::test(tokio::test)]
async fn test_create_file_tree_blob_max_length_minus_one() {
    test_create_file(random_bytes(TREE_BLOB_MAX_LENGTH - 1)).await
}

#[test_log::test(tokio::test)]
async fn test_create_file_tree_blob_max_length_plus_one() {
    test_create_file(random_bytes(TREE_BLOB_MAX_LENGTH + 1)).await
}

#[test_log::test(tokio::test)]
async fn test_create_file_random_100k() {
    test_create_file(random_bytes(100_000)).await
}

#[test_log::test(tokio::test)]
async fn test_create_file_truncate() {
    let file_name = "test.txt";
    let long_content = "looooooong";
    let short_content = "short";
    let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            client.put(file_name, long_content).await.unwrap();
            assert!(
                short_content.len() < long_content.len(),
                "Test is not valid, short content is not shorter than long content"
            );
            client.put(file_name, short_content).await.unwrap();
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
                &format!("/{file_name}"),
                short_content.as_bytes(),
                "text/plain",
            )
            .await;
        })
    };
    test_fresh_dav_server(Some(Box::new(change_files)), &verify_changes).await
}

#[test_log::test(tokio::test)]
async fn test_create_directory() {
    let dir_name = "Dir4";
    let change_files = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            client.mkcol(dir_name).await.unwrap();
        })
    };
    let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            {
                let listed = list_directory(&client, "/").await;
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_directory(&listed[1], &format!("/{dir_name}/"));
            }
            {
                let listed = list_directory(&client, &format!("/{dir_name}/")).await;
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], &format!("/{dir_name}/"));
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

            // We need this extra file so that the state of the directory actually changes.
            client.put("B.txt", "").await.unwrap();
        })
    };
    let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            let listed = list_directory(&client, "/").await;
            assert_eq!(2, listed.len());
            expect_directory(&listed[0], "/");
            expect_file(&client, &listed[1], "/B.txt", "".as_bytes(), "text/plain").await;
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

            // We need this extra file so that the state of the directory actually changes.
            client.put("B.txt", "").await.unwrap();
        })
    };
    let verify_changes = move |client: Client| -> Pin<Box<dyn Future<Output = ()>>> {
        Box::pin(async move {
            let listed = list_directory(&client, "/").await;
            assert_eq!(2, listed.len());
            expect_directory(&listed[0], "/");
            expect_file(&client, &listed[1], "/B.txt", "".as_bytes(), "text/plain").await;
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
                                panic!("{field_error:?}")
                            }
                            reqwest_dav::DecodeError::FieldNotFound(field_error) => {
                                panic!("{field_error:?}")
                            }
                            reqwest_dav::DecodeError::StatusMismatched(status_mismatched_error) => {
                                panic!("{status_mismatched_error:?}")
                            }
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
                        print!("{_decode_error:?}")
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
