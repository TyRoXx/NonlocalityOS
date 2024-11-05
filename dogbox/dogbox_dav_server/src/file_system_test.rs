#[cfg(test)]
mod tests {
    use crate::file_system::{DogBoxFileSystem, DogBoxOpenFile};
    use astraea::{storage::InMemoryValueStorage, tree::BlobDigest};
    use dav_server::{fakels::FakeLs, fs::DavFile, DavHandler};
    use dogbox_tree_editor::{OpenDirectory, OpenFile};
    use hyper::{body, server::conn::http1, Request};
    use hyper_util::rt::TokioIo;
    use pretty_assertions::assert_eq;
    use reqwest_dav::{Auth, ClientBuilder, Depth};
    use std::{
        collections::{BTreeMap, BTreeSet},
        convert::Infallible,
        io::SeekFrom,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };
    use tokio::net::TcpListener;

    fn test_clock() -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH
    }

    #[test_log::test(tokio::test)]
    async fn test_dav_access() {
        let blob_storage = Arc::new(InMemoryValueStorage::new(Mutex::new(BTreeMap::new())));
        let dav_server = DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(
                dogbox_tree_editor::TreeEditor::new(
                    Arc::new(
                        OpenDirectory::create_directory(blob_storage, test_clock)
                            .await
                            .unwrap(),
                    ),
                    None,
                ),
            )))
            .locksystem(FakeLs::new())
            .build_handler();

        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(address).await.unwrap();
        let actual_address = listener.local_addr().unwrap();
        println!("Serving on http://{}", actual_address);

        let serve = || async {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let io = TokioIo::new(stream);
                let dav_server = dav_server.clone();
                tokio::task::spawn(async move {
                    let make_service = move |req: Request<body::Incoming>| {
                        let dav_server = dav_server.clone();
                        async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
                    };

                    // Finally, we bind the incoming connection to our `hello` service
                    if let Err(err) = http1::Builder::new()
                        // `service_fn` converts our function in a `Service`
                        .serve_connection(io, hyper::service::service_fn(make_service))
                        .await
                    {
                        eprintln!("Error serving connection: {:?}", err);
                    }
                });
            }
        };

        let server_url = format!("http://{}", actual_address);
        let run_client = async {
            let client = ClientBuilder::new()
                .set_host(server_url)
                .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
                .build()
                .unwrap();

            match client.get("/test.txt").await.unwrap_err() {
                reqwest_dav::Error::Reqwest(_) => panic!(),
                reqwest_dav::Error::ReqwestDecode(_) => panic!(),
                reqwest_dav::Error::MissingAuthContext => panic!(),
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

            let listed = client.list("", Depth::Number(1)).await.unwrap();
            assert_eq!(1, listed.len());
            let entry = &listed[0];
            match entry {
                reqwest_dav::list_cmd::ListEntity::File(_) => panic!(),
                reqwest_dav::list_cmd::ListEntity::Folder(folder) => {
                    assert_eq!("/", folder.href);
                    assert_eq!(None, folder.quota_used_bytes);
                    assert_eq!(None, folder.quota_available_bytes);
                    //TODO: check tag value
                    assert_eq!(true, folder.tag.is_some());
                    //TODO: check last modified
                }
            }
        };
        tokio::select! {
            result = serve() => {
                panic!("Server isn't expected to exit: {:?}", result);
            }
            _ = run_client => {
            }
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_seek_operations() {
        let data = Vec::new();
        let last_known_digest = BlobDigest::hash(&data);
        let last_known_digest_file_size = data.len() as u64;
        let storage = Arc::new(InMemoryValueStorage::empty());
        {
            let handle = Arc::new(OpenFile::new(
                dogbox_tree_editor::OpenFileContentBuffer::from_data(
                    data,
                    last_known_digest,
                    last_known_digest_file_size,
                )
                .unwrap(),
                storage.clone(),
                test_clock(),
            ));
            let mut file = DogBoxOpenFile::new(handle, None, 0);
            assert_eq!(0, file.seek(SeekFrom::Start(0)).await.unwrap());
            assert_eq!(1, file.seek(SeekFrom::Start(1)).await.unwrap());
            assert_eq!(2, file.seek(SeekFrom::Current(1)).await.unwrap());
            assert_eq!(1, file.seek(SeekFrom::Current(-1)).await.unwrap());
            assert_eq!(1, file.seek(SeekFrom::End(1)).await.unwrap());
            assert_eq!(0, file.seek(SeekFrom::End(-1)).await.unwrap());
            assert_eq!(
                u64::MAX,
                file.seek(SeekFrom::Start(u64::MAX)).await.unwrap()
            );
            assert_eq!(
                (u64::MAX - 1),
                file.seek(SeekFrom::Current(-1)).await.unwrap()
            );
            assert_eq!(u64::MAX, file.seek(SeekFrom::Current(2)).await.unwrap());
            assert_eq!(
                u64::MAX,
                file.seek(SeekFrom::Current(i64::MAX)).await.unwrap()
            );
            assert_eq!(
                9223372036854775807,
                file.seek(SeekFrom::Current(i64::MIN)).await.unwrap()
            );
            assert_eq!(0, file.seek(SeekFrom::Current(i64::MIN)).await.unwrap());
        }
        assert_eq!(BTreeSet::new(), storage.digests());
    }

    #[test_log::test(tokio::test)]
    async fn test_seek_and_write() {
        let data = Vec::new();
        let last_known_digest = BlobDigest::hash(&data);
        let last_known_digest_file_size = data.len() as u64;
        let storage = Arc::new(InMemoryValueStorage::empty());
        {
            let handle = Arc::new(OpenFile::new(
                dogbox_tree_editor::OpenFileContentBuffer::from_data(
                    data,
                    last_known_digest,
                    last_known_digest_file_size,
                )
                .unwrap(),
                storage.clone(),
                test_clock(),
            ));
            let mut file =
                DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);
            file.write_bytes(bytes::Bytes::from("test")).await.unwrap();
            let new_size = 4;
            assert_eq!(new_size, file.seek(SeekFrom::Current(0)).await.unwrap());
            assert_eq!(new_size, handle.size().await);
            assert_eq!(BTreeSet::new(), storage.digests());

            assert_eq!(1, file.seek(SeekFrom::Current(-3)).await.unwrap());
            file.write_bytes(bytes::Bytes::from("E")).await.unwrap();
            assert_eq!(2, file.seek(SeekFrom::Current(0)).await.unwrap());
            assert_eq!(new_size, handle.size().await);
            assert_eq!(BTreeSet::new(), storage.digests());

            file.flush().await.unwrap();
            // cargo fmt silently refuses to format this for an unknown reason:
            let expected_digests =
            BTreeSet::from_iter ([
                "b200e4afa7118a3d238d374dd657cc9bf667634e9f811dc5db071ae26e1b7b43ae085c659946f7d46c20a802d94a327ddc53ae5d11970e34d9dc68ae4da76be3"
          ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
            assert_eq!(expected_digests, storage.digests());
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_seek_beyond_the_end() {
        let data = Vec::new();
        let last_known_digest = BlobDigest::hash(&data);
        let last_known_digest_file_size = data.len() as u64;
        let storage = Arc::new(InMemoryValueStorage::empty());
        {
            let handle = Arc::new(OpenFile::new(
                dogbox_tree_editor::OpenFileContentBuffer::from_data(
                    data,
                    last_known_digest,
                    last_known_digest_file_size,
                )
                .unwrap(),
                storage.clone(),
                test_clock(),
            ));
            let mut file =
                DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);
            assert_eq!(
                1_000_000,
                file.seek(SeekFrom::Start(1_000_000)).await.unwrap()
            );
            let write_data = "test";
            file.write_bytes(bytes::Bytes::from(write_data))
                .await
                .unwrap();
            let new_size = 1_000_000 + write_data.len() as u64;
            assert_eq!(new_size, file.seek(SeekFrom::Current(0)).await.unwrap());
            assert_eq!(new_size, handle.size().await);
            assert_eq!(BTreeSet::new(), storage.digests());

            file.flush().await.unwrap();
            // cargo fmt silently refuses to format this for an unknown reason:
            let expected_digests =
            BTreeSet::from_iter ([
                "66d414061d3fea735e6e9e1cc7fe9cc68e89a46ab46c4a2aaa07d15c093cb8953e20a6552604a3f4875d7c53ead8ce64447242719dad24eac781feccbf67aca6",
        "36708536177e3b63fe3cc7a9ab2e93c26394d2e00933b243c9f3ab93c245a8253a731314365fbd5094ad33d64a083bf1b63b8471c55aab7a7efb4702d7e75459"
        ,           "f38a4f0c3e8e5eec4322ad6c1b4718f7731db33e5af24bd1acf660e8685056b84d9d654a473ab558fc7b32c1a9cbafa61a471ed887b51b511f804a93e3bf2097"
        ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
            assert_eq!(expected_digests, storage.digests());
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_write_out_of_bounds() {
        let data = Vec::new();
        let last_known_digest = BlobDigest::hash(&data);
        let last_known_digest_file_size = data.len() as u64;
        let storage = Arc::new(InMemoryValueStorage::empty());
        {
            let handle = Arc::new(OpenFile::new(
                dogbox_tree_editor::OpenFileContentBuffer::from_data(
                    data,
                    last_known_digest,
                    last_known_digest_file_size,
                )
                .unwrap(),
                storage.clone(),
                test_clock(),
            ));
            let mut file =
                DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);

            assert_eq!(
                i64::MAX as u64,
                file.seek(SeekFrom::Current(i64::MAX)).await.unwrap()
            );
            let cursor_before_write = (i64::MAX as u64) * 2;
            assert_eq!(
                cursor_before_write,
                file.seek(SeekFrom::Current(i64::MAX)).await.unwrap()
            );
            assert_eq!(
                Err(dav_server::fs::FsError::TooLarge),
                file.write_bytes(bytes::Bytes::from("test")).await
            );
            assert_eq!(
                cursor_before_write,
                file.seek(SeekFrom::Current(0)).await.unwrap()
            );
            assert_eq!(last_known_digest_file_size, handle.size().await);
            assert_eq!(BTreeSet::new(), storage.digests());

            file.flush().await.unwrap();
            // cargo fmt silently refuses to format this for an unknown reason:
            let expected_digests =
            BTreeSet::from_iter ([
                "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26"
          ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
            assert_eq!(expected_digests, storage.digests());
        }
    }
}
