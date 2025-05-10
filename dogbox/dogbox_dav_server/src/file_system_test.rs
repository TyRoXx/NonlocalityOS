use crate::file_system::{DogBoxFileSystem, DogBoxOpenFile};
use astraea::{storage::InMemoryTreeStorage, tree::BlobDigest};
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
    sync::Arc,
};
use tokio::{net::TcpListener, sync::Mutex};
use tracing::info;

fn test_clock() -> std::time::SystemTime {
    std::time::SystemTime::UNIX_EPOCH
}

#[test_log::test(tokio::test)]
async fn test_dav_access() {
    let blob_storage = Arc::new(InMemoryTreeStorage::new(Mutex::new(BTreeMap::new())));
    let dav_server = DavHandler::builder()
        .filesystem(Box::new(DogBoxFileSystem::new(
            dogbox_tree_editor::TreeEditor::new(
                Arc::new(
                    OpenDirectory::create_directory(blob_storage, test_clock, 1)
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
    info!("Serving on http://{}", actual_address);

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
                    info!("Error serving connection: {:?}", err);
                }
            });
        }
    };

    let server_url = format!("http://{actual_address}");
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
            panic!("Server isn't expected to exit: {result:?}");
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
    let storage = Arc::new(InMemoryTreeStorage::empty());
    {
        let handle = Arc::new(OpenFile::new(
            dogbox_tree_editor::OpenFileContentBuffer::from_data(
                data,
                last_known_digest,
                last_known_digest_file_size,
                1,
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
    assert_eq!(BTreeSet::new(), storage.digests().await);
}

#[test_log::test(tokio::test)]
async fn test_seek_and_write() {
    let data = Vec::new();
    let last_known_digest = BlobDigest::hash(&data);
    let last_known_digest_file_size = data.len() as u64;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    {
        let handle = Arc::new(OpenFile::new(
            dogbox_tree_editor::OpenFileContentBuffer::from_data(
                data,
                last_known_digest,
                last_known_digest_file_size,
                1,
            )
            .unwrap(),
            storage.clone(),
            test_clock(),
        ));
        let mut file = DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);
        file.write_bytes(bytes::Bytes::from("test")).await.unwrap();
        let new_size = 4;
        assert_eq!(new_size, file.seek(SeekFrom::Current(0)).await.unwrap());
        assert_eq!(new_size, handle.size().await);

        file.flush().await.unwrap();
        let expected_digests = BTreeSet::from_iter(
            [
                concat!(
                    "4f10e21ad7ef2048c73b1e1cd2b1d62b76cbf1240adbada00396aec2718fc897",
                    "7258b448cf20d89ccd6534ca1d216e8f3cbff20ada9e7374c47af42fed87b71d"
                ),
                concat!(
                    "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
                    "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
                ),
            ]
            .map(BlobDigest::parse_hex_string)
            .map(Option::unwrap),
        );
        assert_eq!(expected_digests, storage.digests().await);

        assert_eq!(1, file.seek(SeekFrom::Current(-3)).await.unwrap());
        file.write_bytes(bytes::Bytes::from("E")).await.unwrap();
        assert_eq!(2, file.seek(SeekFrom::Current(0)).await.unwrap());
        assert_eq!(new_size, handle.size().await);
        file.flush().await.unwrap();
        let expected_digests = BTreeSet::from_iter(
            [
                concat!(
                    "636b0957579730d0588aac37a91d1bc6abb6f67553ed14d78cfdf6f094680690",
                    "9ac563076350dea163f955cf785e13d241fe5850774c8bacb34a7f87c05d338c"
                ),
                concat!(
                    "4f10e21ad7ef2048c73b1e1cd2b1d62b76cbf1240adbada00396aec2718fc897",
                    "7258b448cf20d89ccd6534ca1d216e8f3cbff20ada9e7374c47af42fed87b71d"
                ),
                concat!(
                    "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
                    "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
                ),
            ]
            .map(BlobDigest::parse_hex_string)
            .map(Option::unwrap),
        );
        assert_eq!(expected_digests, storage.digests().await);
    }
}

#[test_log::test(tokio::test)]
async fn test_seek_beyond_the_end() {
    let data = Vec::new();
    let last_known_digest = BlobDigest::hash(&data);
    let last_known_digest_file_size = data.len() as u64;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    {
        let handle = Arc::new(OpenFile::new(
            dogbox_tree_editor::OpenFileContentBuffer::from_data(
                data,
                last_known_digest,
                last_known_digest_file_size,
                1,
            )
            .unwrap(),
            storage.clone(),
            test_clock(),
        ));
        let mut file = DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);
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

        file.flush().await.unwrap();
        let expected_digests = BTreeSet::from_iter(
            [
                concat!(
                    "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
                    "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
                ),
                concat!(
                    "33cb1f71a103e2bf443c27930a2fb9871b028a614de0c3acb3cc486074ef9dbd",
                    "d1b4b776727729f4708548185682be845d1b081243105fff5bd4e11bea5fed6f"
                ),
                concat!(
                    "708d4258f26a6d99a6cc10532bd66134f46fd537d51db057c5a083cf8994f07e",
                    "740534b6f795c49aa35513a65e3da7a5518fe163da200e24af0701088b290daa"
                ),
                concat!(
                    "053449bd3fcab54840b5d0ca72dceaa77446d6980d52a54f21ac8f6157e3f8f",
                    "3748f87fbccb5d5071a6d95098468a1c50db64767963066803dca6a8083eb32a8"
                ),
            ]
            .map(BlobDigest::parse_hex_string)
            .map(Option::unwrap),
        );
        assert_eq!(expected_digests, storage.digests().await);

        file.flush().await.unwrap();
        assert_eq!(expected_digests, storage.digests().await);
    }
}

#[test_log::test(tokio::test)]
async fn test_write_out_of_bounds() {
    let data = Vec::new();
    let last_known_digest = BlobDigest::hash(&data);
    let last_known_digest_file_size = data.len() as u64;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    {
        let handle = Arc::new(OpenFile::new(
            dogbox_tree_editor::OpenFileContentBuffer::from_data(
                data,
                last_known_digest,
                last_known_digest_file_size,
                1,
            )
            .unwrap(),
            storage.clone(),
            test_clock(),
        ));
        let mut file = DogBoxOpenFile::new(handle.clone(), Some(handle.get_write_permission()), 0);

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
        assert_eq!(BTreeSet::new(), storage.digests().await);

        file.flush().await.unwrap();
        let expected_digests = BTreeSet::from_iter(
            [concat!(
                "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
                "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
            )]
            .map(BlobDigest::parse_hex_string)
            .map(Option::unwrap),
        );
        assert_eq!(expected_digests, storage.digests().await);
    }
}
