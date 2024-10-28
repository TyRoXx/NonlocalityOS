#[cfg(test)]
mod tests {
    use crate::file_system::DogBoxFileSystem;
    use astraea::storage::InMemoryValueStorage;
    use dav_server::{fakels::FakeLs, DavHandler};
    use dogbox_tree_editor::OpenDirectory;
    use hyper::{body, server::conn::http1, Request};
    use hyper_util::rt::TokioIo;
    use reqwest_dav::{Auth, ClientBuilder, Depth};
    use std::{
        collections::BTreeMap,
        convert::Infallible,
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

        let address = SocketAddr::from(([127, 0, 0, 1], 4919));
        let listener = TcpListener::bind(address).await.unwrap();
        println!("Serving on http://{}", address);

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

        let server_url = format!("http://{}", address);
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
}
