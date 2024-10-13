use astraea::tree::InMemoryValueStorage;
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{DirectoryEntry, DirectoryEntryKind};
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;
mod file_system;
mod file_system_test;
use file_system::DogBoxFileSystem;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    // TODO: persistance
    let blob_storage = Arc::new(InMemoryValueStorage::new(Mutex::new(BTreeMap::new())));
    let dav_server = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(
                dogbox_tree_editor::TreeEditor::from_entries(
                    vec![DirectoryEntry {
                        name: "example.txt".to_string(),
                        kind: DirectoryEntryKind::File(0),
                    }],
                    blob_storage,
                ),
            )))
            .locksystem(FakeLs::new())
            .build_handler(),
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], 4918));
    let listener = TcpListener::bind(addr).await?;
    println!("Serving on http://{}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
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
}
