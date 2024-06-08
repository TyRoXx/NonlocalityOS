#![deny(warnings)]
use dav_server::{fakels::FakeLs, localfs::LocalFs, DavHandler};
use std::convert::Infallible;

#[tokio::main]
async fn main() {
    let dir = "C:\\dev\\NonlocalityOS\\dogbox";
    let addr = ([127, 0, 0, 1], 4918).into();

    let dav_server = DavHandler::builder()
        .filesystem(LocalFs::new(dir, false, false, false))
        .locksystem(FakeLs::new())
        .build_handler();

    let make_service = hyper::service::make_service_fn(move |_| {
        let dav_server = dav_server.clone();
        async move {
            let func = move |req| {
                let dav_server = dav_server.clone();
                async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
            };
            Ok::<_, Infallible>(hyper::service::service_fn(func))
        }
    });

    println!("Serving {} on http://{}", dir, addr);
    let _ = hyper::Server::bind(&addr)
        .serve(make_service)
        .await
        .map_err(|e| eprintln!("server error: {}", e));
}
