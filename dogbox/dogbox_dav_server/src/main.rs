//#![deny(warnings)]
use async_stream::stream;
use dav_server::{fakels::FakeLs, DavHandler};
use std::convert::Infallible;

#[derive(Clone)]
struct DogBoxFileSystem {}

impl DogBoxFileSystem {
    pub fn new() -> DogBoxFileSystem {
        DogBoxFileSystem {}
    }
}

#[derive(Debug, Clone)]
struct DogBoxDirectoryMetaData {}

impl dav_server::fs::DavMetaData for DogBoxDirectoryMetaData {
    fn len(&self) -> u64 {
        0
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }

    fn is_dir(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct DogBoxFileMetaData {}

impl dav_server::fs::DavMetaData for DogBoxFileMetaData {
    fn len(&self) -> u64 {
        5
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }

    fn is_dir(&self) -> bool {
        false
    }
}

struct DogBoxDirEntry {}

impl dav_server::fs::DavDirEntry for DogBoxDirEntry {
    fn name(&self) -> Vec<u8> {
        "hello".as_bytes().into()
    }

    fn metadata(&self) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        Box::pin(async move {
            Ok(Box::new(DogBoxFileMetaData {}) as Box<(dyn dav_server::fs::DavMetaData + 'static)>)
        })
    }
}

impl dav_server::fs::DavFileSystem for DogBoxFileSystem {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: dav_server::fs::OpenOptions,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavFile>> {
        todo!()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        meta: dav_server::fs::ReadDirMeta,
    ) -> dav_server::fs::FsFuture<dav_server::fs::FsStream<Box<dyn dav_server::fs::DavDirEntry>>>
    {
        Box::pin(async move {
            Ok(Box::pin(stream! {
                yield (Box::new(DogBoxDirEntry{}) as Box<dyn dav_server::fs::DavDirEntry>);
            })
                as dav_server::fs::FsStream<
                    Box<dyn dav_server::fs::DavDirEntry>,
                >)
        })
    }

    fn metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        Box::pin(async move {
            Ok(Box::new(DogBoxDirectoryMetaData {})
                as Box<(dyn dav_server::fs::DavMetaData + 'static)>)
        })
    }
}

#[tokio::main]
async fn main() {
    let dav_server = DavHandler::builder()
        .filesystem(Box::new(DogBoxFileSystem::new()))
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

    let addr = ([127, 0, 0, 1], 4918).into();
    println!("Serving on http://{}", addr);
    let _ = hyper::Server::bind(&addr)
        .serve(make_service)
        .await
        .map_err(|e| eprintln!("server error: {}", e));
}
