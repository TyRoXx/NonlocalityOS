use async_stream::stream;
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::DirectoryEntry;
use dogbox_tree_editor::DirectoryEntryKind;
use dogbox_tree_editor::NormalizedPath;
use dogbox_tree_editor::OpenFile;
use futures::stream::StreamExt;
use std::convert::Infallible;
use std::sync::Arc;
use tracing::error;
use tracing::info;

#[derive(Clone)]
struct DogBoxFileSystem {
    editor: Arc<dogbox_tree_editor::TreeEditor>,
}

impl DogBoxFileSystem {
    pub fn new(editor: dogbox_tree_editor::TreeEditor) -> DogBoxFileSystem {
        DogBoxFileSystem {
            editor: Arc::new(editor),
        }
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
struct DogBoxMetaData {
    kind: DirectoryEntryKind,
}

impl dav_server::fs::DavMetaData for DogBoxMetaData {
    fn len(&self) -> u64 {
        match self.kind {
            DirectoryEntryKind::Directory => 0,
            DirectoryEntryKind::File(length) => length,
        }
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }

    fn is_dir(&self) -> bool {
        match self.kind {
            DirectoryEntryKind::Directory => true,
            DirectoryEntryKind::File(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
struct DogBoxFileMetaData {
    size: u64,
}

impl dav_server::fs::DavMetaData for DogBoxFileMetaData {
    fn len(&self) -> u64 {
        self.size
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(std::time::SystemTime::now())
    }

    fn is_dir(&self) -> bool {
        false
    }
}

struct DogBoxDirEntry {
    info: dogbox_tree_editor::DirectoryEntry,
}

impl dav_server::fs::DavDirEntry for DogBoxDirEntry {
    fn name(&self) -> Vec<u8> {
        self.info.name.as_bytes().into()
    }

    fn metadata(&self) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        let result = match self.info.kind {
            dogbox_tree_editor::DirectoryEntryKind::Directory => {
                Box::new(DogBoxDirectoryMetaData {})
                    as Box<(dyn dav_server::fs::DavMetaData + 'static)>
            }
            dogbox_tree_editor::DirectoryEntryKind::File(size) => {
                Box::new(DogBoxFileMetaData { size: size })
                    as Box<(dyn dav_server::fs::DavMetaData + 'static)>
            }
        };
        Box::pin(async move { Ok(result) })
    }
}

#[derive(Debug)]
struct DogBoxOpenFile {
    handle: Arc<OpenFile>,
    cursor: u64,
}

impl dav_server::fs::DavFile for DogBoxOpenFile {
    fn metadata(&mut self) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        Box::pin(async move {
            Ok(Box::new(DogBoxMetaData {
                kind: self.handle.get_meta_data().await,
            }) as Box<(dyn dav_server::fs::DavMetaData)>)
        })
    }

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn write_bytes(&mut self, buf: bytes::Bytes) -> dav_server::fs::FsFuture<()> {
        let write_at = self.cursor;
        self.cursor += buf.len() as u64;
        let open_file = self.handle.clone();
        Box::pin(async move {
            match open_file.write_bytes(write_at, buf).await {
                Ok(result) => Ok(result),
                Err(error) => match error {
                    dogbox_tree_editor::Error::NotFound => todo!(),
                    dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory => todo!(),
                    dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
                },
            }
        })
    }

    fn read_bytes(&mut self, count: usize) -> dav_server::fs::FsFuture<bytes::Bytes> {
        let read_at = self.cursor;
        self.cursor += count as u64;
        let open_file = self.handle.clone();
        Box::pin(async move {
            match open_file.read_bytes(read_at, count).await {
                Ok(result) => Ok(result),
                Err(error) => match error {
                    dogbox_tree_editor::Error::NotFound => todo!(),
                    dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory => todo!(),
                    dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
                },
            }
        })
    }

    fn seek(&mut self, _pos: std::io::SeekFrom) -> dav_server::fs::FsFuture<u64> {
        todo!()
    }

    fn flush(&mut self) -> dav_server::fs::FsFuture<()> {
        self.handle.flush();
        Box::pin(std::future::ready(Ok(())))
    }
}

fn convert_path<'t>(
    path: &'t dav_server::davpath::DavPath,
) -> dav_server::fs::FsResult<&'t relative_path::RelativePath> {
    match relative_path::RelativePath::from_path(path.as_rel_ospath()) {
        Ok(success) => Ok(success),
        Err(error) => {
            error!(
                "Could not convert path {} into a relative path: {}",
                path, error
            );
            Err(dav_server::fs::FsError::GeneralFailure)
        }
    }
}

impl dav_server::fs::DavFileSystem for DogBoxFileSystem {
    fn open<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        options: dav_server::fs::OpenOptions,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavFile>> {
        info!("Open {} | {:?}", path, options);
        Box::pin(async move {
            let converted_path = convert_path(&path)?;
            let open_file = match self
                .editor
                .open_file(NormalizedPath::new(converted_path))
                .await
            {
                Ok(success) => success,
                Err(_error) => todo!(),
            };
            Ok(Box::new(DogBoxOpenFile {
                handle: open_file,
                cursor: 0,
            }) as Box<dyn dav_server::fs::DavFile>)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        _meta: dav_server::fs::ReadDirMeta,
    ) -> dav_server::fs::FsFuture<dav_server::fs::FsStream<Box<dyn dav_server::fs::DavDirEntry>>>
    {
        info!("Read dir {}", path);
        Box::pin(async move {
            let converted_path = convert_path(&path)?;
            let mut directory = match self
                .editor
                .read_directory(NormalizedPath::new(converted_path))
                .await
            {
                Ok(success) => success,
                Err(error) => match error {
                    dogbox_tree_editor::Error::NotFound => {
                        info!("Directory not found: {}", converted_path);
                        return Err(dav_server::fs::FsError::NotFound);
                    }
                    dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory => {
                        info!(
                            "Cannot open regular file as a directory: {}",
                            converted_path
                        );
                        return Err(dav_server::fs::FsError::NotImplemented);
                    }
                    dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
                },
            };
            Ok(Box::pin(stream! {
                while let Some(entry) = directory.next().await {
                    info!("Directory entry {:?}", entry);
                    yield (Box::new(DogBoxDirEntry{info: entry,}) as Box<dyn dav_server::fs::DavDirEntry>);
                }
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
        info!("Metadata {}", path);
        Box::pin(async move {
            let converted_path = convert_path(&path)?;
            match self
                .editor
                .get_meta_data(NormalizedPath::new(converted_path))
                .await
            {
                Ok(success) => Ok(Box::new(DogBoxMetaData { kind: success })
                    as Box<(dyn dav_server::fs::DavMetaData + 'static)>),
                Err(error) => match error {
                    dogbox_tree_editor::Error::NotFound => {
                        info!("File or directory not found: {}", converted_path);
                        return Err(dav_server::fs::FsError::NotFound);
                    }
                    dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory => todo!(),
                    dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
                },
            }
        })
    }

    fn symlink_metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        self.metadata(path)
    }

    fn create_dir<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn remove_dir<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn remove_file<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn rename<'a>(
        &'a self,
        _from: &'a dav_server::davpath::DavPath,
        _to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn copy<'a>(
        &'a self,
        _from: &'a dav_server::davpath::DavPath,
        _to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<()> {
        todo!()
    }

    fn have_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(std::future::ready(false))
    }

    fn patch_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _patch: Vec<(bool, dav_server::fs::DavProp)>,
    ) -> dav_server::fs::FsFuture<Vec<(hyper::StatusCode, dav_server::fs::DavProp)>> {
        todo!()
    }

    fn get_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _do_content: bool,
    ) -> dav_server::fs::FsFuture<Vec<dav_server::fs::DavProp>> {
        todo!()
    }

    fn get_prop<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _prop: dav_server::fs::DavProp,
    ) -> dav_server::fs::FsFuture<Vec<u8>> {
        todo!()
    }

    fn get_quota(&self) -> dav_server::fs::FsFuture<(u64, Option<u64>)> {
        todo!()
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let dav_server = DavHandler::builder()
        .filesystem(Box::new(DogBoxFileSystem::new(
            dogbox_tree_editor::TreeEditor::from_entries(vec![DirectoryEntry {
                name: "example.txt".to_string(),
                kind: DirectoryEntryKind::File(0),
            }]),
        )))
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
