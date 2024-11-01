use async_stream::stream;
use dav_server::fs::FsError;
use dogbox_tree_editor::DirectoryEntryKind;
use dogbox_tree_editor::DirectoryEntryMetaData;
use dogbox_tree_editor::NormalizedPath;
use dogbox_tree_editor::OpenFile;
use futures::stream::StreamExt;
use std::sync::Arc;
use tracing::debug;
use tracing::error;
use tracing::info;

#[derive(Clone)]
pub struct DogBoxFileSystem {
    editor: Arc<dogbox_tree_editor::TreeEditor>,
}

impl DogBoxFileSystem {
    pub fn new(editor: dogbox_tree_editor::TreeEditor) -> DogBoxFileSystem {
        DogBoxFileSystem {
            editor: Arc::new(editor),
        }
    }
}

fn handle_error(err: dogbox_tree_editor::Error) -> FsError {
    return match err {
        dogbox_tree_editor::Error::NotFound(path) => {
            debug!("File or directory not found: {}", path);
            return dav_server::fs::FsError::NotFound;
        }
        dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory(path) => {
            info!("Cannot read regular file as a directory: {}", path);
            return dav_server::fs::FsError::NotImplemented;
        }
        dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
        dogbox_tree_editor::Error::Postcard(_error) => todo!(),
        dogbox_tree_editor::Error::ReferenceIndexOutOfRange => todo!(),
        dogbox_tree_editor::Error::FileSizeMismatch => todo!(),
        dogbox_tree_editor::Error::SegmentedBlobSizeMismatch {
            digest,
            segmented_blob_internal_size,
            directory_entry_size,
        } => {
            error!(
                "Segmented blob {} has internal size {}, but a directory listed it as size {}",
                &digest, segmented_blob_internal_size, directory_entry_size
            );
            return dav_server::fs::FsError::GeneralFailure;
        }
        dogbox_tree_editor::Error::CannotRename => FsError::Forbidden,
        dogbox_tree_editor::Error::MissingValue(_) => todo!(),
        dogbox_tree_editor::Error::Storage(_) => todo!(),
    };
}

#[derive(Debug, Clone)]
struct DogBoxDirectoryMetaData {
    modified: std::time::SystemTime,
}

impl dav_server::fs::DavMetaData for DogBoxDirectoryMetaData {
    fn len(&self) -> u64 {
        0
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(self.modified)
    }

    fn is_dir(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct DogBoxMetaData {
    entry: DirectoryEntryMetaData,
}

impl dav_server::fs::DavMetaData for DogBoxMetaData {
    fn len(&self) -> u64 {
        match self.entry.kind {
            DirectoryEntryKind::Directory => 0,
            DirectoryEntryKind::File(length) => length,
        }
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(self.entry.modified)
    }

    fn is_dir(&self) -> bool {
        match self.entry.kind {
            DirectoryEntryKind::Directory => true,
            DirectoryEntryKind::File(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
struct DogBoxFileMetaData {
    size: u64,
    modified: std::time::SystemTime,
}

impl dav_server::fs::DavMetaData for DogBoxFileMetaData {
    fn len(&self) -> u64 {
        self.size
    }

    fn modified(&self) -> dav_server::fs::FsResult<std::time::SystemTime> {
        Ok(self.modified)
    }

    fn is_dir(&self) -> bool {
        false
    }
}

struct DogBoxDirEntry {
    info: dogbox_tree_editor::MutableDirectoryEntry,
}

impl dav_server::fs::DavDirEntry for DogBoxDirEntry {
    fn name(&self) -> Vec<u8> {
        self.info.name.as_bytes().into()
    }

    fn metadata(&self) -> dav_server::fs::FsFuture<Box<dyn dav_server::fs::DavMetaData>> {
        let result = match self.info.kind {
            dogbox_tree_editor::DirectoryEntryKind::Directory => Box::new(DogBoxDirectoryMetaData {
                modified: self.info.modified,
            })
                as Box<(dyn dav_server::fs::DavMetaData + 'static)>,
            dogbox_tree_editor::DirectoryEntryKind::File(size) => Box::new(DogBoxFileMetaData {
                size: size,
                modified: self.info.modified,
            })
                as Box<(dyn dav_server::fs::DavMetaData + 'static)>,
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
                entry: self.handle.get_meta_data().await,
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
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn read_bytes(&mut self, count: usize) -> dav_server::fs::FsFuture<bytes::Bytes> {
        let read_at = self.cursor;
        let open_file = self.handle.clone();
        Box::pin(async move {
            match open_file.read_bytes(read_at, count).await {
                Ok(result) => {
                    self.cursor += result.len() as u64;
                    Ok(result)
                }
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> dav_server::fs::FsFuture<u64> {
        let open_file = self.handle.clone();
        Box::pin(async move {
            match pos {
                std::io::SeekFrom::Start(offset) => {
                    self.cursor = offset;
                }
                std::io::SeekFrom::End(offset) => {
                    let size = open_file.size().await;
                    self.cursor = size.saturating_add_signed(offset);
                }
                std::io::SeekFrom::Current(offset) => {
                    self.cursor = self.cursor.saturating_add_signed(offset);
                }
            }
            Ok(self.cursor)
        })
    }

    fn flush(&mut self) -> dav_server::fs::FsFuture<()> {
        Box::pin(async {
            match self.handle.flush().await {
                Ok(_) => Ok(()),
                Err(_error) => todo!(),
            }
        })
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
    ) -> dav_server::fs::FsFuture<'a, Box<dyn dav_server::fs::DavFile>> {
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
    ) -> dav_server::fs::FsFuture<'a, dav_server::fs::FsStream<Box<dyn dav_server::fs::DavDirEntry>>>
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
                Err(error) => return Err(handle_error(error)),
            };
            Ok(Box::pin(stream! {
                while let Some(entry) = directory.next().await {
                    info!("Directory entry {:?}", entry);
                    yield Ok(Box::new(DogBoxDirEntry{info: entry,}) as Box<dyn dav_server::fs::DavDirEntry>);
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
    ) -> dav_server::fs::FsFuture<'a, Box<dyn dav_server::fs::DavMetaData>> {
        Box::pin(async move {
            let converted_path = convert_path(&path)?;
            match self
                .editor
                .get_meta_data(NormalizedPath::new(converted_path))
                .await
            {
                Ok(success) => {
                    info!("Metadata {}: {:?}", path, &success);
                    Ok(Box::new(DogBoxMetaData { entry: success })
                        as Box<(dyn dav_server::fs::DavMetaData + 'static)>)
                }
                Err(error) => {
                    info!("Metadata failed for {}: {:?}", path, &error);
                    Err(handle_error(error))
                }
            }
        })
    }

    fn symlink_metadata<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, Box<dyn dav_server::fs::DavMetaData>> {
        self.metadata(path)
    }

    fn create_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Create directory {}", path);
        Box::pin(async move {
            let converted_path = convert_path(&path)?;
            match self
                .editor
                .create_directory(NormalizedPath::new(converted_path))
                .await
            {
                Ok(success) => Ok(success),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn remove_dir<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Removing directory {}", _path);
        Box::pin(async move {
            let converted_path = convert_path(&_path)?;
            match self
                .editor
                .remove(NormalizedPath::new(converted_path))
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn remove_file<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Removing file {}", _path);
        Box::pin(async move {
            let converted_path = convert_path(&_path)?;
            match self
                .editor
                .remove(NormalizedPath::new(converted_path))
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn rename<'a>(
        &'a self,
        _from: &'a dav_server::davpath::DavPath,
        _to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Rename {} to {}", _from, _to);
        Box::pin(async move {
            let from_converted_path = convert_path(&_from)?;
            let to_converted_path = convert_path(&_to)?;
            match self
                .editor
                .rename(
                    NormalizedPath::new(from_converted_path),
                    NormalizedPath::new(to_converted_path),
                )
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn copy<'a>(
        &'a self,
        _from: &'a dav_server::davpath::DavPath,
        _to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        todo!()
    }

    fn have_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        debug!("have_props");
        Box::pin(std::future::ready(false))
    }

    fn patch_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _patch: Vec<(bool, dav_server::fs::DavProp)>,
    ) -> dav_server::fs::FsFuture<'a, Vec<(hyper::StatusCode, dav_server::fs::DavProp)>> {
        todo!()
    }

    fn get_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _do_content: bool,
    ) -> dav_server::fs::FsFuture<'a, Vec<dav_server::fs::DavProp>> {
        todo!()
    }

    fn get_prop<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _prop: dav_server::fs::DavProp,
    ) -> dav_server::fs::FsFuture<'a, Vec<u8>> {
        todo!()
    }

    fn get_quota(&self) -> dav_server::fs::FsFuture<(u64, Option<u64>)> {
        Box::pin(core::future::ready(Err(FsError::NotImplemented)))
    }
}
