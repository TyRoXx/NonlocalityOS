use async_stream::stream;
use dav_server::fs::FsError;
use dogbox_tree::serialization::DirectoryEntryKind;
use dogbox_tree_editor::DirectoryEntryMetaData;
use dogbox_tree_editor::NormalizedPath;
use dogbox_tree_editor::OpenFile;
use dogbox_tree_editor::OpenFileReadPermission;
use dogbox_tree_editor::OpenFileWritePermission;
use futures::stream::StreamExt;
use std::sync::Arc;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

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
    match err {
        dogbox_tree_editor::Error::NotFound(path) => {
            debug!("File or directory not found: {}", path);
            dav_server::fs::FsError::NotFound
        }
        dogbox_tree_editor::Error::CannotOpenRegularFileAsDirectory(path) => {
            info!("Cannot read regular file as a directory: {}", path);
            dav_server::fs::FsError::NotImplemented
        }
        dogbox_tree_editor::Error::CannotOpenDirectoryAsRegularFile => todo!(),
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
            dav_server::fs::FsError::GeneralFailure
        }
        dogbox_tree_editor::Error::CannotRename => FsError::Forbidden,
        dogbox_tree_editor::Error::Storage(_) => todo!(),
        dogbox_tree_editor::Error::TooManyReferences(_blob_digest) => todo!(),
        dogbox_tree_editor::Error::SaveFailed => {
            error!("Saving failed");
            dav_server::fs::FsError::GeneralFailure
        }
        dogbox_tree_editor::Error::Deserialization(deserialization_error) => {
            match deserialization_error {
                dogbox_tree::serialization::DeserializationError::MissingTree(digest) => {
                    error!("Deserialization failed due to missing tree: {}", digest);
                    dav_server::fs::FsError::GeneralFailure
                }
                dogbox_tree::serialization::DeserializationError::Postcard(postcard_error) => {
                    error!(
                        "Deserialization failed due to postcard error: {}",
                        postcard_error
                    );
                    dav_server::fs::FsError::GeneralFailure
                }
                dogbox_tree::serialization::DeserializationError::ReferenceIndexOutOfRange => {
                    error!("Deserialization failed due to reference index out of range");
                    dav_server::fs::FsError::GeneralFailure
                }
            }
        }
        dogbox_tree_editor::Error::OtherDeserializationError(message) => {
            error!("Deserialization failed: {}", message);
            dav_server::fs::FsError::GeneralFailure
        }
        dogbox_tree_editor::Error::OtherSerializationError(message) => {
            error!("Serialization failed: {}", message);
            dav_server::fs::FsError::GeneralFailure
        }
    }
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
        self.info.name.as_str().as_bytes().into()
    }

    fn metadata(&self) -> dav_server::fs::FsFuture<'_, Box<dyn dav_server::fs::DavMetaData>> {
        let result = match self.info.kind {
            dogbox_tree::serialization::DirectoryEntryKind::Directory => {
                Box::new(DogBoxDirectoryMetaData {
                    modified: self.info.modified,
                }) as Box<dyn dav_server::fs::DavMetaData + 'static>
            }
            dogbox_tree::serialization::DirectoryEntryKind::File(size) => {
                Box::new(DogBoxFileMetaData {
                    size,
                    modified: self.info.modified,
                }) as Box<dyn dav_server::fs::DavMetaData + 'static>
            }
        };
        Box::pin(async move { Ok(result) })
    }
}

#[derive(Debug)]
pub(crate) struct DogBoxOpenFile {
    opened_path: relative_path::RelativePathBuf,
    handle: Arc<OpenFile>,
    read_permission: Option<Arc<OpenFileReadPermission>>,
    write_permission: Option<Arc<OpenFileWritePermission>>,
    cursor: u64,
}

impl DogBoxOpenFile {
    #[cfg(test)]
    pub(crate) fn new(
        opened_path: relative_path::RelativePathBuf,
        handle: Arc<OpenFile>,
        read_permission: Option<Arc<OpenFileReadPermission>>,
        write_permission: Option<Arc<OpenFileWritePermission>>,
        cursor: u64,
    ) -> Self {
        Self {
            opened_path,
            handle,
            read_permission,
            write_permission,
            cursor,
        }
    }
}

impl dav_server::fs::DavFile for DogBoxOpenFile {
    fn metadata(&mut self) -> dav_server::fs::FsFuture<'_, Box<dyn dav_server::fs::DavMetaData>> {
        Box::pin(async move {
            Ok(Box::new(DogBoxMetaData {
                entry: self.handle.get_meta_data().await,
            }) as Box<dyn dav_server::fs::DavMetaData>)
        })
    }

    fn write_buf(&mut self, _buf: Box<dyn bytes::Buf + Send>) -> dav_server::fs::FsFuture<'_, ()> {
        todo!()
    }

    fn write_bytes(&mut self, buf: bytes::Bytes) -> dav_server::fs::FsFuture<'_, ()> {
        let write_at = self.cursor;
        let maybe_new_cursor = self.cursor.checked_add(buf.len() as u64);
        match maybe_new_cursor {
            Some(new_cursor) => self.cursor = new_cursor,
            None => return Box::pin(async move { Err(FsError::TooLarge) }),
        }
        let open_file = self.handle.clone();
        Box::pin(async move {
            match &self.write_permission {
                Some(writeable) => match open_file.write_bytes(writeable, write_at, buf).await {
                    Ok(result) => Ok(result),
                    Err(error) => Err(handle_error(error)),
                },
                None => {
                    warn!("Disallowed writing to a file that has not been opened for writing.");
                    Err(FsError::Forbidden)
                }
            }
        })
    }

    fn read_bytes(&mut self, count: usize) -> dav_server::fs::FsFuture<'_, bytes::Bytes> {
        let read_at = self.cursor;
        let open_file = self.handle.clone();
        Box::pin(async move {
            match &self.read_permission {
                Some(readable) => match open_file.read_bytes(readable, read_at, count).await {
                    Ok(result) => {
                        self.cursor += result.len() as u64;
                        Ok(result)
                    }
                    Err(error) => {
                        error!(
                        "Error reading from file {} at {read_at} (up to {count} bytes): {error}",
                        &self.opened_path
                    );
                        Err(handle_error(error))
                    }
                },
                None => {
                    warn!("Disallowed reading from a file that has not been opened for reading.");
                    Err(FsError::Forbidden)
                }
            }
        })
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> dav_server::fs::FsFuture<'_, u64> {
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

    fn flush(&mut self) -> dav_server::fs::FsFuture<'_, ()> {
        Box::pin(async {
            match self.handle.flush().await {
                Ok(_) => Ok(()),
                Err(_error) => todo!(),
            }
        })
    }
}

impl Drop for DogBoxOpenFile {
    fn drop(&mut self) {
        if self.read_permission.is_some() {
            self.read_permission = None;
            self.handle.notify_dropped_read_permission();
        }
        if self.write_permission.is_some() {
            self.write_permission = None;
            self.handle.notify_dropped_write_permission();
        }
    }
}

fn convert_path(
    path: &dav_server::davpath::DavPath,
) -> dav_server::fs::FsResult<&relative_path::RelativePath> {
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

fn normalize_path(path: &dav_server::davpath::DavPath) -> dav_server::fs::FsResult<NormalizedPath> {
    let converted_path = convert_path(path)?;
    match NormalizedPath::try_from(converted_path) {
        Ok(success) => Ok(success),
        Err(error) => {
            error!("Could not normalize path {}: {}", path, error);
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
        debug!("Open {} | write: {}", path, options.write);
        if options.append {
            todo!()
        }
        if options.create_new {
            warn!("options.create_new not supported yet");
        }
        if options.checksum.is_some() {
            todo!()
        }
        if let Some(size) = options.size {
            if size != 0 {
                debug!("Ignoring size hint ({} B)", size);
            }
        }
        Box::pin(async move {
            let converted_path = convert_path(path)?;
            let normalized_path = normalize_path(path)?;
            let open_file = match self.editor.open_file(normalized_path).await {
                Ok(success) => success,
                Err(_error) => todo!(),
            };
            let read_permission = match options.read {
                true => Some(open_file.get_read_permission()),
                false => None,
            };
            let write_permission = match options.write {
                true => Some(open_file.get_write_permission()),
                false => None,
            };
            if options.truncate {
                match &write_permission {
                    Some(writeable) => open_file.truncate(writeable).await.map_err(handle_error)?,
                    None => return Err(FsError::Forbidden),
                }
            }
            let result = Box::new(DogBoxOpenFile {
                opened_path: converted_path.to_owned(),
                handle: open_file,
                cursor: 0,
                read_permission,
                write_permission,
            });
            Ok(result as Box<dyn dav_server::fs::DavFile>)
        })
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
        _meta: dav_server::fs::ReadDirMeta,
    ) -> dav_server::fs::FsFuture<'a, dav_server::fs::FsStream<Box<dyn dav_server::fs::DavDirEntry>>>
    {
        debug!("Read dir {}", path);
        Box::pin(async move {
            let normalized_path = normalize_path(path)?;
            let mut directory = match self.editor.read_directory(normalized_path).await {
                Ok(success) => success,
                Err(error) => return Err(handle_error(error)),
            };
            Ok(Box::pin(stream! {
                while let Some(entry) = directory.next().await {
                    debug!("Directory entry {:?}", entry);
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
            let normalized_path = normalize_path(path)?;
            match self.editor.get_meta_data(normalized_path).await {
                Ok(success) => {
                    debug!("Metadata {}: {:?}", path, &success);
                    Ok(Box::new(DogBoxMetaData { entry: success })
                        as Box<dyn dav_server::fs::DavMetaData + 'static>)
                }
                Err(error) => Err(handle_error(error)),
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
            let normalized_path = normalize_path(path)?;
            match self.editor.create_directory(normalized_path).await {
                Ok(success) => Ok(success),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn remove_dir<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Removing directory {}", path);
        Box::pin(async move {
            let normalized_path = normalize_path(path)?;
            match self.editor.remove(normalized_path).await {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn remove_file<'a>(
        &'a self,
        path: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Removing file {}", path);
        Box::pin(async move {
            let normalized_path = normalize_path(path)?;
            match self.editor.remove(normalized_path).await {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        debug!("Rename {} to {}", from, to);
        Box::pin(async move {
            let from_normalized_path = normalize_path(from)?;
            let to_normalized_path = normalize_path(to)?;
            match self
                .editor
                .rename(from_normalized_path, to_normalized_path)
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    //#[instrument(skip(self))]
    fn copy<'a>(
        &'a self,
        from: &'a dav_server::davpath::DavPath,
        to: &'a dav_server::davpath::DavPath,
    ) -> dav_server::fs::FsFuture<'a, ()> {
        info!("Copy {} to {}", from, to);
        Box::pin(async move {
            let from_normalized_path = normalize_path(from)?;
            let to_normalized_path = normalize_path(to)?;
            match self
                .editor
                .copy(from_normalized_path, to_normalized_path)
                .await
            {
                Ok(_) => Ok(()),
                Err(error) => Err(handle_error(error)),
            }
        })
    }

    fn have_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(std::future::ready(true))
    }

    fn patch_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _patch: Vec<(bool, dav_server::fs::DavProp)>,
    ) -> dav_server::fs::FsFuture<'a, Vec<(hyper::StatusCode, dav_server::fs::DavProp)>> {
        Box::pin(core::future::ready(Err(FsError::NotImplemented)))
    }

    fn get_props<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _do_content: bool,
    ) -> dav_server::fs::FsFuture<'a, Vec<dav_server::fs::DavProp>> {
        Box::pin(core::future::ready(Ok(vec![])))
    }

    //#[instrument(skip(self))]
    fn get_prop<'a>(
        &'a self,
        _path: &'a dav_server::davpath::DavPath,
        _prop: dav_server::fs::DavProp,
    ) -> dav_server::fs::FsFuture<'a, Vec<u8>> {
        Box::pin(core::future::ready(Err(FsError::NotImplemented)))
    }

    //#[instrument(skip(self))]
    fn get_quota(&self) -> dav_server::fs::FsFuture<'_, (u64, Option<u64>)> {
        Box::pin(core::future::ready(Err(FsError::NotImplemented)))
    }
}
