#![feature(test)]
#[cfg(test)]
mod benchmarks;

#[cfg(test)]
mod tests2;

use astraea::{
    storage::{LoadStoreValue, StoreError},
    tree::{BlobDigest, HashedValue, ReferenceIndex, Tree, TreeBlob, VALUE_BLOB_MAX_LENGTH},
};
use async_stream::stream;
use bytes::Buf;
use cached::Cached;
use dogbox_tree::serialization::{self, DirectoryTree, FileName, SegmentedBlob};
use futures::future::join_all;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    pin::Pin,
    sync::Arc,
    u64,
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    NotFound(String),
    CannotOpenRegularFileAsDirectory(String),
    CannotOpenDirectoryAsRegularFile,
    Postcard(postcard::Error),
    ReferenceIndexOutOfRange,
    FileSizeMismatch,
    SegmentedBlobSizeMismatch {
        digest: BlobDigest,
        segmented_blob_internal_size: u64,
        directory_entry_size: u64,
    },
    CannotRename,
    MissingValue(BlobDigest),
    Storage(StoreError),
    TooManyReferences(BlobDigest),
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Future<'a, T> = Pin<Box<dyn core::future::Future<Output = Result<T>> + Send + 'a>>;
pub type Stream<T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send>>;

#[derive(Clone, Debug, PartialEq, Copy)]
pub enum DirectoryEntryKind {
    Directory,
    File(u64),
}

#[derive(Clone, Debug, PartialEq, Copy)]
pub struct DirectoryEntryMetaData {
    pub kind: DirectoryEntryKind,
    pub modified: std::time::SystemTime,
}

impl DirectoryEntryMetaData {
    pub fn new(kind: DirectoryEntryKind, modified: std::time::SystemTime) -> Self {
        Self { kind, modified }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
    pub digest: BlobDigest,
}

impl DirectoryEntry {
    pub fn new(name: String, kind: DirectoryEntryKind, digest: BlobDigest) -> Self {
        Self { name, kind, digest }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutableDirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
    pub modified: std::time::SystemTime,
}

impl MutableDirectoryEntry {
    pub fn new(name: String, kind: DirectoryEntryKind, modified: std::time::SystemTime) -> Self {
        Self {
            name,
            kind,
            modified,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct CacheDropStats {
    hashed_values_dropped: usize,
    open_files_closed: usize,
    open_directories_closed: usize,
}

impl CacheDropStats {
    pub fn new(
        hashed_values_dropped: usize,
        open_files_closed: usize,
        open_directories_closed: usize,
    ) -> Self {
        Self {
            hashed_values_dropped,
            open_files_closed,
            open_directories_closed,
        }
    }

    pub fn add(&mut self, other: &CacheDropStats) {
        self.hashed_values_dropped += other.hashed_values_dropped;
        self.open_files_closed += other.open_files_closed;
        self.open_directories_closed += other.open_directories_closed;
    }
}

pub enum OpenNamedEntryStatus {
    Directory(OpenDirectoryStatus),
    File(OpenFileStatus),
}

pub enum NamedEntryStatus {
    Closed(serialization::DirectoryEntryKind, BlobDigest),
    Open(OpenNamedEntryStatus),
}

#[derive(Clone, Debug)]
pub enum NamedEntry {
    NotOpen(DirectoryEntryMetaData, BlobDigest),
    OpenRegularFile(Arc<OpenFile>, tokio::sync::watch::Receiver<OpenFileStatus>),
    OpenSubdirectory(
        Arc<OpenDirectory>,
        tokio::sync::watch::Receiver<OpenDirectoryStatus>,
    ),
}

impl NamedEntry {
    async fn get_meta_data(&self) -> DirectoryEntryMetaData {
        match self {
            NamedEntry::NotOpen(meta_data, _) => meta_data.clone(),
            NamedEntry::OpenRegularFile(open_file, _) => open_file.get_meta_data().await,
            NamedEntry::OpenSubdirectory(open_directory, _) => DirectoryEntryMetaData::new(
                DirectoryEntryKind::Directory,
                open_directory.modified(),
            ),
        }
    }

    fn get_status(&self) -> NamedEntryStatus {
        match self {
            NamedEntry::NotOpen(directory_entry_meta_data, blob_digest) => {
                NamedEntryStatus::Closed(
                    match directory_entry_meta_data.kind {
                        DirectoryEntryKind::Directory => {
                            serialization::DirectoryEntryKind::Directory
                        }
                        DirectoryEntryKind::File(size) => {
                            serialization::DirectoryEntryKind::File(size)
                        }
                    },
                    *blob_digest,
                )
            }
            NamedEntry::OpenRegularFile(_open_file, receiver) => {
                let open_file_status: OpenFileStatus = *receiver.borrow();
                NamedEntryStatus::Open(OpenNamedEntryStatus::File(open_file_status))
            }
            NamedEntry::OpenSubdirectory(_directory, receiver) => {
                let open_directory_status: OpenDirectoryStatus = *receiver.borrow();
                NamedEntryStatus::Open(OpenNamedEntryStatus::Directory(open_directory_status))
            }
        }
    }

    fn watch(&mut self, on_change: Box<(dyn Fn() -> Future<'static, ()> + Send + Sync)>) {
        match self {
            NamedEntry::NotOpen(_directory_entry_meta_data, _blob_digest) => {}
            NamedEntry::OpenRegularFile(_arc, receiver) => {
                let mut cloned_receiver = receiver.clone();
                let mut previous_status = *cloned_receiver.borrow_and_update();
                debug!("The previous status was: {:?}", &previous_status);
                tokio::task::spawn(async move {
                    loop {
                        match cloned_receiver.changed().await {
                            Ok(_) => {
                                let current_status = *cloned_receiver.borrow();
                                if previous_status == current_status {
                                    warn!(
                                        "Open file status received, but it is the same as before: {:?}",
                                        &previous_status
                                    );
                                } else {
                                    debug!(
                                        "Open file status changed from {:?} to {:?}",
                                        &previous_status, &current_status
                                    );
                                    previous_status = current_status;
                                    on_change().await.unwrap();
                                }
                            }
                            Err(error) => {
                                debug!("No longer watching a file: {}", &error);
                                break;
                            }
                        }
                    }
                });
            }
            NamedEntry::OpenSubdirectory(_arc, receiver) => {
                let mut cloned_receiver = receiver.clone();
                let mut previous_status = *cloned_receiver.borrow();
                debug!("The previous status was: {:?}", &previous_status);
                tokio::task::spawn(async move {
                    loop {
                        match cloned_receiver.changed().await {
                            Ok(_) => {
                                let current_status = *cloned_receiver.borrow();
                                if previous_status == current_status {
                                    info!(
                                        "Open directory status received, but it is the same as before: {:?}",
                                        &previous_status
                                    );
                                } else {
                                    info!("Open directory status changed: {:?}", &current_status);
                                    previous_status = current_status;
                                    on_change().await.unwrap();
                                }
                            }
                            Err(error) => {
                                debug!("No longer watching a directory: {}", &error);
                                break;
                            }
                        }
                    }
                });
            }
        }
    }

    async fn request_save(&self) -> Result<NamedEntryStatus> {
        match self {
            NamedEntry::NotOpen(directory_entry_meta_data, blob_digest) => {
                Ok(NamedEntryStatus::Closed(
                    match directory_entry_meta_data.kind {
                        DirectoryEntryKind::Directory => {
                            serialization::DirectoryEntryKind::Directory
                        }
                        DirectoryEntryKind::File(size) => {
                            serialization::DirectoryEntryKind::File(size)
                        }
                    },
                    *blob_digest,
                ))
            }
            NamedEntry::OpenRegularFile(arc, _receiver) => {
                Ok(NamedEntryStatus::Open(OpenNamedEntryStatus::File(
                    arc.request_save()
                        .await
                        .map_err(|error| Error::Storage(error))?,
                )))
            }
            NamedEntry::OpenSubdirectory(arc, _receiver) => Ok(NamedEntryStatus::Open(
                OpenNamedEntryStatus::Directory(arc.request_save().await?),
            )),
        }
    }

    async fn drop_all_read_caches(&mut self) -> CacheDropStats {
        match self {
            NamedEntry::NotOpen(_directory_entry_meta_data, _blob_digest) => {
                CacheDropStats::new(0, 0, 0)
            }
            NamedEntry::OpenRegularFile(arc, _receiver) => {
                if Arc::strong_count(arc) == 1 {
                    let (digest, size) = arc.last_known_digest().await;
                    if digest.is_digest_up_to_date {
                        let modified = arc.modified();
                        *self = NamedEntry::NotOpen(
                            DirectoryEntryMetaData::new(DirectoryEntryKind::File(size), modified),
                            digest.last_known_digest,
                        );
                        return CacheDropStats::new(0, 1, 0);
                    }
                }
                arc.drop_all_read_caches().await
            }
            NamedEntry::OpenSubdirectory(arc, _receiver) => {
                //TODO: close open directories (correctly)
                Box::pin(arc.drop_all_read_caches()).await
            }
        }
    }
}

pub type WallClock = fn() -> std::time::SystemTime;

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct OpenDirectoryStatus {
    pub digest: DigestStatus,
    pub directories_open_count: usize,
    pub directories_unsaved_count: usize,
    pub files_open_count: usize,
    pub files_open_for_writing_count: usize,
    pub files_unflushed_count: usize,
    pub bytes_unflushed_count: u64,
}

impl OpenDirectoryStatus {
    pub fn new(
        digest: DigestStatus,
        directories_open_count: usize,
        directories_unsaved_count: usize,
        files_open_count: usize,
        files_open_for_writing_count: usize,
        files_unflushed_count: usize,
        bytes_unflushed_count: u64,
    ) -> Self {
        Self {
            digest,
            directories_open_count,
            directories_unsaved_count,
            files_open_count,
            files_open_for_writing_count,
            files_unflushed_count,
            bytes_unflushed_count,
        }
    }
}

#[derive(Debug)]
struct OpenDirectoryMutableState {
    // TODO: support really big directories. We may not be able to hold all entries in memory at the same time.
    names: BTreeMap<String, NamedEntry>,
    has_unsaved_changes: bool,
}

impl OpenDirectoryMutableState {
    fn new(names: BTreeMap<String, NamedEntry>, has_unsaved_changes: bool) -> Self {
        Self {
            names,
            has_unsaved_changes,
        }
    }
}

#[derive(Debug)]
pub struct OpenDirectory {
    state: tokio::sync::Mutex<OpenDirectoryMutableState>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    change_event_sender: tokio::sync::watch::Sender<OpenDirectoryStatus>,
    _change_event_receiver: tokio::sync::watch::Receiver<OpenDirectoryStatus>,
    modified: std::time::SystemTime,
    clock: WallClock,
    open_file_write_buffer_in_blocks: usize,
}

impl OpenDirectory {
    pub fn new(
        digest: DigestStatus,
        names: BTreeMap<String, NamedEntry>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        modified: std::time::SystemTime,
        clock: WallClock,
        open_file_write_buffer_in_blocks: usize,
    ) -> Self {
        let has_unsaved_changes = !digest.is_digest_up_to_date;
        let (change_event_sender, change_event_receiver) =
            tokio::sync::watch::channel(OpenDirectoryStatus::new(digest, 1, 0, 0, 0, 0, 0));
        Self {
            state: Mutex::new(OpenDirectoryMutableState::new(names, has_unsaved_changes)),
            storage,
            change_event_sender,
            _change_event_receiver: change_event_receiver,
            modified,
            clock,
            open_file_write_buffer_in_blocks,
        }
    }

    pub fn from_entries(
        digest: DigestStatus,
        entries: Vec<DirectoryEntry>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        modified: std::time::SystemTime,
        clock: WallClock,
        open_file_write_buffer_in_blocks: usize,
    ) -> OpenDirectory {
        let names = BTreeMap::from_iter(entries.iter().map(|entry| {
            (
                entry.name.clone(),
                NamedEntry::NotOpen(
                    DirectoryEntryMetaData::new(entry.kind.clone(), modified),
                    entry.digest,
                ),
            )
        }));
        OpenDirectory::new(
            digest,
            names,
            storage.clone(),
            modified,
            clock,
            open_file_write_buffer_in_blocks,
        )
    }

    pub fn get_storage(&self) -> Arc<dyn LoadStoreValue + Send + Sync> {
        self.storage.clone()
    }

    pub fn get_clock(&self) -> fn() -> std::time::SystemTime {
        self.clock
    }

    pub fn latest_status(&self) -> OpenDirectoryStatus {
        *self.change_event_sender.borrow()
    }

    pub fn modified(&self) -> std::time::SystemTime {
        self.modified
    }

    pub async fn read(&self) -> Stream<MutableDirectoryEntry> {
        let state_locked = self.state.lock().await;
        let snapshot = state_locked.names.clone();
        debug!("Reading directory with {} entries", snapshot.len());
        Box::pin(stream! {
            for cached_entry in snapshot {
                let meta_data = cached_entry.1.get_meta_data().await;
                yield MutableDirectoryEntry{name: cached_entry.0, kind: meta_data.kind, modified: meta_data.modified,};
            }
        })
    }

    pub async fn get_meta_data(&self, name: &str) -> Result<DirectoryEntryMetaData> {
        let state_locked = self.state.lock().await;
        match state_locked.names.get(name) {
            Some(found) => {
                let found_clone = (*found).clone();
                Ok(found_clone.get_meta_data().await)
            }
            None => Err(Error::NotFound(name.to_string())),
        }
    }

    pub async fn open_file(
        self: Arc<OpenDirectory>,
        name: &str,
        empty_file_digest: &BlobDigest,
    ) -> Result<Arc<OpenFile>> {
        let mut state_locked = self.state.lock().await;
        match state_locked.names.get_mut(name) {
            Some(found) => match found {
                NamedEntry::NotOpen(meta_data, digest) => match meta_data.kind {
                    DirectoryEntryKind::Directory => todo!(),
                    DirectoryEntryKind::File(length) => {
                        debug!(
                            "Opening file of size {} and content {} for reading.",
                            length, digest
                        );
                        let open_file = Arc::new(OpenFile::new(
                            OpenFileContentBuffer::from_storage(
                                digest.clone(),
                                length,
                                self.open_file_write_buffer_in_blocks,
                            ),
                            self.storage.clone(),
                            self.modified,
                        ));
                        let receiver = open_file.watch().await;
                        let mut new_entry =
                            NamedEntry::OpenRegularFile(open_file.clone(), receiver);
                        self.clone().watch_new_entry(&mut new_entry);
                        *found = new_entry;
                        Ok(open_file)
                    }
                },
                NamedEntry::OpenRegularFile(open_file, _) => Ok(open_file.clone()),
                NamedEntry::OpenSubdirectory(_, _) => Err(Error::CannotOpenDirectoryAsRegularFile),
            },
            None => {
                let open_file = Arc::new(OpenFile::new(
                    OpenFileContentBuffer::from_storage(
                        *empty_file_digest,
                        0,
                        self.open_file_write_buffer_in_blocks,
                    ),
                    self.storage.clone(),
                    (self.clock)(),
                ));
                info!("Adding file {} to the directory which sends a change event for its parent directory.", &name);
                let receiver = open_file.watch().await;
                self.clone().insert_entry(
                    &mut state_locked,
                    name.to_string(),
                    NamedEntry::OpenRegularFile(open_file.clone(), receiver),
                );
                Self::notify_about_change(&mut state_locked, &self.change_event_sender).await;
                Ok(open_file)
            }
        }
    }

    fn watch_new_entry(self: Arc<OpenDirectory>, entry: &mut NamedEntry) {
        entry.watch(Box::new(move || {
            debug!("Notifying directory of changes in one of the entries.");
            let self2 = self.clone();
            Box::pin(async move {
                let mut state_locked = self2.state.lock().await;
                debug!("ACTUALLY Notifying directory of changes in one of the entries.");
                Self::notify_about_change(&mut state_locked, &self2.change_event_sender).await;
                Ok(())
            })
        }));
    }

    fn insert_entry(
        self: Arc<OpenDirectory>,
        state: &mut OpenDirectoryMutableState,
        name: String,
        mut entry: NamedEntry,
    ) {
        self.watch_new_entry(&mut entry);
        let previous_entry = state.names.insert(name, entry);
        assert!(previous_entry.is_none());
    }

    pub async fn load_directory(
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        digest: &BlobDigest,
        modified: std::time::SystemTime,
        clock: WallClock,
        open_file_write_buffer_in_blocks: usize,
    ) -> Result<Arc<OpenDirectory>> {
        match storage.load_value(digest).await {
            Some(delayed_loaded) => {
                let loaded = delayed_loaded.hash().unwrap(/*TODO*/);
                let parsed_directory: DirectoryTree =
                    match postcard::from_bytes(loaded.value().blob().as_slice()) {
                        Ok(success) => success,
                        Err(error) => return Err(Error::Postcard(error)),
                    };
                let mut entries = vec![];
                info!(
                    "Loading directory with {} entries",
                    parsed_directory.children.len()
                );
                entries.reserve(parsed_directory.children.len());
                for maybe_entry in parsed_directory.children.iter().map(|child| {
                    let kind = match child.1.kind {
                        serialization::DirectoryEntryKind::Directory => {
                            DirectoryEntryKind::Directory
                        }
                        serialization::DirectoryEntryKind::File(size) => {
                            DirectoryEntryKind::File(size)
                        }
                    };
                    match &child.1.content {
                        serialization::ReferenceIndexOrInlineContent::Indirect(reference_index) => {
                            let index: usize = usize::try_from(reference_index.0)
                                .map_err(|_error| Error::ReferenceIndexOutOfRange)?;
                            if index >= loaded.value().references().len() {
                                return Err(Error::ReferenceIndexOutOfRange);
                            }
                            let digest = loaded.value().references()[index];
                            Ok(DirectoryEntry::new(child.0.clone().into(), kind, digest))
                        }
                        serialization::ReferenceIndexOrInlineContent::Direct(_vec) => todo!(),
                    }
                }) {
                    let entry = maybe_entry?;
                    entries.push(entry);
                }
                Ok(Arc::new(OpenDirectory::from_entries(
                    DigestStatus::new(digest.clone(), true),
                    entries,
                    storage,
                    modified,
                    clock,
                    open_file_write_buffer_in_blocks,
                )))
            }
            None => todo!(),
        }
    }

    async fn open_subdirectory(
        self: Arc<OpenDirectory>,
        name: String,
    ) -> Result<Arc<OpenDirectory>> {
        let mut state_locked = self.state.lock().await;
        match state_locked.names.get_mut(&name) {
            Some(found) => match found {
                NamedEntry::NotOpen(meta_data, digest) => match meta_data.kind {
                    DirectoryEntryKind::Directory => {
                        let subdirectory = Self::load_directory(
                            self.storage.clone(),
                            digest,
                            self.modified,
                            self.clock,
                            self.open_file_write_buffer_in_blocks,
                        )
                        .await?;
                        let receiver = subdirectory.watch().await;
                        let mut new_entry =
                            NamedEntry::OpenSubdirectory(subdirectory.clone(), receiver);
                        self.clone().watch_new_entry(&mut new_entry);
                        *found = new_entry;
                        Ok(subdirectory)
                    }
                    DirectoryEntryKind::File(_) => {
                        Err(Error::CannotOpenRegularFileAsDirectory(name.to_string()))
                    }
                },
                NamedEntry::OpenRegularFile(_, _) => {
                    Err(Error::CannotOpenRegularFileAsDirectory(name.to_string()))
                }
                NamedEntry::OpenSubdirectory(subdirectory, _) => Ok(subdirectory.clone()),
            },
            None => Err(Error::NotFound(name.to_string())),
        }
    }

    pub async fn open_directory(
        self: &Arc<OpenDirectory>,
        path: NormalizedPath,
    ) -> Result<Arc<OpenDirectory>> {
        match path.split_left() {
            PathSplitLeftResult::Root => Ok(self.clone()),
            PathSplitLeftResult::Leaf(name) => self.clone().open_subdirectory(name).await,
            PathSplitLeftResult::Directory(directory_name, tail) => {
                let subdirectory = self.clone().open_subdirectory(directory_name).await?;
                Box::pin(subdirectory.open_directory(tail)).await
            }
        }
    }

    pub async fn create_directory(
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        clock: WallClock,
        open_file_write_buffer_in_blocks: usize,
    ) -> Result<OpenDirectory> {
        let value_blob = TreeBlob::try_from(bytes::Bytes::from(
            postcard::to_allocvec(&DirectoryTree {
                children: BTreeMap::new(),
            })
            .unwrap(),
        ))
        .unwrap();
        debug!("Storing empty directory");
        let empty_directory_digest = match storage
            .store_value(&HashedValue::from(Arc::new(Tree::new(value_blob, vec![]))))
            .await
        {
            Ok(success) => success,
            Err(error) => return Err(Error::Storage(error)),
        };
        Ok(OpenDirectory::new(
            DigestStatus::new(empty_directory_digest, true),
            BTreeMap::new(),
            storage,
            (clock)(),
            clock,
            open_file_write_buffer_in_blocks,
        ))
    }

    pub async fn create_subdirectory(
        self: Arc<OpenDirectory>,
        name: String,
        empty_directory_digest: BlobDigest,
    ) -> Result<()> {
        let mut state_locked = self.state.lock().await;
        match state_locked.names.get(&name) {
            Some(_found) => todo!(),
            None => {
                info!(
                    "Creating directory {} sends a change event for its parent directory.",
                    &name
                );
                let directory = Self::load_directory(
                    self.storage.clone(),
                    &empty_directory_digest,
                    (self.clock)(),
                    self.clock,
                    self.open_file_write_buffer_in_blocks,
                )
                .await?;
                let receiver = directory.watch().await;
                self.clone().insert_entry(
                    &mut state_locked,
                    name,
                    NamedEntry::OpenSubdirectory(directory, receiver),
                );
                Self::notify_about_change(&mut state_locked, &self.change_event_sender).await;
                Ok(())
            }
        }
    }

    pub async fn remove(&self, name_here: &str) -> Result<()> {
        let mut state_locked = self.state.lock().await;
        if !state_locked.names.contains_key(name_here) {
            return Err(Error::NotFound(name_here.to_string()));
        }

        state_locked.names.remove(name_here);
        Self::notify_about_change(&mut state_locked, &self.change_event_sender).await;
        Ok(())
    }

    pub async fn copy(
        self: Arc<OpenDirectory>,
        name_here: &str,
        there: &OpenDirectory,
        name_there: &str,
    ) -> Result<()> {
        let mut state_locked: MutexGuard<'_, _>;
        let mut state_there_locked: Option<MutexGuard<'_, _>>;

        let comparison = std::ptr::from_ref(&*self).cmp(&std::ptr::from_ref(there));
        match comparison {
            std::cmp::Ordering::Less => {
                state_locked = self.state.lock().await;
                state_there_locked = Some(there.state.lock().await);
            }
            std::cmp::Ordering::Equal => {
                state_locked = self.state.lock().await;
                state_there_locked = None;
            }
            std::cmp::Ordering::Greater => {
                state_there_locked = Some(there.state.lock().await);
                state_locked = self.state.lock().await;
            }
        }

        match state_locked.names.get(name_here) {
            Some(_) => {}
            None => return Err(Error::NotFound(name_here.to_string())),
        }

        debug!(
            "Copying from {} to {} sending a change event to the directory.",
            name_here, name_there
        );

        let old_entry = state_locked.names.get(name_here).unwrap();
        let new_entry = Self::copy_named_entry(old_entry, self.clock)
            .await
            .map_err(|error| Error::Storage(error))?;
        match state_there_locked {
            Some(ref mut value) => {
                Self::write_into_directory(self.clone(), value, name_there, new_entry)
            }
            None => {
                Self::write_into_directory(self.clone(), &mut state_locked, name_there, new_entry)
            }
        }

        if state_there_locked.is_some() {
            Self::notify_about_change(&mut state_there_locked.unwrap(), &there.change_event_sender)
                .await;
        }
        Self::notify_about_change(&mut state_locked, &self.change_event_sender).await;
        Ok(())
    }

    async fn copy_named_entry(
        original: &NamedEntry,
        clock: WallClock,
    ) -> std::result::Result<NamedEntry, StoreError> {
        match original {
            NamedEntry::NotOpen(directory_entry_meta_data, blob_digest) => Ok(NamedEntry::NotOpen(
                *directory_entry_meta_data,
                *blob_digest,
            )),
            NamedEntry::OpenRegularFile(open_file, _receiver) => {
                let status = open_file.flush().await?;
                assert!(status.digest.is_digest_up_to_date);
                Ok(NamedEntry::NotOpen(
                    DirectoryEntryMetaData::new(
                        DirectoryEntryKind::File(status.last_known_digest_file_size),
                        clock(),
                    ),
                    status.digest.last_known_digest,
                ))
            }
            NamedEntry::OpenSubdirectory(_arc, _receiver) => todo!(),
        }
    }

    pub async fn rename(
        self: Arc<OpenDirectory>,
        name_here: &str,
        there: &OpenDirectory,
        name_there: &str,
    ) -> Result<()> {
        let mut state_locked: MutexGuard<'_, _>;
        let mut state_there_locked: Option<MutexGuard<'_, _>>;

        let comparison = std::ptr::from_ref(&*self).cmp(&std::ptr::from_ref(there));
        match comparison {
            std::cmp::Ordering::Less => {
                state_locked = self.state.lock().await;
                state_there_locked = Some(there.state.lock().await);
            }
            std::cmp::Ordering::Equal => {
                state_locked = self.state.lock().await;
                state_there_locked = None;
            }
            std::cmp::Ordering::Greater => {
                state_there_locked = Some(there.state.lock().await);
                state_locked = self.state.lock().await;
            }
        }

        match state_locked.names.get(name_here) {
            Some(_) => {}
            None => return Err(Error::NotFound(name_here.to_string())),
        }

        info!(
            "Renaming from {} to {} sending a change event to the directory.",
            name_here, name_there
        );

        let (_obsolete_name, entry) = /*TODO: stop watching the entry*/ state_locked.names.remove_entry(name_here).unwrap();
        match state_there_locked {
            Some(ref mut value) => self.clone().write_into_directory(value, name_there, entry),
            None => self
                .clone()
                .write_into_directory(&mut state_locked, name_there, entry),
        }

        Self::notify_about_change(&mut state_locked, &self.change_event_sender).await;
        if let Some(ref mut state_there) = state_there_locked {
            Self::notify_about_change(state_there, &there.change_event_sender).await;
        }
        Ok(())
    }

    fn write_into_directory(
        self: Arc<OpenDirectory>,
        state: &mut MutexGuard<'_, OpenDirectoryMutableState>,
        name_there: &str,
        entry: NamedEntry,
    ) {
        match state.names.get_mut(name_there) {
            Some(existing_name) => *existing_name = entry,
            None => {
                self.insert_entry(state, name_there.to_string(), entry);
            }
        };
    }

    pub async fn watch(&self) -> tokio::sync::watch::Receiver<OpenDirectoryStatus> {
        self.change_event_sender.subscribe()
    }

    pub fn request_save<'t>(&'t self) -> Future<'t, OpenDirectoryStatus> {
        Box::pin(async move {
            let mut state_locked = self.state.lock().await;
            Self::consider_saving_and_updating_status(
                &self.change_event_sender,
                &mut state_locked,
                self.storage.clone(),
            )
            .await
        })
    }

    async fn notify_about_change(
        state_locked: &mut OpenDirectoryMutableState,
        change_event_sender: &tokio::sync::watch::Sender<OpenDirectoryStatus>,
    ) {
        if state_locked.has_unsaved_changes {
            debug!("Directory had unsaved changes already.");
        } else {
            debug!("Directory has unsaved changes now.");
            state_locked.has_unsaved_changes = true;
        }
        change_event_sender.send_if_modified(|last_status| {
            if last_status.digest.is_digest_up_to_date {
                last_status.digest.is_digest_up_to_date = false;
                last_status.directories_unsaved_count += 1;
                true
            } else {
                false
            }
        });
    }

    async fn consider_saving_and_updating_status(
        change_event_sender: &tokio::sync::watch::Sender<OpenDirectoryStatus>,
        state_locked: &mut OpenDirectoryMutableState,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<OpenDirectoryStatus> {
        let digest: Option<BlobDigest> = Self::consider_saving(state_locked, storage).await?;
        Ok(Self::update_status(change_event_sender, state_locked, digest).await)
    }

    async fn consider_saving(
        state_locked: &mut OpenDirectoryMutableState,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<Option<BlobDigest>> {
        if state_locked.has_unsaved_changes {
            debug!("We should save this directory.");
            for entry in state_locked.names.iter() {
                entry.1.request_save().await?;
            }
            let saved = Self::save(state_locked, storage).await.unwrap(/*TODO*/);
            assert!(state_locked.has_unsaved_changes);
            state_locked.has_unsaved_changes = false;
            Ok(Some(saved))
        } else {
            debug!("Nothing to save for this directory.");
            Ok(None)
        }
    }

    async fn update_status(
        change_event_sender: &tokio::sync::watch::Sender<OpenDirectoryStatus>,
        state_locked: &mut OpenDirectoryMutableState,
        new_digest: Option<BlobDigest>,
    ) -> OpenDirectoryStatus {
        let mut directories_open_count: usize= /*count self*/ 1;
        let mut directories_unsaved_count: usize = 0;
        let mut files_open_count: usize = 0;
        let mut files_open_for_writing_count: usize = 0;
        let mut files_unflushed_count: usize = 0;
        let mut bytes_unflushed_count: u64 = 0;
        let mut are_children_up_to_date = true;
        for entry in state_locked.names.iter_mut() {
            let named_entry_status = entry.1.get_status();
            match named_entry_status {
                NamedEntryStatus::Closed(_directory_entry_kind, _blob_digest) => {}
                NamedEntryStatus::Open(open_named_entry_status) => match open_named_entry_status {
                    OpenNamedEntryStatus::Directory(open_directory_status) => {
                        directories_open_count += open_directory_status.directories_open_count;
                        directories_unsaved_count +=
                            open_directory_status.directories_unsaved_count;
                        files_open_count += open_directory_status.files_open_count;
                        files_open_for_writing_count =
                            open_directory_status.files_open_for_writing_count;
                        files_unflushed_count += open_directory_status.files_unflushed_count;
                        bytes_unflushed_count += open_directory_status.bytes_unflushed_count;
                        if !open_directory_status.digest.is_digest_up_to_date {
                            debug!("Child directory is not up to date.");
                            are_children_up_to_date = false;
                        }
                    }
                    OpenNamedEntryStatus::File(open_file_status) => {
                        files_open_count += 1;
                        if open_file_status.is_open_for_writing {
                            files_open_for_writing_count += 1;
                        }
                        if open_file_status.bytes_unflushed_count > 0 {
                            files_unflushed_count += 1;
                        }
                        bytes_unflushed_count += open_file_status.bytes_unflushed_count;
                        if !open_file_status.digest.is_digest_up_to_date {
                            debug!("Child file is not up to date.");
                            are_children_up_to_date = false;
                        }
                    }
                },
            }
        }
        let is_up_to_date = are_children_up_to_date && !state_locked.has_unsaved_changes;
        if !is_up_to_date {
            debug!("Some children are not up to date, so this directory has unsaved changes.");
            directories_unsaved_count += 1;
            state_locked.has_unsaved_changes = true;
        }
        change_event_sender.send_if_modified(|last_status| {
            let digest = match new_digest {
                Some(new_digest) => DigestStatus::new(new_digest, is_up_to_date),
                None => DigestStatus::new(last_status.digest.last_known_digest, is_up_to_date),
            };
            let status = OpenDirectoryStatus::new(
                digest,
                directories_open_count,
                directories_unsaved_count,
                files_open_count,
                files_open_for_writing_count,
                files_unflushed_count,
                bytes_unflushed_count,
            );
            if *last_status == status {
                debug!(
                    "Not sending directory status because it didn't change: {:?}",
                    &status
                );
                false
            } else {
                debug!("Sending directory status: {:?}", &status);
                *last_status = status;
                true
            }
        });
        *change_event_sender.borrow()
    }

    async fn save(
        state_locked: &mut OpenDirectoryMutableState,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<BlobDigest, StoreError> {
        let mut serialization_children = std::collections::BTreeMap::new();
        let mut serialization_references = Vec::new();
        for entry in state_locked.names.iter_mut() {
            let name = FileName::try_from(entry.0.as_str()).unwrap();
            let named_entry_status = entry.1.get_status();
            let (kind, digest) = match named_entry_status {
                NamedEntryStatus::Closed(directory_entry_kind, blob_digest) => {
                    (directory_entry_kind, blob_digest)
                }
                NamedEntryStatus::Open(open_named_entry_status) => match open_named_entry_status {
                    OpenNamedEntryStatus::Directory(open_directory_status) => (
                        serialization::DirectoryEntryKind::Directory,
                        open_directory_status.digest.last_known_digest,
                    ),
                    OpenNamedEntryStatus::File(open_file_status) => (
                        serialization::DirectoryEntryKind::File(
                            open_file_status.last_known_digest_file_size,
                        ),
                        open_file_status.digest.last_known_digest,
                    ),
                },
            };
            let reference_index = ReferenceIndex(serialization_references.len() as u64);
            serialization_references.push(digest);
            serialization_children.insert(
                name,
                serialization::DirectoryEntry {
                    kind: kind,
                    content: serialization::ReferenceIndexOrInlineContent::Indirect(
                        reference_index,
                    ),
                },
            );
        }
        if serialization_children.len() > 5 {
            info!(
                "Saving directory with {} entries",
                serialization_children.len()
            );
            debug!("Saving directory: {:?}", &serialization_children);
        } else {
            info!("Saving directory: {:?}", &serialization_children);
        }
        let maybe_value_blob = TreeBlob::try_from(bytes::Bytes::from(
            postcard::to_allocvec(&DirectoryTree {
                children: serialization_children,
            })
            .unwrap(),
        ));
        match maybe_value_blob {
            Some(value_blob) => {
                storage
                    .store_value(&HashedValue::from(Arc::new(Tree::new(
                        value_blob,
                        serialization_references,
                    ))))
                    .await
            }
            None => todo!(),
        }
    }

    //#[instrument(skip_all)]
    pub async fn drop_all_read_caches(&self) -> CacheDropStats {
        let mut state_locked = self.state.lock().await;
        let mut result = CacheDropStats::new(0, 0, 0);
        for (_name, entry) in state_locked.names.iter_mut() {
            result.add(&entry.drop_all_read_caches().await);
        }
        result
    }
}

pub enum PathSplitLeftResult {
    Root,
    Leaf(String),
    Directory(String, NormalizedPath),
}

pub enum PathSplitRightResult {
    Root,
    Entry(NormalizedPath, String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedPath {
    components: VecDeque<String>,
}

impl NormalizedPath {
    pub fn new(input: &relative_path::RelativePath) -> NormalizedPath {
        NormalizedPath {
            components: input
                .normalize()
                .components()
                .map(|component| match component {
                    relative_path::Component::CurDir => todo!(),
                    relative_path::Component::ParentDir => todo!(),
                    relative_path::Component::Normal(name) => name.to_string(),
                })
                .collect(),
        }
    }

    pub fn root() -> NormalizedPath {
        NormalizedPath {
            components: VecDeque::new(),
        }
    }

    pub fn split_left(mut self) -> PathSplitLeftResult {
        let head = match self.components.pop_front() {
            Some(head) => head,
            None => return PathSplitLeftResult::Root,
        };
        if self.components.is_empty() {
            PathSplitLeftResult::Leaf(head)
        } else {
            PathSplitLeftResult::Directory(
                head,
                NormalizedPath {
                    components: self.components,
                },
            )
        }
    }

    pub fn split_right(mut self) -> PathSplitRightResult {
        let tail = match self.components.pop_back() {
            Some(tail) => tail,
            None => return PathSplitRightResult::Root,
        };
        PathSplitRightResult::Entry(self, tail)
    }
}

#[derive(PartialEq, Debug, Copy, Clone, PartialOrd, Ord, Eq)]
pub struct DigestStatus {
    pub last_known_digest: BlobDigest,
    pub is_digest_up_to_date: bool,
}

impl DigestStatus {
    pub fn new(last_known_digest: BlobDigest, is_digest_up_to_date: bool) -> Self {
        Self {
            last_known_digest,
            is_digest_up_to_date,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct OpenFileStatus {
    pub digest: DigestStatus,
    pub size: u64,
    pub last_known_digest_file_size: u64,
    pub is_open_for_writing: bool,
    pub bytes_unflushed_count: u64,
}

impl OpenFileStatus {
    pub fn new(
        digest: DigestStatus,
        size: u64,
        last_known_digest_file_size: u64,
        is_open_for_writing: bool,
        bytes_unflushed_count: u64,
    ) -> Self {
        Self {
            digest,
            size,
            last_known_digest_file_size,
            is_open_for_writing,
            bytes_unflushed_count,
        }
    }
}

#[derive(Debug)]
pub struct WriteResult {
    remaining: bytes::Bytes,
}

impl WriteResult {
    pub fn new(remaining: bytes::Bytes) -> Self {
        Self { remaining }
    }
}

#[derive(PartialEq)]
pub enum LoadedBlock {
    KnownDigest(HashedValue),
    UnknownDigest(Vec<u8>),
}

impl std::fmt::Debug for LoadedBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KnownDigest(arg0) => f.debug_tuple("KnownDigest").field(arg0).finish(),
            Self::UnknownDigest(arg0) => f
                .debug_tuple("UnknownDigest.0.len()")
                .field(&arg0.len())
                .finish(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum OpenFileContentBlock {
    NotLoaded(BlobDigest, u16),
    Loaded(LoadedBlock),
}

impl OpenFileContentBlock {
    pub fn prepare_for_reading<'t>(
        &self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Option<Future<'t, HashedValue>> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, size) => {
                let blob_digest = *blob_digest;
                let size = *size;
                Some(Box::pin(async move {
                    Self::load(&blob_digest, size, storage).await
                }))
            }
            OpenFileContentBlock::Loaded(_loaded_block) => None,
        }
    }

    pub fn set_prepare_for_reading_result(&mut self, prepared: HashedValue) {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, _size) => {
                assert_eq!(blob_digest, prepared.digest())
            }
            OpenFileContentBlock::Loaded(loaded) => match loaded {
                LoadedBlock::KnownDigest(_hashed_value) => assert!(false),
                LoadedBlock::UnknownDigest(_vec) => assert!(false),
            },
        }
        *self = OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(prepared));
    }

    async fn load(
        blob_digest: &BlobDigest,
        size: u16,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<HashedValue> {
        let loaded = if size == 0 {
            // there is nothing to load
            HashedValue::from(Arc::new(Tree::new(TreeBlob::empty(), Vec::new())))
        } else {
            let delayed = match storage.load_value(blob_digest).await {
                Some(success) => success,
                None => return Err(Error::MissingValue(*blob_digest)),
            };
            let hashed = tokio::task::spawn_blocking(move || delayed.hash())
                .await
                .unwrap();
            match hashed {
                Some(success) => success,
                None => return Err(Error::MissingValue(*blob_digest)),
            }
        };
        if loaded.value().blob().as_slice().len() != size as usize {
            error!(
                "Loaded blob {:?} of size {}, but it was expected to be {} long",
                blob_digest,
                loaded.value().blob().as_slice().len(),
                size
            );
            return Err(Error::FileSizeMismatch);
        }
        if !loaded.value().references().is_empty() {
            error!(
                "Loaded blob {:?} of size {}, and its size was correct, but it had unexpected references (number: {}).",
                blob_digest,
                size, loaded.value().references().len()
            );
            return Err(Error::TooManyReferences(*blob_digest));
        }
        Ok(loaded)
    }

    pub async fn access_content_for_reading<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, size) => {
                let loaded = Self::load(&blob_digest, *size, storage).await?;
                *self = OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(loaded));
            }
            OpenFileContentBlock::Loaded(_) => {}
        }
        Ok(match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(loaded) => match loaded {
                LoadedBlock::KnownDigest(hashed_value) => {
                    hashed_value.value().blob().content.clone()
                }
                LoadedBlock::UnknownDigest(vec) => bytes::Bytes::copy_from_slice(&vec),
            },
        })
    }

    pub async fn access_content_for_writing<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut Vec<u8>> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, size) => {
                let loaded = Self::load(&blob_digest, *size, storage).await?;
                *self = OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(loaded));
            }
            OpenFileContentBlock::Loaded(_) => {}
        }
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(loaded) => match loaded {
                LoadedBlock::KnownDigest(hashed_value) => {
                    *loaded =
                        LoadedBlock::UnknownDigest(hashed_value.value().blob().as_slice().to_vec());
                }
                LoadedBlock::UnknownDigest(_vec) => {}
            },
        }
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(loaded) => match loaded {
                LoadedBlock::KnownDigest(_hashed_value) => {
                    panic!()
                }
                LoadedBlock::UnknownDigest(vec) => Ok(vec),
            },
        }
    }

    pub async fn write(
        &mut self,
        position_in_block: u16,
        buf: bytes::Bytes,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<WriteResult> {
        let data = self.access_content_for_writing(storage).await?;
        let (mut for_extending, overwritten) =
            match data.split_at_mut_checked(position_in_block as usize) {
                Some((_, overwriting)) => {
                    let can_overwrite = usize::min(overwriting.len(), buf.len());
                    let mut for_overwriting = buf.clone();
                    let for_extending = for_overwriting.split_off(can_overwrite);
                    for_overwriting.copy_to_slice(overwriting.split_at_mut(can_overwrite).0);
                    (for_extending, can_overwrite)
                }
                None => {
                    let previous_content_length = data.len();
                    let zeroes = position_in_block as usize - (previous_content_length as usize);
                    data.extend(std::iter::repeat(0u8).take(zeroes));
                    (buf.clone(), 0)
                }
            };
        let remaining_capacity: u16 = (VALUE_BLOB_MAX_LENGTH as u16 - (data.len() as u16)) as u16;
        let extension_size = usize::min(for_extending.len(), remaining_capacity as usize);
        let rest = for_extending.split_off(extension_size);
        assert_eq!(buf.len(), (overwritten + extension_size + rest.len()));
        data.extend(for_extending);
        Ok(WriteResult::new(rest))
    }

    pub async fn try_store(
        &mut self,
        is_allowed_to_calculate_digest: bool,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<Option<BlobDigest>, StoreError> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, _) => Ok(Some(*blob_digest)),
            OpenFileContentBlock::Loaded(loaded) => {
                let hashed_value = match loaded {
                    LoadedBlock::KnownDigest(hashed_value) => hashed_value.clone(),
                    LoadedBlock::UnknownDigest(vec) => {
                        assert!(vec.len() <= VALUE_BLOB_MAX_LENGTH);
                        if !is_allowed_to_calculate_digest {
                            return Ok(None);
                        }
                        debug!("Calculating unknown digest of size {}", vec.len());
                        let hashed_value = HashedValue::from(Arc::new(Tree::new(
                            TreeBlob::try_from( bytes::Bytes::from(vec.clone() /*TODO: avoid clone*/)).unwrap(/*TODO*/),
                            vec![],
                        )));
                        hashed_value
                    }
                };
                let size = hashed_value.value().blob().len();
                let result = storage.store_value(&hashed_value).await?;
                assert_eq!(hashed_value.digest(), &result);
                // free the memory
                *self = OpenFileContentBlock::NotLoaded(result, size);
                Ok(Some(result))
            }
        }
    }

    pub fn size(&self) -> u16 {
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, size) => *size,
            OpenFileContentBlock::Loaded(loaded) => match loaded {
                LoadedBlock::KnownDigest(hashed_value) => hashed_value.value().blob().len(),
                LoadedBlock::UnknownDigest(vec) => vec.len() as u16,
            },
        }
    }

    async fn drop_all_read_caches(&mut self) -> CacheDropStats {
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => CacheDropStats::new(0, 0, 0),
            OpenFileContentBlock::Loaded(loaded_block) => match loaded_block {
                LoadedBlock::KnownDigest(hashed_value) => {
                    // free some memory:
                    *self = OpenFileContentBlock::NotLoaded(
                        *hashed_value.digest(),
                        hashed_value.value().blob().len(),
                    );
                    CacheDropStats::new(1, 0, 0)
                }
                LoadedBlock::UnknownDigest(_vec) => CacheDropStats::new(0, 0, 0),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum StreakDirection {
    Up,
    Down,
    Neither,
}

impl StreakDirection {
    fn saturated_sum(size: &[AccessOrderLowerIsMoreRecent]) -> usize {
        size.iter()
            .fold(0usize, |left, right| usize::saturating_add(left, right.0))
    }

    fn detect_from_block_access_order(
        block_access_order: &[AccessOrderLowerIsMoreRecent],
    ) -> StreakDirection {
        let (left, mut right) = block_access_order.split_at(block_access_order.len() / 2);
        if right.len() > left.len() {
            right = right.split_at(1).1;
        }
        assert_eq!(left.len(), right.len());
        let left_sum = Self::saturated_sum(left);
        let right_sum = Self::saturated_sum(right);
        match left_sum.cmp(&right_sum) {
            std::cmp::Ordering::Less => StreakDirection::Down,
            std::cmp::Ordering::Equal => StreakDirection::Neither,
            std::cmp::Ordering::Greater => StreakDirection::Up,
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct AccessOrderLowerIsMoreRecent(pub usize);

#[derive(Debug, PartialEq)]
pub struct Prefetcher {
    last_explicitly_requested_blocks: cached::stores::SizedCache<u64, ()>,
}

impl Prefetcher {
    pub fn new() -> Self {
        Self {
            last_explicitly_requested_blocks: cached::stores::SizedCache::with_size(16),
        }
    }

    pub fn max_number_of_blocks_tracked(&self) -> usize {
        self.last_explicitly_requested_blocks
            .cache_capacity()
            .unwrap()
    }

    pub fn add_explicitly_requested_block(&mut self, requested_block_index: u64) -> bool {
        self.last_explicitly_requested_blocks
            .cache_set(requested_block_index, ())
            .is_none()
    }

    pub fn get_prefetch_length_from_streak_length(streak_length: usize) -> usize {
        let max_prefetch_length = 24;
        std::cmp::min(max_prefetch_length, streak_length * 2)
    }

    pub fn analyze_streak(
        highest_block_index_in_streak: u64,
        streak_order: &[AccessOrderLowerIsMoreRecent],
        total_block_count: u64,
    ) -> BTreeSet<u64> {
        let mut blocks_to_prefetch = BTreeSet::new();
        let streak_direction = StreakDirection::detect_from_block_access_order(streak_order);
        match streak_direction {
            StreakDirection::Down => {
                let lowest_block_index_in_streak =
                    highest_block_index_in_streak + 1 - streak_order.len() as u64;
                for offset in 0..Self::get_prefetch_length_from_streak_length(streak_order.len()) {
                    match u64::checked_sub(lowest_block_index_in_streak, (offset + 1) as u64) {
                        Some(prefetched) => {
                            blocks_to_prefetch.insert(prefetched);
                        }
                        None => break,
                    }
                }
            }
            StreakDirection::Up => {
                for offset in 0..Self::get_prefetch_length_from_streak_length(streak_order.len()) {
                    match u64::checked_add(highest_block_index_in_streak, (offset + 1) as u64) {
                        Some(prefetched) => {
                            if prefetched >= total_block_count {
                                break;
                            }
                            blocks_to_prefetch.insert(prefetched);
                        }
                        None => break,
                    }
                }
            }
            StreakDirection::Neither => {
                debug!("Streak neither: {:?}", streak_order);
            }
        }
        blocks_to_prefetch
    }

    pub fn get_blocks_to_access_order(&self) -> BTreeMap<u64, AccessOrderLowerIsMoreRecent> {
        let recently_read_blocks = BTreeMap::from_iter(
            self.last_explicitly_requested_blocks
                .key_order()
                .copied()
                .enumerate()
                .map(|(order, block_index)| (block_index, AccessOrderLowerIsMoreRecent(order))),
        );
        assert_eq!(
            self.last_explicitly_requested_blocks.cache_size(),
            recently_read_blocks.len()
        );
        recently_read_blocks
    }

    pub fn find_blocks_to_prefetch(&self, total_block_count: u64) -> BTreeSet<u64> {
        let recently_read_blocks = self.get_blocks_to_access_order();
        let mut blocks_to_prefetch = BTreeSet::new();
        let mut maybe_previous_block_index = None;
        let mut streak_order = Vec::new();
        for (block_index, order) in recently_read_blocks {
            match maybe_previous_block_index {
                Some(previous_block_index) => {
                    if previous_block_index + 1 != block_index {
                        blocks_to_prefetch.append(&mut Self::analyze_streak(
                            previous_block_index,
                            &streak_order,
                            total_block_count,
                        ));
                        streak_order.clear();
                    }
                }
                None => {}
            }
            streak_order.push(order);
            maybe_previous_block_index = Some(block_index);
        }
        if let Some(previous_block_index) = maybe_previous_block_index {
            blocks_to_prefetch.append(&mut Self::analyze_streak(
                previous_block_index,
                &streak_order,
                total_block_count,
            ));
        }
        blocks_to_prefetch
    }

    //#[instrument(skip_all)]
    pub async fn prefetch(
        &mut self,
        blocks: &mut Vec<OpenFileContentBlock>,
        explicitly_requested_blocks_right_now: std::ops::Range<u64>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        for index in explicitly_requested_blocks_right_now
            .clone()
            .take(self.max_number_of_blocks_tracked())
        {
            self.add_explicitly_requested_block(index);
        }
        let blocks_to_prefetch = self.find_blocks_to_prefetch(blocks.len() as u64);
        let blocks_to_prefetch_count = blocks_to_prefetch.len();

        let mut blocks_to_load = blocks_to_prefetch;
        blocks_to_load.extend(
            explicitly_requested_blocks_right_now.take(self.max_number_of_blocks_tracked()),
        );

        let futures: Vec<Future<(u64, Result<HashedValue>)>> = blocks_to_load
            .into_iter()
            .map(|block_index| {
                let block = &mut blocks[block_index as usize];
                let result: Option<Future<(u64, Result<HashedValue>)>> = block
                    .prepare_for_reading(storage.clone())
                    .map(|future: Future<HashedValue>| {
                        let result2: Future<(u64, Result<HashedValue>)> =
                            Box::pin(async move { Ok((block_index, future.await)) });
                        result2
                    });
                result
            })
            .filter_map(|maybe_future| maybe_future)
            .collect();

        if futures.len() < (blocks_to_prefetch_count / 2) {
            return;
        }

        let joined: Vec<Result<(u64, Result<HashedValue>)>> = join_all(futures).await;
        for join_result in joined.into_iter() {
            let (block_index, prepare_result) = join_result.unwrap();
            let prepared = match prepare_result {
                Ok(success) => success,
                Err(_) => todo!(),
            };
            let block = &mut blocks[block_index as usize];
            block.set_prepare_for_reading_result(prepared);
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct OpenFileContentBufferLoaded {
    size: u64,
    blocks: Vec<OpenFileContentBlock>,
    digest: DigestStatus,
    last_known_digest_file_size: u64,
    dirty_blocks: VecDeque<usize>,
    write_buffer_in_blocks: usize,
    prefetcher: Prefetcher,
}

impl OpenFileContentBufferLoaded {
    pub fn new(
        size: u64,
        blocks: Vec<OpenFileContentBlock>,
        digest: DigestStatus,
        last_known_digest_file_size: u64,
        dirty_blocks: VecDeque<usize>,
        write_buffer_in_blocks: usize,
        prefetcher: Prefetcher,
    ) -> Self {
        Self {
            size,
            blocks,
            digest,
            last_known_digest_file_size,
            dirty_blocks,
            write_buffer_in_blocks,
            prefetcher,
        }
    }

    pub fn last_known_digest(&self) -> DigestStatus {
        self.digest
    }

    //#[instrument(skip_all)]
    pub async fn store_cheap_blocks(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<(), StoreError> {
        debug!(
            "store_cheap_blocks, {} dirty blocks",
            self.dirty_blocks.len()
        );
        self.verify_integrity();
        let mut skipped = 0;
        loop {
            let index = match self.dirty_blocks.get(skipped) {
                Some(index) => *index,
                None => break,
            };
            let block = &mut self.blocks[index];
            let block_stored: Option<BlobDigest> = block.try_store(false, storage.clone()).await?;
            match block_stored {
                Some(_) => {
                    self.dirty_blocks.pop_front();
                }
                None => {
                    skipped += 1;
                }
            }
        }
        self.verify_integrity();
        Ok(())
    }

    fn verify_integrity(&self) {
        let length = self.blocks.len();
        for (index, block) in self.blocks.iter().enumerate() {
            assert!(block.size() <= VALUE_BLOB_MAX_LENGTH as u16);
            if index < (length - 1) {
                assert_eq!(VALUE_BLOB_MAX_LENGTH as u16, block.size());
            }
        }
    }

    //#[instrument(skip_all)]
    pub async fn store_all(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<StoreChanges, StoreError> {
        debug!("store_all, {} dirty blocks", self.dirty_blocks.len());

        let mut blocks_stored = Vec::new();
        self.verify_integrity();
        for block in self.blocks.iter_mut() {
            let block_stored = block.try_store(true, storage.clone()).await?;
            blocks_stored.push(block_stored.unwrap());
        }
        self.verify_integrity();
        self.dirty_blocks.clear();
        assert!(blocks_stored.len() >= 1);
        if blocks_stored.len() == 1 {
            return Ok(self.update_digest(blocks_stored[0]));
        }
        let info = SegmentedBlob {
            size_in_bytes: self.size,
        };
        let value = Tree::new(
            TreeBlob::try_from(bytes::Bytes::from(postcard::to_allocvec(&info).unwrap())).unwrap(),
            blocks_stored,
        );
        let reference = storage
            .store_value(&HashedValue::from(Arc::new(value)))
            .await?;
        Ok(self.update_digest(reference))
    }

    fn update_digest(&mut self, new_digest: BlobDigest) -> StoreChanges {
        let old_digest = self.digest;
        self.digest = DigestStatus::new(new_digest, true);
        self.last_known_digest_file_size = self.size;
        if old_digest == self.digest {
            StoreChanges::NoChanges
        } else {
            StoreChanges::SomeChanges
        }
    }

    async fn drop_all_read_caches(&mut self) -> CacheDropStats {
        let mut result = CacheDropStats::new(0, 0, 0);
        for block in self.blocks.iter_mut() {
            result.add(&block.drop_all_read_caches().await);
        }
        assert_eq!(0, result.open_files_closed);
        assert_eq!(0, result.open_directories_closed);
        result
    }
}

#[derive(PartialEq, Debug)]
pub enum StoreChanges {
    SomeChanges,
    NoChanges,
}

#[derive(Debug)]
pub struct OptimizedWriteBuffer {
    // less than VALUE_BLOB_MAX_LENGTH
    prefix: bytes::Bytes,
    // each one is exactly VALUE_BLOB_MAX_LENGTH
    full_blocks: Vec<HashedValue>,
    // less than VALUE_BLOB_MAX_LENGTH
    suffix: bytes::Bytes,
}

impl OptimizedWriteBuffer {
    pub fn prefix(&self) -> &bytes::Bytes {
        &self.prefix
    }

    pub fn full_blocks(&self) -> &Vec<HashedValue> {
        &self.full_blocks
    }

    pub fn suffix(&self) -> &bytes::Bytes {
        &self.suffix
    }

    //#[instrument(skip(content))]
    pub async fn from_bytes(write_position: u64, content: bytes::Bytes) -> OptimizedWriteBuffer {
        let first_block_offset = (write_position % VALUE_BLOB_MAX_LENGTH as u64) as usize;
        let first_block_capacity = VALUE_BLOB_MAX_LENGTH - first_block_offset;
        let mut block_aligned_content = content.clone();
        let prefix = if (first_block_offset == 0)
            && (block_aligned_content.len() >= VALUE_BLOB_MAX_LENGTH)
        {
            bytes::Bytes::new()
        } else {
            let prefix = block_aligned_content.split_to(std::cmp::min(
                block_aligned_content.len(),
                first_block_capacity,
            ));
            assert!(prefix.len() <= first_block_capacity);
            assert!((first_block_offset + prefix.len()) <= VALUE_BLOB_MAX_LENGTH);
            prefix
        };
        let mut full_block_hashing: Vec<tokio::task::JoinHandle<HashedValue>> = Vec::new();
        loop {
            if block_aligned_content.len() < VALUE_BLOB_MAX_LENGTH {
                let mut full_blocks = Vec::new();
                full_blocks.reserve(full_block_hashing.len());
                for handle in full_block_hashing.into_iter() {
                    full_blocks.push(handle.await.unwrap());
                }
                let result = OptimizedWriteBuffer {
                    prefix: prefix,
                    full_blocks: full_blocks,
                    suffix: block_aligned_content,
                };
                assert!((first_block_offset + result.prefix.len()) <= VALUE_BLOB_MAX_LENGTH);
                assert!(result.prefix.len() < VALUE_BLOB_MAX_LENGTH);
                assert!(result.suffix.len() < VALUE_BLOB_MAX_LENGTH);
                assert_eq!(content.len(), result.len());
                return result;
            }
            let next = block_aligned_content.split_to(VALUE_BLOB_MAX_LENGTH);

            // Calculating the SHA-3 digest of 64 KB of data can take surprisingly long, especially in Debug mode.
            // Parallelizing the computations should save a lot of time.
            let blocking_task = tokio::task::spawn_blocking(|| {
                HashedValue::from(Arc::new(Tree::new(
                    TreeBlob::try_from(next).unwrap(),
                    vec![],
                )))
            });
            full_block_hashing.push(blocking_task);
        }
    }

    pub fn len(&self) -> usize {
        self.prefix.len() + (self.full_blocks.len() * VALUE_BLOB_MAX_LENGTH) + self.suffix.len()
    }
}

#[derive(Debug, PartialEq)]
pub enum OpenFileContentBuffer {
    NotLoaded {
        digest: BlobDigest,
        size: u64,
        write_buffer_in_blocks: usize,
    },
    Loaded(OpenFileContentBufferLoaded),
}

impl OpenFileContentBuffer {
    pub fn from_storage(digest: BlobDigest, size: u64, write_buffer_in_blocks: usize) -> Self {
        Self::NotLoaded {
            digest: digest,
            size: size,
            write_buffer_in_blocks,
        }
    }

    pub fn from_data(
        data: Vec<u8>,
        last_known_digest: BlobDigest,
        last_known_digest_file_size: u64,
        write_buffer_in_blocks: usize,
    ) -> Option<Self> {
        if data.len() > VALUE_BLOB_MAX_LENGTH {
            None
        } else {
            let size = data.len() as u64;
            Some(Self::Loaded(OpenFileContentBufferLoaded {
                size: size,
                blocks: vec![OpenFileContentBlock::Loaded(LoadedBlock::UnknownDigest(
                    data,
                ))],
                digest: DigestStatus::new(last_known_digest, false),
                last_known_digest_file_size,
                dirty_blocks: vec![0].into(),
                write_buffer_in_blocks,
                prefetcher: Prefetcher::new(),
            }))
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size,
                write_buffer_in_blocks: _,
            } => *size,
            OpenFileContentBuffer::Loaded(OpenFileContentBufferLoaded {
                size,
                blocks: _,
                digest: _,
                last_known_digest_file_size: _,
                dirty_blocks: _,
                write_buffer_in_blocks: _,
                prefetcher: _,
            }) => *size,
        }
    }

    pub fn unsaved_blocks(&self) -> u64 {
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size: _,
                write_buffer_in_blocks: _,
            } => 0,
            OpenFileContentBuffer::Loaded(OpenFileContentBufferLoaded {
                size: _,
                blocks: _,
                digest: _,
                last_known_digest_file_size: _,
                dirty_blocks,
                write_buffer_in_blocks: _,
                prefetcher: _,
            }) => dirty_blocks.len() as u64,
        }
    }

    pub fn last_known_digest(&self) -> (DigestStatus, u64) {
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest,
                size,
                write_buffer_in_blocks: _,
            } => (DigestStatus::new(*digest, true), *size),
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => (
                open_file_content_buffer_loaded.last_known_digest(),
                open_file_content_buffer_loaded.last_known_digest_file_size,
            ),
        }
    }

    pub async fn read(
        &mut self,
        position: u64,
        count: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        let mut loaded = self.require_loaded(storage.clone()).await?;
        Self::read_from_blocks(&mut loaded, position, count, storage).await
    }

    async fn require_loaded<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut OpenFileContentBufferLoaded> {
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest,
                size,
                write_buffer_in_blocks,
            } => {
                let blocks = if *size <= VALUE_BLOB_MAX_LENGTH as u64 {
                    vec![OpenFileContentBlock::NotLoaded(*digest, *size as u16)]
                } else {
                    let delayed_hashed_value = match storage.load_value(digest).await {
                        Some(success) => success,
                        None => return Err(Error::MissingValue(*digest)),
                    };
                    let hashed_value = match delayed_hashed_value.hash() {
                        Some(success) => success,
                        None => return Err(Error::MissingValue(*digest)),
                    };
                    let info: SegmentedBlob =
                        match postcard::from_bytes(&hashed_value.value().blob().as_slice()) {
                            Ok(success) => success,
                            Err(error) => return Err(Error::Postcard(error)),
                        };
                    if info.size_in_bytes != *size {
                        return Err(Error::SegmentedBlobSizeMismatch {
                            digest: *digest,
                            segmented_blob_internal_size: info.size_in_bytes,
                            directory_entry_size: *size,
                        });
                    }
                    if hashed_value.value().references().len() < 1 {
                        todo!()
                    }
                    let full_blocks = hashed_value
                        .value()
                        .references()
                        .iter()
                        .take(hashed_value.value().references().len() - 1)
                        .map(|reference| {
                            OpenFileContentBlock::NotLoaded(
                                *reference,
                                VALUE_BLOB_MAX_LENGTH as u16,
                            )
                        });
                    let full_blocks_size = full_blocks.len() as u64 * VALUE_BLOB_MAX_LENGTH as u64;
                    if full_blocks_size > *size {
                        todo!()
                    }
                    let final_block_size = *size - full_blocks_size;
                    if final_block_size > VALUE_BLOB_MAX_LENGTH as u64 {
                        todo!()
                    }
                    full_blocks
                        .chain(std::iter::once(OpenFileContentBlock::NotLoaded(
                            *hashed_value.value().references().last().unwrap(),
                            final_block_size as u16,
                        )))
                        .collect()
                };
                *self = Self::Loaded(OpenFileContentBufferLoaded {
                    size: *size,
                    blocks: blocks,
                    digest: DigestStatus::new(*digest, true),
                    last_known_digest_file_size: *size,
                    dirty_blocks: VecDeque::new(),
                    write_buffer_in_blocks: *write_buffer_in_blocks,
                    prefetcher: Prefetcher::new(),
                });
            }
            OpenFileContentBuffer::Loaded(_loaded) => {}
        }
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size: _,
                write_buffer_in_blocks: _,
            } => panic!(),
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                Ok(open_file_content_buffer_loaded)
            }
        }
    }

    async fn read_from_blocks(
        loaded: &mut OpenFileContentBufferLoaded,
        position: u64,
        count: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        let block_size = VALUE_BLOB_MAX_LENGTH;
        let first_block_index = position / (block_size as u64);
        let blocks = &mut loaded.blocks;
        if first_block_index >= (blocks.len() as u64) {
            return Ok(bytes::Bytes::new());
        }
        {
            let last_block_index = std::cmp::min(
                (position + count as u64 - 1) / (block_size as u64),
                blocks.len() as u64 - 1,
            );
            loaded
                .prefetcher
                .prefetch(blocks, first_block_index..last_block_index, storage.clone())
                .await;
        }

        let block = &mut blocks[first_block_index as usize];
        let mut data = block.access_content_for_reading(storage).await?;
        let position_in_block = (position % VALUE_BLOB_MAX_LENGTH as u64) as usize;
        Ok(if position_in_block > data.len() {
            bytes::Bytes::new()
        } else {
            let mut result = data.split_off(position_in_block);
            if result.len() > count {
                result.truncate(count);
                result
            } else {
                result
            }
        })
    }

    pub async fn write(
        &mut self,
        position: u64,
        buf: OptimizedWriteBuffer,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<()> {
        debug!(
            "Write prefix {}, full blocks {}, suffix {}",
            buf.prefix.len(),
            buf.full_blocks.len(),
            buf.suffix.len()
        );
        let loaded = self.require_loaded(storage.clone()).await?;

        if loaded.dirty_blocks.len() >= loaded.write_buffer_in_blocks {
            debug!(
                "Saving data before writing more ({} dirty blocks)",
                loaded.dirty_blocks.len()
            );

            loaded
                .store_cheap_blocks(storage.clone())
                .await
                .map_err(|error| Error::Storage(error))?;

            if (loaded.dirty_blocks.len() * 2) >= loaded.write_buffer_in_blocks {
                debug!(
                    "Still {} dirty blocks after the cheap stores. Will have to calculate some digests.",
                    loaded.dirty_blocks.len()
                );

                loaded
                    .store_all(storage.clone())
                    .await
                    .map_err(|error| Error::Storage(error))?;
                assert_eq!(0, loaded.dirty_blocks.len());
            }
        } else {
            debug!("Only {} dirty blocks?", loaded.dirty_blocks.len());
        }

        // Consider the digest outdated because any write is very likely to change the digest.
        loaded.digest.is_digest_up_to_date = false;

        let new_size = std::cmp::max(loaded.size, position + buf.len() as u64);
        assert!(new_size >= loaded.size);
        loaded.size = new_size;

        let first_block_index = position / (VALUE_BLOB_MAX_LENGTH as u64);
        if first_block_index >= (loaded.blocks.len() as u64) {
            if let Some(last_block) = loaded.blocks.last_mut() {
                let filler = VALUE_BLOB_MAX_LENGTH - last_block.size() as usize;
                let write_result = last_block
                    .write(
                        last_block.size(),
                        std::iter::repeat_n(0u8, filler).collect::<Vec<_>>().into(),
                        storage.clone(),
                    )
                    .await.unwrap(/*TODO: somehow recover and fix loaded.size*/);
                assert!(write_result.remaining.is_empty());
                loaded.dirty_blocks.push_back(loaded.blocks.len() - 1);
            }
            while first_block_index > (loaded.blocks.len() as u64) {
                // TODO: make this a static constant
                let filler = HashedValue::from(Arc::new(Tree::new(
                    TreeBlob::try_from(bytes::Bytes::from(vec![0u8; VALUE_BLOB_MAX_LENGTH]))
                        .unwrap(),
                    vec![],
                )));
                loaded.dirty_blocks.push_back(loaded.blocks.len());
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(
                        filler,
                    )));
            }
        }

        let mut next_block_index = first_block_index as usize;
        let position_in_block = (position % (VALUE_BLOB_MAX_LENGTH as u64)) as u16;
        if buf.prefix.is_empty() && (position_in_block == 0) {
            // special case where we do nothing
        } else {
            assert!((position_in_block != 0) || (buf.prefix.len() < VALUE_BLOB_MAX_LENGTH));
            if next_block_index == loaded.blocks.len() {
                let block_content: Vec<u8> = std::iter::repeat_n(0u8, position_in_block as usize)
                    .chain(buf.prefix)
                    .collect();
                assert!(block_content.len() <= VALUE_BLOB_MAX_LENGTH);
                debug!(
                    "Writing prefix creates an unknown digest block at {}",
                    next_block_index
                );
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(LoadedBlock::UnknownDigest(
                        block_content,
                    )));
            } else {
                let block = &mut loaded.blocks[next_block_index];
                assert!(buf.prefix.len() < VALUE_BLOB_MAX_LENGTH);
                assert!((position_in_block as usize) < VALUE_BLOB_MAX_LENGTH);
                assert!((position_in_block as usize + buf.prefix.len()) <= VALUE_BLOB_MAX_LENGTH);
                let write_result = block
                        .write(position_in_block, buf.prefix, storage.clone())
                        .await.unwrap(/*TODO: somehow recover and fix loaded.size*/);
                assert_eq!(0, write_result.remaining.len());
            }
            loaded.dirty_blocks.push_back(next_block_index);
            next_block_index += 1;
        }

        for full_block in buf.full_blocks {
            if next_block_index == loaded.blocks.len() {
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(
                        full_block,
                    )));
            } else {
                let existing_block = &mut loaded.blocks[next_block_index];
                *existing_block =
                    OpenFileContentBlock::Loaded(LoadedBlock::KnownDigest(full_block));
            }
            loaded.dirty_blocks.push_back(next_block_index);
            next_block_index += 1;
        }

        if !buf.suffix.is_empty() {
            if next_block_index == loaded.blocks.len() {
                debug!(
                    "Writing suffix creates an unknown digest block at {}",
                    next_block_index
                );
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(LoadedBlock::UnknownDigest(
                        buf.suffix.to_vec(),
                    )));
            } else {
                let block = &mut loaded.blocks[next_block_index];
                let write_result = block.write(0, buf.suffix, storage.clone()).await.unwrap(/*TODO: somehow recover and fix loaded.size*/);
                assert_eq!(0, write_result.remaining.len());
            }
            loaded.dirty_blocks.push_back(next_block_index);
        }
        Ok(())
    }

    pub async fn store_all(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<StoreChanges, StoreError> {
        match self {
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                debug!(
                    "Only {} dirty blocks?",
                    open_file_content_buffer_loaded.dirty_blocks.len()
                );
                open_file_content_buffer_loaded.store_all(storage).await
            }
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size: _,
                write_buffer_in_blocks: _,
            } => Ok(StoreChanges::NoChanges),
        }
    }

    async fn drop_all_read_caches(&mut self) -> CacheDropStats {
        match self {
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size: _,
                write_buffer_in_blocks: _,
            } => CacheDropStats::new(0, 0, 0),
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                open_file_content_buffer_loaded.drop_all_read_caches().await
            }
        }
    }
}

#[derive(Debug)]
pub struct OpenFileWritePermission {}

#[derive(Debug)]
pub struct OpenFile {
    content: tokio::sync::Mutex<OpenFileContentBuffer>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    change_event_sender: tokio::sync::watch::Sender<OpenFileStatus>,
    _change_event_receiver: tokio::sync::watch::Receiver<OpenFileStatus>,
    modified: std::time::SystemTime,
    write_permission: Arc<OpenFileWritePermission>,
}

impl OpenFile {
    pub fn new(
        content: OpenFileContentBuffer,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        modified: std::time::SystemTime,
    ) -> OpenFile {
        let (last_known_digest, last_known_digest_file_size) = content.last_known_digest();
        let (sender, receiver) = tokio::sync::watch::channel(OpenFileStatus::new(
            last_known_digest,
            content.size(),
            last_known_digest_file_size,
            false,
            0,
        ));
        OpenFile {
            content: tokio::sync::Mutex::new(content),
            storage: storage,
            change_event_sender: sender,
            _change_event_receiver: receiver,
            modified,
            write_permission: Arc::new(OpenFileWritePermission {}),
        }
    }

    pub fn modified(&self) -> std::time::SystemTime {
        self.modified
    }

    pub async fn size(&self) -> u64 {
        self.content.lock().await.size()
    }

    pub async fn get_meta_data(&self) -> DirectoryEntryMetaData {
        DirectoryEntryMetaData::new(DirectoryEntryKind::File(self.size().await), self.modified)
    }

    pub async fn request_save(&self) -> std::result::Result<OpenFileStatus, StoreError> {
        debug!("Requesting save on an open file. Will try to flush it.");
        self.flush().await
    }

    pub async fn last_known_digest(&self) -> (DigestStatus, u64) {
        let content_locked = self.content.lock().await;
        content_locked.last_known_digest()
    }

    fn is_open_for_writing(write_permission: &Arc<OpenFileWritePermission>) -> bool {
        Arc::strong_count(write_permission) > 1
    }

    async fn update_status(
        change_event_sender: &tokio::sync::watch::Sender<OpenFileStatus>,
        content: &OpenFileContentBuffer,
        write_permission: &Arc<OpenFileWritePermission>,
    ) -> std::result::Result<OpenFileStatus, StoreError> {
        let (last_known_digest, last_known_digest_file_size) = content.last_known_digest();
        let is_open_for_writing = Self::is_open_for_writing(write_permission);
        let status = OpenFileStatus::new(
            last_known_digest,
            content.size(),
            last_known_digest_file_size,
            is_open_for_writing,
            content.unsaved_blocks() * (VALUE_BLOB_MAX_LENGTH as u64),
        );
        if change_event_sender.send_if_modified(|last_status| {
            if *last_status == status {
                false
            } else {
                *last_status = status;
                true
            }
        }) {
            debug!("Sending changed file status: {:?}", &status);
        } else {
            debug!(
                "Not sending file status because it didn't change: {:?}",
                &status
            );
        }
        Ok(status)
    }

    pub fn get_write_permission(&self) -> Arc<OpenFileWritePermission> {
        self.write_permission.clone()
    }

    //#[instrument(skip_all)]
    pub fn notify_dropped_write_permission(&self) {
        self.change_event_sender.send_if_modified(|status| {
            let is_open_for_writing = Self::is_open_for_writing(&self.write_permission);
            if status.is_open_for_writing == is_open_for_writing {
                false
            } else {
                status.is_open_for_writing = is_open_for_writing;
                true
            }
        });
    }

    fn assert_write_permission(&self, write_permission: &OpenFileWritePermission) {
        assert!(std::ptr::eq(
            self.write_permission.as_ref(),
            write_permission
        ));
    }

    pub fn write_bytes(
        &self,
        write_permission: &OpenFileWritePermission,
        position: u64,
        buf: bytes::Bytes,
    ) -> Future<()> {
        self.assert_write_permission(write_permission);
        debug!("Write at {}: {} bytes", position, buf.len());
        Box::pin(async move {
            let write_buffer = OptimizedWriteBuffer::from_bytes(position, buf).await;
            let mut content_locked = self.content.lock().await;
            let write_result = content_locked
                .write(position, write_buffer, self.storage.clone())
                .await;
            debug!("Writing to file sends a change event for this file.");
            let update_result = Self::update_status(
                &self.change_event_sender,
                &mut content_locked,
                &self.write_permission,
            )
            .await;
            // We want to update the status even if parts of the write failed.
            write_result?;
            update_result
                .map_err(|error| Error::Storage(error))
                .map(|status| {
                    debug!("Status after writing: {:?}", &status);
                    ()
                })
        })
    }

    pub fn read_bytes(&self, position: u64, count: usize) -> Future<bytes::Bytes> {
        debug!("Read at {}: Up to {} bytes", position, count);
        Box::pin(async move {
            let mut content_locked = self.content.lock().await;
            let read_result = content_locked
                .read(position, count, self.storage.clone())
                .await
                .inspect(|bytes_read| debug!("Read {} bytes", bytes_read.len()))?;
            assert!(read_result.len() <= count);
            return Ok(read_result);
        })
    }

    //#[instrument(skip(self))]
    pub async fn flush(&self) -> std::result::Result<OpenFileStatus, StoreError> {
        debug!("Flushing open file");
        let mut content_locked = self.content.lock().await;
        match content_locked.store_all(self.storage.clone()).await? {
            StoreChanges::SomeChanges => {
                Self::update_status(
                    &self.change_event_sender,
                    &mut content_locked,
                    &self.write_permission,
                )
                .await
            }
            StoreChanges::NoChanges => Ok(*self.change_event_sender.borrow()),
        }
    }

    pub async fn watch(&self) -> tokio::sync::watch::Receiver<OpenFileStatus> {
        self.change_event_sender.subscribe()
    }

    pub async fn truncate(
        &self,
        write_permission: &OpenFileWritePermission,
    ) -> std::result::Result<(), Error> {
        self.assert_write_permission(write_permission);
        debug!("Truncating a file sends a change event for this file.");
        let mut content_locked = self.content.lock().await;
        let write_buffer_in_blocks = match &*content_locked {
            OpenFileContentBuffer::NotLoaded {
                digest: _,
                size: _,
                write_buffer_in_blocks,
            } => *write_buffer_in_blocks,
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                open_file_content_buffer_loaded.write_buffer_in_blocks
            }
        };
        let (last_known_digest, last_known_digest_file_size) = content_locked.last_known_digest();
        *content_locked = OpenFileContentBuffer::from_data(
            Vec::new(),
            last_known_digest.last_known_digest,
            last_known_digest_file_size,
            write_buffer_in_blocks,
        )
        .unwrap();
        let _update_result = Self::update_status(
            &self.change_event_sender,
            &mut content_locked,
            &self.write_permission,
        )
        .await
        .map_err(|error| Error::Storage(error))?;
        Ok(())
    }

    async fn drop_all_read_caches(&self) -> CacheDropStats {
        let mut content_locked = self.content.lock().await;
        content_locked.drop_all_read_caches().await
    }
}

pub struct TreeEditor {
    root: Arc<OpenDirectory>,
    empty_directory_digest: Mutex<Option<BlobDigest>>,
    empty_file_digest: Mutex<Option<BlobDigest>>,
}

impl TreeEditor {
    pub fn new(root: Arc<OpenDirectory>, empty_directory_digest: Option<BlobDigest>) -> TreeEditor {
        Self {
            root,
            empty_directory_digest: Mutex::new(empty_directory_digest),
            empty_file_digest: Mutex::new(None),
        }
    }

    pub async fn read_directory(
        &self,
        path: NormalizedPath,
    ) -> Result<Stream<MutableDirectoryEntry>> {
        let directory = match self.root.open_directory(path).await {
            Ok(opened) => opened,
            Err(error) => return Err(error),
        };
        Ok(directory.read().await)
    }

    pub fn get_meta_data<'a>(&self, path: NormalizedPath) -> Future<'a, DirectoryEntryMetaData> {
        match path.split_right() {
            PathSplitRightResult::Root => Box::pin(std::future::ready(Ok(
                DirectoryEntryMetaData::new(DirectoryEntryKind::Directory, self.root.modified()),
            ))),
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    match root.open_directory(directory_path).await {
                        Ok(directory) => directory.get_meta_data(&leaf_name).await,
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }

    pub fn open_file<'a>(&'a self, path: NormalizedPath) -> Future<'a, Arc<OpenFile>> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    let directory = match root.open_directory(directory_path).await {
                        Ok(opened) => opened,
                        Err(error) => return Err(error),
                    };
                    let empty_file_digest = self.require_empty_file_digest().await?;
                    directory.open_file(&file_name, &empty_file_digest).await
                })
            }
        }
    }

    async fn require_empty_directory_digest(&self) -> Result<BlobDigest> {
        let mut empty_directory_digest_locked = self.empty_directory_digest.lock().await;
        match *empty_directory_digest_locked {
            Some(exists) => Ok(exists),
            None => {
                let directory = OpenDirectory::create_directory(
                    self.root.get_storage(),
                    self.root.get_clock(),
                    1,
                )
                .await?;
                let status = directory.latest_status();
                assert!(status.digest.is_digest_up_to_date);
                let result = status.digest.last_known_digest;
                *empty_directory_digest_locked = Some(result);
                Ok(result)
            }
        }
    }

    pub async fn store_empty_file(
        storage: Arc<dyn LoadStoreValue + Send + Sync>,
    ) -> Result<BlobDigest> {
        debug!("Storing empty file");
        match storage
            .store_value(&HashedValue::from(Arc::new(Tree::new(
                TreeBlob::empty(),
                Vec::new(),
            ))))
            .await
        {
            Ok(success) => Ok(success),
            Err(error) => Err(Error::Storage(error)),
        }
    }

    async fn require_empty_file_digest(&self) -> Result<BlobDigest> {
        let mut empty_file_digest_locked: MutexGuard<'_, Option<BlobDigest>> =
            self.empty_file_digest.lock().await;
        match *empty_file_digest_locked {
            Some(exists) => Ok(exists),
            None => {
                let result = Self::store_empty_file(self.root.get_storage()).await?;
                *empty_file_digest_locked = Some(result);
                Ok(result)
            }
        }
    }

    pub fn create_directory<'a>(&'a self, path: NormalizedPath) -> Future<'a, ()> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    match root.open_directory(directory_path).await {
                        Ok(directory) => {
                            directory
                                .create_subdirectory(
                                    file_name,
                                    self.require_empty_directory_digest().await.unwrap(/*TODO*/),
                                )
                                .await
                        }
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }

    pub fn copy<'a>(&'a self, from: NormalizedPath, to: NormalizedPath) -> Future<'a, ()> {
        let opening_directory_from = match from.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        let opening_directory_to = match to.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        Box::pin(async move {
            let (maybe_directory_from, maybe_directory_to) =
                futures::join!(opening_directory_from.0, opening_directory_to.0);
            let directory_from = maybe_directory_from?;
            let directory_to = maybe_directory_to?;
            directory_from
                .copy(
                    &opening_directory_from.1,
                    &directory_to,
                    &opening_directory_to.1,
                )
                .await
        })
    }

    pub fn rename<'a>(&'a self, from: NormalizedPath, to: NormalizedPath) -> Future<'a, ()> {
        let opening_directory_from = match from.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        let opening_directory_to = match to.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        Box::pin(async move {
            let (maybe_directory_from, maybe_directory_to) =
                futures::join!(opening_directory_from.0, opening_directory_to.0);
            let directory_from = maybe_directory_from?;
            let directory_to = maybe_directory_to?;
            directory_from
                .rename(
                    &opening_directory_from.1,
                    &directory_to,
                    &opening_directory_to.1,
                )
                .await
        })
    }

    pub fn remove<'a>(&'a self, path: NormalizedPath) -> Future<'a, ()> {
        let opening_directory = match path.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        return Box::pin(async move {
            let directory = opening_directory.0.await?;
            directory.remove(&opening_directory.1).await
        });
    }
}
