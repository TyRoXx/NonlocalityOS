use astraea::{
    storage::{LoadStoreValue, StoreError},
    tree::{
        BlobDigest, Reference, ReferenceIndex, TypeId, TypedReference, Value, ValueBlob,
        VALUE_BLOB_MAX_LENGTH,
    },
};
use async_stream::stream;
use bytes::Buf;
use dogbox_tree::serialization::{self, DirectoryTree, FileName, SegmentedBlob};
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
    u64,
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{debug, error, info};

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
                let mut previous_status = *cloned_receiver.borrow();
                info!("The previous status was: {:?}", &previous_status);
                tokio::task::spawn(async move {
                    debug!("Hello from the spawned task!");
                    loop {
                        match cloned_receiver.changed().await {
                            Ok(_) => {
                                let current_status = *cloned_receiver.borrow();
                                if previous_status == current_status {
                                    info!(
                                        "Open file status received, but it is the same as before: {:?}",
                                        &previous_status
                                    );
                                } else {
                                    info!(
                                        "Open file status changed from {:?} to {:?}",
                                        &previous_status, &current_status
                                    );
                                    previous_status = current_status;
                                    on_change().await.unwrap();
                                }
                            }
                            Err(error) => {
                                info!("No longer watching a file: {}", &error);
                                break;
                            }
                        }
                    }
                });
            }
            NamedEntry::OpenSubdirectory(_arc, receiver) => {
                let mut cloned_receiver = receiver.clone();
                let mut previous_status = *cloned_receiver.borrow();
                info!("The previous status was: {:?}", &previous_status);
                tokio::task::spawn(async move {
                    debug!("Hello from the spawned task!");
                    loop {
                        match cloned_receiver.changed().await {
                            Ok(_) => {
                                let current_status = *cloned_receiver.borrow();
                                if previous_status == current_status {
                                    panic!(
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
                                info!("No longer watching a directory: {}", &error);
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
    fn new(names: BTreeMap<String, NamedEntry>) -> Self {
        Self {
            names,
            has_unsaved_changes: true,
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
}

impl OpenDirectory {
    pub fn new(
        digest: DigestStatus,
        names: BTreeMap<String, NamedEntry>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        modified: std::time::SystemTime,
        clock: WallClock,
    ) -> Self {
        let (change_event_sender, change_event_receiver) =
            tokio::sync::watch::channel(OpenDirectoryStatus::new(digest, 1, 0, 0, 0, 0, 0));
        Self {
            state: Mutex::new(OpenDirectoryMutableState::new(names)),
            storage,
            change_event_sender,
            _change_event_receiver: change_event_receiver,
            modified,
            clock,
        }
    }

    pub fn from_entries(
        digest: DigestStatus,
        entries: Vec<DirectoryEntry>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        modified: std::time::SystemTime,
        clock: WallClock,
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
        OpenDirectory::new(digest, names, storage.clone(), modified, clock)
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

    async fn read(&self) -> Stream<MutableDirectoryEntry> {
        let state_locked = self.state.lock().await;
        let snapshot = state_locked.names.clone();
        info!("Reading directory with {} entries", snapshot.len());
        Box::pin(stream! {
            for cached_entry in snapshot {
                let meta_data = cached_entry.1.get_meta_data().await;
                yield MutableDirectoryEntry{name: cached_entry.0, kind: meta_data.kind, modified: meta_data.modified,};
            }
        })
    }

    async fn get_meta_data(&self, name: &str) -> Result<DirectoryEntryMetaData> {
        let state_locked = self.state.lock().await;
        match state_locked.names.get(name) {
            Some(found) => {
                let found_clone = (*found).clone();
                Ok(found_clone.get_meta_data().await)
            }
            None => Err(Error::NotFound(name.to_string())),
        }
    }

    async fn open_file(
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
                        info!(
                            "Opening file of size {} and content {} for reading.",
                            length, digest
                        );
                        let open_file = Arc::new(OpenFile::new(
                            OpenFileContentBuffer::from_storage(digest.clone(), length),
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
                    OpenFileContentBuffer::from_storage(*empty_file_digest, 0),
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
                Self::notify_about_change(&self.change_event_sender, &mut state_locked).await;
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
                Self::notify_about_change(&self2.change_event_sender, &mut state_locked).await;
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
    ) -> Result<Arc<OpenDirectory>> {
        match storage.load_value(&Reference::new(*digest)) {
            Some(loaded) => {
                let parsed_directory: DirectoryTree =
                    match postcard::from_bytes(loaded.blob.as_slice()) {
                        Ok(success) => success,
                        Err(error) => return Err(Error::Postcard(error)),
                    };
                let mut entries = vec![];
                info!("Loading directory: {:?}", &parsed_directory.children);
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
                            if index >= loaded.references.len() {
                                return Err(Error::ReferenceIndexOutOfRange);
                            }
                            let digest = loaded.references[index].reference.digest;
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

    async fn open_directory(
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
    ) -> Result<OpenDirectory> {
        let value_blob = ValueBlob::try_from(
            postcard::to_allocvec(&DirectoryTree {
                children: BTreeMap::new(),
            })
            .unwrap(),
        )
        .unwrap();
        info!("Storing empty directory");
        let empty_directory_digest =
            match storage.store_value(Arc::new(Value::new(value_blob, vec![]))) {
                Ok(success) => success,
                Err(error) => return Err(Error::Storage(error)),
            }
            .digest;
        Ok(OpenDirectory::new(
            DigestStatus::new(empty_directory_digest, true),
            BTreeMap::new(),
            storage,
            (clock)(),
            clock,
        ))
    }

    async fn create_subdirectory(
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
                )
                .await?;
                let receiver = directory.watch().await;
                self.clone().insert_entry(
                    &mut state_locked,
                    name,
                    NamedEntry::OpenSubdirectory(directory, receiver),
                );
                Self::notify_about_change(&self.change_event_sender, &mut state_locked).await;
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
        Self::notify_about_change(&self.change_event_sender, &mut state_locked).await;
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

        info!(
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
            Self::notify_about_change(&self.change_event_sender, &mut state_there_locked.unwrap())
                .await;
        } else {
            Self::notify_about_change(&self.change_event_sender, &mut state_locked).await;
        }
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

        Self::notify_about_change(&self.change_event_sender, &mut state_locked).await;
        if let Some(ref mut state_there) = state_there_locked {
            Self::notify_about_change(&there.change_event_sender, state_there).await;
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
        change_event_sender: &tokio::sync::watch::Sender<OpenDirectoryStatus>,
        state_locked: &mut OpenDirectoryMutableState,
    ) -> OpenDirectoryStatus {
        if !state_locked.has_unsaved_changes {
            info!("Directory has unsaved changes now.");
            state_locked.has_unsaved_changes = true;
        }
        Self::update_status(change_event_sender, state_locked, None).await
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
            directories_unsaved_count += 1;
            state_locked.has_unsaved_changes = true;
        }
        change_event_sender.send_if_modified(|last_status| {
            let digest = match new_digest {
                Some(new_digest) => DigestStatus::new(new_digest, is_up_to_date),
                None => DigestStatus::new(
                    last_status.digest.last_known_digest,
                    last_status.digest.is_digest_up_to_date && is_up_to_date,
                ),
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
                info!("Sending directory status: {:?}", &status);
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
            serialization_references.push(TypedReference::new(
                TypeId(/*TODO get rid of this ID*/ 0),
                Reference::new(digest),
            ));
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
        let maybe_value_blob = ValueBlob::try_from(
            postcard::to_allocvec(&DirectoryTree {
                children: serialization_children,
            })
            .unwrap(),
        );
        match maybe_value_blob {
            Some(value_blob) => storage
                .store_value(Arc::new(Value::new(value_blob, serialization_references)))
                .map(|reference| reference.digest),
            None => todo!(),
        }
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

#[test]
fn test_normalized_path_new() {
    assert_eq!(
        NormalizedPath::root(),
        NormalizedPath::new(&relative_path::RelativePath::new(""))
    );
}

#[derive(PartialEq, Debug, Copy, Clone)]
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
    growth: u16,
    remaining: bytes::Bytes,
}

impl WriteResult {
    pub fn new(growth: u16, remaining: bytes::Bytes) -> Self {
        Self { growth, remaining }
    }
}

#[derive(Debug)]
pub enum OpenFileContentBlock {
    NotLoaded(BlobDigest, u16),
    Loaded(Option<BlobDigest>, Vec<u8>),
}

impl OpenFileContentBlock {
    pub async fn access_content_for_reading<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t Vec<u8>> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, size) => {
                let loaded = match storage.load_value(&Reference::new(*blob_digest)) {
                    Some(success) => success,
                    None => return Err(Error::MissingValue(*blob_digest)),
                };
                if loaded.blob.as_slice().len() != *size as usize {
                    error!(
                        "Loaded blob of size {}, but it was expected to be {} long",
                        loaded.blob.as_slice().len(),
                        *size
                    );
                    return Err(Error::FileSizeMismatch);
                }
                *self = OpenFileContentBlock::Loaded(
                    Some(*blob_digest),
                    /*TODO: avoid cloning*/ loaded.blob.as_slice().to_vec(),
                );
            }
            OpenFileContentBlock::Loaded(_blob_digest, _vec) => {}
        }
        Ok(match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(_blob_digest, vec) => vec,
        })
    }

    pub async fn access_content_for_writing<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut Vec<u8>> {
        self.access_content_for_reading(storage).await?;
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(blob_digest, vec) => {
                *blob_digest = None;
                Ok(vec)
            }
        }
    }

    pub async fn write(
        &mut self,
        position_in_block: u16,
        buf: bytes::Bytes,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<WriteResult> {
        let data = self.access_content_for_writing(storage).await?;
        let mut for_extending = match data.split_at_mut_checked(position_in_block as usize) {
            Some((_, overwriting)) => {
                let can_overwrite = usize::min(overwriting.len(), buf.len());
                let mut for_overwriting = buf;
                let for_extending = for_overwriting.split_off(can_overwrite);
                for_overwriting.copy_to_slice(overwriting.split_at_mut(can_overwrite).0);
                for_extending
            }
            None => {
                let previous_content_length = data.len();
                let zeroes = position_in_block as usize - (previous_content_length as usize);
                data.extend(std::iter::repeat(0u8).take(zeroes));
                buf
            }
        };
        let remaining_capacity: u16 = (VALUE_BLOB_MAX_LENGTH as u16 - (data.len() as u16)) as u16;
        let extension_size = usize::min(for_extending.len(), remaining_capacity as usize);
        let rest = for_extending.split_off(extension_size);
        data.extend(for_extending);
        Ok(WriteResult::new(extension_size as u16, rest))
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<BlobDigest, StoreError> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, _) => Ok(*blob_digest),
            OpenFileContentBlock::Loaded(blob_digest, vec) => {
                if let Some(stored) = blob_digest {
                    return Ok(*stored);
                }
                assert!(vec.len() <= VALUE_BLOB_MAX_LENGTH);
                let size = vec.len() as u16;
                debug!("Storing content block of size {}", size);
                let result = storage
                    .store_value(Arc::new(Value::new(
                        ValueBlob::try_from( vec.clone()).unwrap(/*TODO*/),
                        vec![],
                    )))
                    .map(|success| {
                        *blob_digest = Some(success.digest);
                        success.digest
                    })?;
                // free the memory
                *self = OpenFileContentBlock::NotLoaded(result, size);
                Ok(result)
            }
        }
    }

    pub fn size(&self) -> u16 {
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, size) => *size,
            OpenFileContentBlock::Loaded(_blob_digest, vec) => vec.len() as u16,
        }
    }
}

#[derive(Debug)]
pub struct OpenFileContentBufferLoaded {
    size: u64,
    blocks: Vec<OpenFileContentBlock>,
    digest: DigestStatus,
    last_known_digest_file_size: u64,
    number_of_bytes_written_since_last_save: u64,
}

impl OpenFileContentBufferLoaded {
    pub fn new(
        size: u64,
        blocks: Vec<OpenFileContentBlock>,
        digest: DigestStatus,
        last_known_digest_file_size: u64,
        number_of_bytes_written_since_last_save: u64,
    ) -> Self {
        Self {
            size,
            blocks,
            digest,
            last_known_digest_file_size,
            number_of_bytes_written_since_last_save,
        }
    }

    pub fn last_known_digest(&self) -> DigestStatus {
        self.digest
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<StoreChanges, StoreError> {
        let mut blocks_stored = Vec::new();
        // TODO(KWI): find unsaved blocks faster than O(N)
        for block in self.blocks.iter_mut() {
            let block_stored = block.store(storage.clone()).await?;
            blocks_stored.push(TypedReference::new(
                TypeId(u64::MAX),
                Reference::new(block_stored),
            ));
        }
        assert!(blocks_stored.len() >= 1);
        if blocks_stored.len() == 1 {
            return Ok(self.update_digest(blocks_stored[0].reference.digest));
        }
        let info = SegmentedBlob {
            size_in_bytes: self.size,
        };
        let value = Value::new(
            ValueBlob::try_from(postcard::to_allocvec(&info).unwrap()).unwrap(),
            blocks_stored,
        );
        let reference = storage.store_value(Arc::new(value))?;
        Ok(self.update_digest(reference.digest))
    }

    fn update_digest(&mut self, new_digest: BlobDigest) -> StoreChanges {
        let old_digest = self.digest;
        self.digest = DigestStatus::new(new_digest, true);
        self.last_known_digest_file_size = self.size;
        self.number_of_bytes_written_since_last_save = 0;
        if old_digest == self.digest {
            StoreChanges::NoChanges
        } else {
            StoreChanges::SomeChanges
        }
    }
}

pub enum StoreChanges {
    SomeChanges,
    NoChanges,
}

#[derive(Debug)]
pub struct OptimizedWriteBuffer {
    // less than VALUE_BLOB_MAX_LENGTH
    prefix: bytes::Bytes,
    // each one is exactly VALUE_BLOB_MAX_LENGTH
    full_blocks: Vec<bytes::Bytes>,
    // less than VALUE_BLOB_MAX_LENGTH
    suffix: bytes::Bytes,
}

impl OptimizedWriteBuffer {
    pub fn from_bytes(write_position: u64, content: bytes::Bytes) -> OptimizedWriteBuffer {
        let first_block_offset = (write_position % VALUE_BLOB_MAX_LENGTH as u64) as usize;
        let first_block_capacity = VALUE_BLOB_MAX_LENGTH - first_block_offset;
        let mut block_aligned_content = content.clone();
        let prefix = match first_block_offset {
            0 => bytes::Bytes::new(),
            _ => {
                let prefix = block_aligned_content.split_to(std::cmp::min(
                    block_aligned_content.len(),
                    first_block_capacity,
                ));
                assert!(prefix.len() <= first_block_capacity);
                assert!((first_block_offset + prefix.len()) <= VALUE_BLOB_MAX_LENGTH);
                prefix
            }
        };
        let mut full_blocks = Vec::new();
        loop {
            if block_aligned_content.len() < VALUE_BLOB_MAX_LENGTH {
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
            // TODO: hash the full blocks here before taking any locks on the file, and in parallel
            full_blocks.push(next);
        }
    }

    pub fn len(&self) -> usize {
        self.prefix.len() + (self.full_blocks.len() * VALUE_BLOB_MAX_LENGTH) + self.suffix.len()
    }
}

#[derive(Debug)]
pub enum OpenFileContentBuffer {
    NotLoaded { digest: BlobDigest, size: u64 },
    Loaded(OpenFileContentBufferLoaded),
}

impl OpenFileContentBuffer {
    pub fn from_storage(digest: BlobDigest, size: u64) -> Self {
        Self::NotLoaded {
            digest: digest,
            size: size,
        }
    }

    pub fn from_data(
        data: Vec<u8>,
        last_known_digest: BlobDigest,
        last_known_digest_file_size: u64,
    ) -> Option<Self> {
        if data.len() > VALUE_BLOB_MAX_LENGTH {
            None
        } else {
            let size = data.len() as u64;
            Some(Self::Loaded(OpenFileContentBufferLoaded {
                size: size,
                blocks: vec![OpenFileContentBlock::Loaded(None, data)],
                digest: DigestStatus::new(last_known_digest, false),
                last_known_digest_file_size,
                number_of_bytes_written_since_last_save: size,
            }))
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            OpenFileContentBuffer::NotLoaded { digest: _, size } => *size,
            OpenFileContentBuffer::Loaded(OpenFileContentBufferLoaded {
                size,
                blocks: _,
                digest: _,
                last_known_digest_file_size: _,
                number_of_bytes_written_since_last_save: _,
            }) => *size,
        }
    }

    pub fn unsaved_bytes(&self) -> u64 {
        match self {
            OpenFileContentBuffer::NotLoaded { digest: _, size: _ } => 0,
            OpenFileContentBuffer::Loaded(OpenFileContentBufferLoaded {
                size: _,
                blocks: _,
                digest: _,
                last_known_digest_file_size: _,
                number_of_bytes_written_since_last_save,
            }) => *number_of_bytes_written_since_last_save,
        }
    }

    pub fn last_known_digest(&self) -> (DigestStatus, u64) {
        match self {
            OpenFileContentBuffer::NotLoaded { digest, size } => {
                (DigestStatus::new(*digest, true), *size)
            }
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
        let loaded = self.require_loaded(storage.clone()).await?;
        Self::read_from_blocks(&mut loaded.blocks, position, count, storage).await
    }

    async fn require_loaded<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut OpenFileContentBufferLoaded> {
        match self {
            OpenFileContentBuffer::NotLoaded { digest, size } => {
                let blocks = if *size <= VALUE_BLOB_MAX_LENGTH as u64 {
                    vec![OpenFileContentBlock::NotLoaded(*digest, *size as u16)]
                } else {
                    let value = match storage.load_value(&Reference::new(*digest)) {
                        Some(success) => success,
                        None => return Err(Error::MissingValue(*digest)),
                    };
                    let info: SegmentedBlob = match postcard::from_bytes(&value.blob.as_slice()) {
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
                    if value.references.len() < 1 {
                        todo!()
                    }
                    let full_blocks = value
                        .references
                        .iter()
                        .take(value.references.len() - 1)
                        .map(|reference| {
                            OpenFileContentBlock::NotLoaded(
                                reference.reference.digest,
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
                            value.references.last().unwrap().reference.digest,
                            final_block_size as u16,
                        )))
                        .collect()
                };
                *self = Self::Loaded(OpenFileContentBufferLoaded {
                    size: *size,
                    blocks: blocks,
                    digest: DigestStatus::new(*digest, true),
                    last_known_digest_file_size: *size,
                    number_of_bytes_written_since_last_save: 0,
                });
            }
            OpenFileContentBuffer::Loaded(_loaded) => {}
        }
        match self {
            OpenFileContentBuffer::NotLoaded { digest: _, size: _ } => panic!(),
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                Ok(open_file_content_buffer_loaded)
            }
        }
    }

    async fn read_from_blocks(
        blocks: &mut Vec<OpenFileContentBlock>,
        position: u64,
        count: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        let block_size = VALUE_BLOB_MAX_LENGTH;
        let first_block_index = position / (block_size as u64);
        if first_block_index >= (blocks.len() as u64) {
            todo!()
        }
        let next_block_index = first_block_index as usize;
        let block = &mut blocks[next_block_index];
        let data = block.access_content_for_reading(storage).await?;
        let position_in_block = (position % VALUE_BLOB_MAX_LENGTH as u64) as usize;
        match data.split_at_checked(position_in_block) {
            Some((_, from_position)) => {
                return Ok(bytes::Bytes::copy_from_slice(
                    match from_position.split_at_checked(count) {
                        Some((result, _)) => result,
                        None => from_position,
                    },
                ))
            }
            None => todo!(),
        }
    }

    pub async fn write(
        &mut self,
        position: u64,
        buf: OptimizedWriteBuffer,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<()> {
        let loaded = self.require_loaded(storage.clone()).await?;

        if loaded.number_of_bytes_written_since_last_save >= 5_000_000 {
            info!(
                "Saving data before writing more ({} unsaved bytes)",
                loaded.number_of_bytes_written_since_last_save
            );
            loaded
                .store(storage.clone())
                .await
                .map_err(|error| Error::Storage(error))?;
            assert_eq!(0, loaded.number_of_bytes_written_since_last_save);
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
                loaded.number_of_bytes_written_since_last_save += write_result.growth as u64;
            }
            while first_block_index >= (loaded.blocks.len() as u64) {
                let filler = vec![0u8; VALUE_BLOB_MAX_LENGTH];
                loaded.number_of_bytes_written_since_last_save += filler.len() as u64;
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(None, filler));
            }
        }

        loaded.number_of_bytes_written_since_last_save += buf.len() as u64;
        let mut next_block_index = first_block_index as usize;
        {
            let position_in_block = (position % (VALUE_BLOB_MAX_LENGTH as u64)) as u16;
            if buf.prefix.is_empty() {
                assert_eq!(0, position_in_block);
            } else {
                assert_ne!(0, position_in_block);
                if next_block_index == loaded.blocks.len() {
                    loaded
                        .blocks
                        .push(OpenFileContentBlock::Loaded(None, buf.prefix.to_vec()));
                } else {
                    let block = &mut loaded.blocks[next_block_index];
                    assert!(buf.prefix.len() < VALUE_BLOB_MAX_LENGTH);
                    assert!((position_in_block as usize) < VALUE_BLOB_MAX_LENGTH);
                    assert!(
                        (position_in_block as usize + buf.prefix.len()) <= VALUE_BLOB_MAX_LENGTH
                    );
                    let write_result = block
                        .write(position_in_block, buf.prefix, storage.clone())
                        .await.unwrap(/*TODO: somehow recover and fix loaded.size*/);
                    assert_eq!(0, write_result.remaining.len());
                }
                next_block_index += 1;
            }
        }

        for full_block in buf.full_blocks {
            if next_block_index == loaded.blocks.len() {
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(None, full_block.to_vec()));
            } else {
                let existing_block = &mut loaded.blocks[next_block_index];
                match existing_block {
                    OpenFileContentBlock::NotLoaded(_blob_digest, _) => {
                        *existing_block = OpenFileContentBlock::Loaded(None, full_block.to_vec());
                    }
                    OpenFileContentBlock::Loaded(blob_digest, vec) => {
                        *blob_digest = None;
                        // reuse the memory
                        vec.clear();
                        vec.extend_from_slice(&full_block);
                    }
                }
            }
            next_block_index += 1;
        }

        if !buf.suffix.is_empty() {
            if next_block_index == loaded.blocks.len() {
                loaded
                    .blocks
                    .push(OpenFileContentBlock::Loaded(None, buf.suffix.to_vec()));
            } else {
                let block = &mut loaded.blocks[next_block_index];
                let write_result = block.write(0, buf.suffix, storage.clone()).await.unwrap(/*TODO: somehow recover and fix loaded.size*/);
                assert_eq!(0, write_result.remaining.len());
            }
        }
        Ok(())
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<StoreChanges, StoreError> {
        match self {
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                open_file_content_buffer_loaded.store(storage).await
            }
            OpenFileContentBuffer::NotLoaded { digest: _, size: _ } => Ok(StoreChanges::NoChanges),
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
        info!("Requesting save on an open file. Will try to flush it.");
        self.flush().await
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
            content.unsaved_bytes(),
        );
        if change_event_sender.send_if_modified(|last_status| {
            if *last_status == status {
                false
            } else {
                *last_status = status;
                true
            }
        }) {
            info!("Sending file status: {:?}", &status);
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

    pub fn write_bytes(
        &self,
        write_permission: &OpenFileWritePermission,
        position: u64,
        buf: bytes::Bytes,
    ) -> Future<()> {
        assert!(std::ptr::eq(
            self.write_permission.as_ref(),
            write_permission
        ));
        debug!("Write at {}: {} bytes", position, buf.len());
        Box::pin(async move {
            let write_buffer = OptimizedWriteBuffer::from_bytes(position, buf);
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
            content_locked
                .read(position, count, self.storage.clone())
                .await
                .inspect(|bytes_read| debug!("Read {} bytes", bytes_read.len()))
        })
    }

    pub async fn flush(&self) -> std::result::Result<OpenFileStatus, StoreError> {
        let mut content_locked = self.content.lock().await;
        match content_locked.store(self.storage.clone()).await? {
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
                let directory =
                    OpenDirectory::create_directory(self.root.get_storage(), self.root.get_clock())
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
        info!("Storing empty file");
        match storage.store_value(Arc::new(Value::new(ValueBlob::empty(), Vec::new()))) {
            Ok(success) => Ok(success.digest),
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

#[cfg(test)]
mod tests {
    use super::*;
    use astraea::storage::{InMemoryValueStorage, LoadValue, StoreValue};
    use lazy_static::lazy_static;

    fn test_clock() -> std::time::SystemTime {
        std::time::SystemTime::UNIX_EPOCH
    }

    lazy_static! {
        static ref DUMMY_DIGEST: BlobDigest = BlobDigest::new(&[
            104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108, 6, 169, 103,
            74, 159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205, 186, 47, 210, 169, 3, 196,
            19, 48, 211, 86, 202, 96, 177, 113, 146, 195, 171, 48, 102, 23, 244, 236, 205, 2, 38,
            202, 233, 41, 2, 52, 27,
        ]);
    }

    #[tokio::test]
    async fn test_open_directory_get_meta_data() {
        let modified = test_clock();
        let expected = DirectoryEntryMetaData::new(DirectoryEntryKind::File(12), modified);
        let directory = OpenDirectory::new(
            DigestStatus::new(*DUMMY_DIGEST, false),
            BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )]),
            Arc::new(NeverUsedStorage {}),
            modified,
            test_clock,
        );
        let meta_data = directory.get_meta_data("test.txt").await.unwrap();
        assert_eq!(expected, meta_data);
    }

    #[tokio::test]
    async fn test_open_directory_nothing_happens() {
        let modified = test_clock();
        let expected = DirectoryEntryMetaData::new(DirectoryEntryKind::File(12), modified);
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = OpenDirectory::new(
            DigestStatus::new(*DUMMY_DIGEST, false),
            BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )]),
            storage.clone(),
            modified,
            test_clock,
        );
        let mut receiver = directory.watch().await;
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), receiver.changed()).await;
        assert_eq!("deadline has elapsed", format!("{}", result.unwrap_err()));
        let status = *receiver.borrow();
        assert_eq!(
            OpenDirectoryStatus::new(
                DigestStatus::new(
                    BlobDigest::new(&[
                        104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108,
                        6, 169, 103, 74, 159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205,
                        186, 47, 210, 169, 3, 196, 19, 48, 211, 86, 202, 96, 177, 113, 146, 195,
                        171, 48, 102, 23, 244, 236, 205, 2, 38, 202, 233, 41, 2, 52, 27
                    ]),
                    false
                ),
                1,
                0,
                0,
                0,
                0,
                0
            ),
            status
        );
        assert_eq!(0, storage.len());
    }

    #[tokio::test]
    async fn test_open_directory_open_file() {
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = Arc::new(OpenDirectory::new(
            DigestStatus::new(*DUMMY_DIGEST, false),
            BTreeMap::new(),
            storage.clone(),
            modified,
            test_clock,
        ));
        let file_name = "test.txt";
        let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
        let opened = directory
            .clone()
            .open_file(file_name, &empty_file_digest)
            .await
            .unwrap();
        opened.flush().await.unwrap();
        assert_eq!(
            DirectoryEntryMetaData::new(DirectoryEntryKind::File(0), modified),
            directory.get_meta_data(file_name).await.unwrap()
        );
        use futures::StreamExt;
        let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
        assert_eq!(
            &[MutableDirectoryEntry {
                name: file_name.to_string(),
                kind: DirectoryEntryKind::File(0),
                modified: modified,
            }][..],
            &directory_entries[..]
        );
    }

    #[tokio::test]
    async fn test_read_directory_after_file_write() {
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = Arc::new(OpenDirectory::new(
            DigestStatus::new(*DUMMY_DIGEST, false),
            BTreeMap::new(),
            storage.clone(),
            modified,
            test_clock,
        ));
        let file_name = "test.txt";
        let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
        let opened = directory
            .clone()
            .open_file(file_name, &empty_file_digest)
            .await
            .unwrap();
        let write_permission = opened.get_write_permission();
        let file_content = &b"hello world"[..];
        opened
            .write_bytes(&write_permission, 0, file_content.into())
            .await
            .unwrap();
        use futures::StreamExt;
        let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
        assert_eq!(
            &[MutableDirectoryEntry {
                name: file_name.to_string(),
                kind: DirectoryEntryKind::File(file_content.len() as u64),
                modified,
            }][..],
            &directory_entries[..]
        );
    }

    #[tokio::test]
    async fn test_get_meta_data_after_file_write() {
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = Arc::new(OpenDirectory::new(
            DigestStatus::new(*DUMMY_DIGEST, false),
            BTreeMap::new(),
            storage.clone(),
            modified,
            test_clock,
        ));
        let file_name = "test.txt";
        let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
        let opened = directory
            .clone()
            .open_file(file_name, &empty_file_digest)
            .await
            .unwrap();
        let write_permission = opened.get_write_permission();
        let file_content = &b"hello world"[..];
        opened
            .write_bytes(&write_permission, 0, file_content.into())
            .await
            .unwrap();
        assert_eq!(
            DirectoryEntryMetaData::new(
                DirectoryEntryKind::File(file_content.len() as u64),
                modified
            ),
            directory.get_meta_data(file_name).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_read_empty_root() {
        use futures::StreamExt;
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let mut directory = editor
            .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
            .await
            .unwrap();
        let end = directory.next().await;
        assert!(end.is_none());
    }

    struct NeverUsedStorage {}

    impl LoadValue for NeverUsedStorage {
        fn load_value(
            &self,
            _reference: &astraea::tree::Reference,
        ) -> Option<Arc<astraea::tree::Value>> {
            panic!()
        }
    }

    impl StoreValue for NeverUsedStorage {
        fn store_value(
            &self,
            _value: Arc<astraea::tree::Value>,
        ) -> std::result::Result<astraea::tree::Reference, StoreError> {
            panic!()
        }
    }

    impl LoadStoreValue for NeverUsedStorage {}

    #[tokio::test]
    async fn test_get_meta_data_of_root() {
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let meta_data = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new("/")))
            .await
            .unwrap();
        assert_eq!(
            DirectoryEntryMetaData::new(DirectoryEntryKind::Directory, modified),
            meta_data
        );
    }

    #[tokio::test]
    async fn test_get_meta_data_of_non_normalized_path() {
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "unknown.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound("unknown.txt".to_string()), error);
    }

    #[tokio::test]
    async fn test_get_meta_data_of_unknown_path() {
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "/unknown.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound("unknown.txt".to_string()), error);
    }

    #[tokio::test]
    async fn test_get_meta_data_of_unknown_path_in_unknown_directory() {
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "/unknown/file.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound("unknown".to_string()), error);
    }

    #[tokio::test]
    async fn test_read_directory_on_closed_regular_file() {
        let modified = test_clock();
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![DirectoryEntry {
                    name: "test.txt".to_string(),
                    kind: DirectoryEntryKind::File(4),
                    digest: BlobDigest::hash(b"TEST"),
                }],
                Arc::new(NeverUsedStorage {}),
                modified,
                test_clock,
            )),
            None,
        );
        let result = editor
            .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                "/test.txt",
            )))
            .await;
        assert_eq!(
            Some(Error::CannotOpenRegularFileAsDirectory(
                "test.txt".to_string()
            )),
            result.err()
        );
    }

    #[tokio::test]
    async fn test_read_directory_on_open_regular_file() {
        use relative_path::RelativePath;
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![DirectoryEntry {
                    name: "test.txt".to_string(),
                    kind: DirectoryEntryKind::File(0),
                    digest: BlobDigest::hash(b""),
                }],
                storage,
                modified,
                test_clock,
            )),
            None,
        );
        let _open_file = editor
            .open_file(NormalizedPath::new(RelativePath::new("/test.txt")))
            .await
            .unwrap();
        let result = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/test.txt")))
            .await;
        assert_eq!(
            Some(Error::CannotOpenRegularFileAsDirectory(
                "test.txt".to_string()
            )),
            result.err()
        );
    }

    #[tokio::test]
    async fn test_create_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                storage,
                modified,
                test_clock,
            )),
            None,
        );
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let mut reading = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/")))
            .await
            .unwrap();
        let entry: MutableDirectoryEntry = reading.next().await.unwrap();
        assert_eq!(
            MutableDirectoryEntry {
                name: "test".to_string(),
                kind: DirectoryEntryKind::Directory,
                modified,
            },
            entry
        );
        let end = reading.next().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_read_created_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                storage,
                modified,
                test_clock,
            )),
            None,
        );
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let mut reading = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let end = reading.next().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_nested_create_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let modified = test_clock();
        let storage = Arc::new(InMemoryValueStorage::empty());
        let editor = TreeEditor::new(
            Arc::new(OpenDirectory::from_entries(
                DigestStatus::new(*DUMMY_DIGEST, false),
                vec![],
                storage,
                modified,
                test_clock,
            )),
            None,
        );
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test/subdir")))
            .await
            .unwrap();
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                    "/test/subdir",
                )))
                .await
                .unwrap();
            let end = reading.next().await;
            assert!(end.is_none());
        }
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                    "/test",
                )))
                .await
                .unwrap();
            let entry: MutableDirectoryEntry = reading.next().await.unwrap();
            assert_eq!(
                MutableDirectoryEntry {
                    name: "subdir".to_string(),
                    kind: DirectoryEntryKind::Directory,
                    modified,
                },
                entry
            );
            let end = reading.next().await;
            assert!(end.is_none());
        }
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
                .await
                .unwrap();
            let entry: MutableDirectoryEntry = reading.next().await.unwrap();
            assert_eq!(
                MutableDirectoryEntry {
                    name: "test".to_string(),
                    kind: DirectoryEntryKind::Directory,
                    modified,
                },
                entry
            );
            let end = reading.next().await;
            assert!(end.is_none());
        }
    }
}
