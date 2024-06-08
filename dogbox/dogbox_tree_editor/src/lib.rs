use async_stream::stream;
use bytes::Buf;
use relative_path::RelativePath;
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    NotFound,
    CannotOpenRegularFileAsDirectory,
    CannotOpenDirectoryAsRegularFile,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Future<'a, T> = Pin<Box<dyn core::future::Future<Output = Result<T>> + Send + 'a>>;
pub type Stream<T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send>>;

#[derive(Clone, Debug, PartialEq)]
pub enum DirectoryEntryKind {
    Directory,
    File(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
}

#[derive(Clone, Debug)]
pub enum NamedEntry {
    NotOpen(DirectoryEntryKind),
    OpenRegularFile(Arc<OpenFile>),
    OpenSubdirectory(Arc<OpenDirectory>),
}

impl NamedEntry {
    async fn get_meta_data(&self) -> DirectoryEntryKind {
        match self {
            NamedEntry::NotOpen(kind) => kind.clone(),
            NamedEntry::OpenRegularFile(open_file) => open_file.get_meta_data().await,
            NamedEntry::OpenSubdirectory(_) => DirectoryEntryKind::Directory,
        }
    }
}

#[derive(Debug)]
struct OpenDirectory {
    // TODO: support really big directories. We may not be able to hold all entries in memory at the same time.
    // TODO: remove this unused field
    cached_entries: Vec<DirectoryEntry>,
    names: BTreeMap<String, NamedEntry>,
}

impl OpenDirectory {
    fn read(&self) -> Stream<DirectoryEntry> {
        let snapshot = self.names.clone();
        Box::pin(stream! {
            for cached_entry in snapshot {
                let kind = cached_entry.1.get_meta_data().await;
                yield DirectoryEntry{name: cached_entry.0, kind: kind};
            }
        })
    }

    fn get_meta_data<'a>(&self, name: &str) -> Future<'a, DirectoryEntryKind> {
        match self.names.get(name) {
            Some(found) => {
                let found_clone = (*found).clone();
                Box::pin(async move { Ok(found_clone.get_meta_data().await) })
            }
            None => Box::pin(std::future::ready(Err(Error::NotFound))),
        }
    }

    fn open_file<'a>(&mut self, name: &str) -> Future<'a, Arc<OpenFile>> {
        match self.names.get_mut(name) {
            Some(found) => match found {
                NamedEntry::NotOpen(kind) => match kind {
                    DirectoryEntryKind::Directory => todo!(),
                    DirectoryEntryKind::File(length) => {
                        // TODO: read file contents. For now we assume that the example file is empty at the start.
                        assert_eq!(0, *length);
                        let open_file = Arc::new(OpenFile::new(vec![]));
                        *found = NamedEntry::OpenRegularFile(open_file.clone());
                        Box::pin(std::future::ready(Ok(open_file)))
                    }
                },
                NamedEntry::OpenRegularFile(open_file) => {
                    Box::pin(std::future::ready(Ok(open_file.clone())))
                }
                NamedEntry::OpenSubdirectory(_) => Box::pin(std::future::ready(Err(
                    Error::CannotOpenDirectoryAsRegularFile,
                ))),
            },
            None => {
                let open_file = Arc::new(OpenFile::new(vec![]));
                self.names.insert(
                    name.to_string(),
                    NamedEntry::OpenRegularFile(open_file.clone()),
                );
                self.cached_entries.push(DirectoryEntry {
                    name: name.to_string(),
                    kind: DirectoryEntryKind::File(0),
                });
                Box::pin(std::future::ready(Ok(open_file)))
            }
        }
    }

    fn open_directory<'a>(&mut self, name: &str) -> Future<'a, Arc<OpenDirectory>> {
        todo!()
    }
}

#[tokio::test]
async fn test_open_directory_get_meta_data() {
    let expected = DirectoryEntryKind::File(12);
    let directory = OpenDirectory {
        cached_entries: Vec::new(),
        names: BTreeMap::from([(
            "test.txt".to_string(),
            NamedEntry::NotOpen(expected.clone()),
        )]),
    };
    let meta_data = directory.get_meta_data("test.txt").await.unwrap();
    assert_eq!(expected, meta_data);
}

#[tokio::test]
async fn test_open_directory_open_file() {
    let mut directory = OpenDirectory {
        cached_entries: Vec::new(),
        names: BTreeMap::new(),
    };
    let file_name = "test.txt";
    let opened = directory.open_file(file_name).await.unwrap();
    opened.flush();
    assert_eq!(
        DirectoryEntryKind::File(0),
        directory.get_meta_data(file_name).await.unwrap()
    );
    use futures::StreamExt;
    let directory_entries: Vec<DirectoryEntry> = directory.read().collect().await;
    assert_eq!(
        &[DirectoryEntry {
            name: file_name.to_string(),
            kind: DirectoryEntryKind::File(0)
        }][..],
        &directory_entries[..]
    );
}

#[tokio::test]
async fn test_read_directory_after_file_write() {
    let mut directory = OpenDirectory {
        cached_entries: Vec::new(),
        names: BTreeMap::new(),
    };
    let file_name = "test.txt";
    let opened = directory.open_file(file_name).await.unwrap();
    let file_content = &b"hello world"[..];
    opened.write_bytes(0, file_content.into()).await.unwrap();
    use futures::StreamExt;
    let directory_entries: Vec<DirectoryEntry> = directory.read().collect().await;
    assert_eq!(
        &[DirectoryEntry {
            name: file_name.to_string(),
            kind: DirectoryEntryKind::File(file_content.len() as u64)
        }][..],
        &directory_entries[..]
    );
}

#[tokio::test]
async fn test_get_meta_data_after_file_write() {
    let mut directory = OpenDirectory {
        cached_entries: Vec::new(),
        names: BTreeMap::new(),
    };
    let file_name = "test.txt";
    let opened = directory.open_file(file_name).await.unwrap();
    let file_content = &b"hello world"[..];
    opened.write_bytes(0, file_content.into()).await.unwrap();
    assert_eq!(
        DirectoryEntryKind::File(file_content.len() as u64),
        directory.get_meta_data(file_name).await.unwrap()
    );
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

#[derive(Debug)]
pub struct OpenFile {
    content: tokio::sync::Mutex<Vec<u8>>,
}

impl OpenFile {
    pub fn new(content: Vec<u8>) -> OpenFile {
        OpenFile {
            content: tokio::sync::Mutex::new(content),
        }
    }

    pub async fn get_meta_data(&self) -> DirectoryEntryKind {
        DirectoryEntryKind::File(self.content.lock().await.len() as u64)
    }

    pub fn write_bytes(&self, position: u64, buf: bytes::Bytes) -> Future<()> {
        Box::pin(async move {
            let position_usize = position.try_into().unwrap();
            let mut content_locked = self.content.lock().await;
            let previous_content_length = content_locked.len();
            match content_locked.split_at_mut_checked(position_usize) {
                Some((_, overwriting)) => {
                    let can_overwrite = usize::min(overwriting.len(), buf.len());
                    let (mut for_overwriting, for_extending) = buf.split_at(can_overwrite);
                    for_overwriting.copy_to_slice(overwriting.split_at_mut(can_overwrite).0);
                    content_locked.extend(for_extending);
                }
                None => {
                    content_locked.extend(
                        std::iter::repeat(0u8).take(position_usize - previous_content_length),
                    );
                    content_locked.extend(buf);
                }
            };
            Ok(())
        })
    }

    pub fn read_bytes(&self, position: u64, count: usize) -> Future<bytes::Bytes> {
        Box::pin(async move {
            let content_locked = self.content.lock().await;
            match content_locked.split_at_checked(position.try_into().unwrap()) {
                Some((_, from_position)) => Ok(bytes::Bytes::copy_from_slice(match from_position
                    .split_at_checked(count)
                {
                    Some((result, _)) => result,
                    None => from_position,
                })),
                None => todo!(),
            }
        })
    }

    pub fn flush(&self) {}
}

pub struct TreeEditor {
    root: Arc<tokio::sync::Mutex<OpenDirectory>>,
}

impl TreeEditor {
    pub fn new() -> TreeEditor {
        TreeEditor::from_entries(vec![])
    }

    pub fn from_entries(entries: Vec<DirectoryEntry>) -> TreeEditor {
        let names = BTreeMap::from_iter(
            entries
                .iter()
                .map(|entry| (entry.name.clone(), NamedEntry::NotOpen(entry.kind.clone()))),
        );
        TreeEditor {
            root: Arc::new(tokio::sync::Mutex::new(OpenDirectory {
                cached_entries: entries,
                names: names,
            })),
        }
    }

    fn open_directory<'t>(
        relative_root: &'t mut OpenDirectory,
        path: NormalizedPath,
    ) -> Result<&'t mut OpenDirectory> {
        match path.split_left() {
            PathSplitLeftResult::Root => Ok(relative_root),
            PathSplitLeftResult::Leaf(name) => match relative_root.names.get(&name) {
                Some(found) => match found {
                    NamedEntry::NotOpen(kind) => match kind {
                        DirectoryEntryKind::Directory => todo!(),
                        DirectoryEntryKind::File(_) => Err(Error::CannotOpenRegularFileAsDirectory),
                    },
                    NamedEntry::OpenRegularFile(_) => Err(Error::CannotOpenRegularFileAsDirectory),
                    NamedEntry::OpenSubdirectory(_) => todo!(),
                },
                None => Err(Error::NotFound),
            },
            PathSplitLeftResult::Directory(directory_name, tail) => {
                todo!("Opening {}, {:?}", &directory_name, &tail)
            }
        }
    }

    pub fn read_directory<'a>(&self, path: NormalizedPath) -> Future<'a, Stream<DirectoryEntry>> {
        let root = self.root.clone();
        Box::pin(async move {
            let mut root_lock = root.lock().await;
            let directory = match TreeEditor::open_directory(&mut root_lock, path) {
                Ok(opened) => opened,
                Err(error) => return Err(error),
            };
            Ok(directory.read())
        })
    }

    pub fn get_meta_data<'a>(&self, path: NormalizedPath) -> Future<'a, DirectoryEntryKind> {
        match path.split_right() {
            PathSplitRightResult::Root => {
                Box::pin(std::future::ready(Ok(DirectoryEntryKind::Directory)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    let mut root_lock = root.lock().await;
                    match TreeEditor::open_directory(&mut root_lock, directory_path) {
                        Ok(directory) => directory.get_meta_data(&leaf_name).await,
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }

    pub fn open_file<'a>(&self, path: NormalizedPath) -> Future<'a, Arc<OpenFile>> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    let mut root_lock = root.lock().await;
                    let directory = match TreeEditor::open_directory(&mut root_lock, directory_path)
                    {
                        Ok(opened) => opened,
                        Err(error) => return Err(error),
                    };
                    directory.open_file(&file_name).await
                })
            }
        }
    }

    pub fn create_directory<'a>(&self, path: NormalizedPath) -> Future<'a, ()> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    let mut root_lock = root.lock().await;
                    match TreeEditor::open_directory(&mut root_lock, directory_path) {
                        Ok(directory) => match directory.names.get(&file_name) {
                            Some(_found) => todo!(),
                            None => {
                                directory.names.insert(
                                    file_name,
                                    NamedEntry::NotOpen(DirectoryEntryKind::Directory),
                                );
                                Ok(())
                            }
                        },
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }
}

#[tokio::test]
async fn test_read_empty_root() {
    use futures::StreamExt;
    let editor = TreeEditor::new();
    let mut directory = editor
        .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
        .await
        .unwrap();
    let end = directory.next().await;
    assert!(end.is_none());
}

#[tokio::test]
async fn test_get_meta_data_of_root() {
    let editor = TreeEditor::new();
    let meta_data = editor
        .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new("/")))
        .await
        .unwrap();
    assert_eq!(DirectoryEntryKind::Directory, meta_data);
}

#[tokio::test]
async fn test_get_meta_data_of_non_normalized_path() {
    let editor = TreeEditor::new();
    let error = editor
        .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
            "unknown.txt",
        )))
        .await
        .unwrap_err();
    assert_eq!(Error::NotFound, error);
}

#[tokio::test]
async fn test_get_meta_data_of_unknown_path() {
    let editor = TreeEditor::new();
    let error = editor
        .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
            "/unknown.txt",
        )))
        .await
        .unwrap_err();
    assert_eq!(Error::NotFound, error);
}

#[tokio::test]
async fn test_get_meta_data_of_unknown_path_in_unknown_directory() {
    let editor = TreeEditor::new();
    let error = editor
        .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
            "/unknown/file.txt",
        )))
        .await
        .unwrap_err();
    assert_eq!(Error::NotFound, error);
}

#[tokio::test]
async fn test_read_directory_on_closed_regular_file() {
    let editor = TreeEditor::from_entries(vec![DirectoryEntry {
        name: "test.txt".to_string(),
        kind: DirectoryEntryKind::File(0),
    }]);
    let result = editor
        .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
            "/test.txt",
        )))
        .await;
    assert_eq!(Some(Error::CannotOpenRegularFileAsDirectory), result.err());
}

#[tokio::test]
async fn test_read_directory_on_open_regular_file() {
    let editor = TreeEditor::from_entries(vec![DirectoryEntry {
        name: "test.txt".to_string(),
        kind: DirectoryEntryKind::File(0),
    }]);
    let _open_file = editor
        .open_file(NormalizedPath::new(RelativePath::new("/test.txt")))
        .await
        .unwrap();
    let result = editor
        .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
            "/test.txt",
        )))
        .await;
    assert_eq!(Some(Error::CannotOpenRegularFileAsDirectory), result.err());
}

#[tokio::test]
async fn test_create_directory() {
    use futures::StreamExt;
    let editor = TreeEditor::from_entries(vec![]);
    editor
        .create_directory(NormalizedPath::new(RelativePath::new("/test")))
        .await
        .unwrap();

    let mut reading = editor
        .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
        .await
        .unwrap();
    let entry: DirectoryEntry = reading.next().await.unwrap();
    assert_eq!(
        DirectoryEntry {
            name: "test".to_string(),
            kind: DirectoryEntryKind::Directory
        },
        entry
    );
    let end = reading.next().await;
    assert!(end.is_none());
}
