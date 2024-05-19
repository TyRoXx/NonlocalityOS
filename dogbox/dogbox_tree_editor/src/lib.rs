#![deny(warnings)]
use async_stream::stream;
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    NotFound,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Future<'a, T> = Pin<Box<dyn core::future::Future<Output = Result<T>> + Send + 'a>>;
pub type Stream<T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send>>;

#[derive(Clone, Debug, PartialEq)]
pub enum DirectoryEntryKind {
    Directory,
    File(u64),
}

#[derive(Clone, Debug)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
}

struct OpenDirectory {
    // TODO: support really big directories. We may not be able to hold all entries in memory at the same time.
    cached_entries: Vec<DirectoryEntry>,
    names: BTreeMap<String, DirectoryEntryKind>,
}

impl OpenDirectory {
    fn read(&self) -> Stream<DirectoryEntry> {
        let snapshot = self.cached_entries.clone();
        Box::pin(stream! {
            for cached_entry in snapshot {
                yield cached_entry;
            }
        })
    }
}

pub enum PathSplitResult {
    Root,
    Leaf(String),
    Directory(String, NormalizedPath),
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

    pub fn split(mut self) -> PathSplitResult {
        let head = match self.components.pop_front() {
            Some(head) => head,
            None => return PathSplitResult::Root,
        };
        if self.components.is_empty() {
            PathSplitResult::Leaf(head)
        } else {
            PathSplitResult::Directory(
                head,
                NormalizedPath {
                    components: self.components,
                },
            )
        }
    }
}

#[test]
fn test_normalized_path_new() {
    assert_eq!(
        NormalizedPath::root(),
        NormalizedPath::new(&relative_path::RelativePath::new(""))
    );
}

pub struct TreeEditor {
    root: Arc<OpenDirectory>,
}

impl TreeEditor {
    pub fn new() -> TreeEditor {
        TreeEditor {
            root: Arc::new(OpenDirectory {
                cached_entries: vec![],
                names: BTreeMap::new(),
            }),
        }
    }

    fn open_directory<'t>(
        relative_root: &'t OpenDirectory,
        path: NormalizedPath,
    ) -> Option<&'t OpenDirectory> {
        match path.split() {
            PathSplitResult::Root => Some(&relative_root),
            PathSplitResult::Leaf(_) => todo!(),
            PathSplitResult::Directory(_, _) => todo!(),
        }
    }

    pub fn read_directory<'a>(&self, path: NormalizedPath) -> Future<'a, Stream<DirectoryEntry>> {
        let root = self.root.clone();
        Box::pin(async move {
            let directory = match TreeEditor::open_directory(&root, path) {
                Some(opened) => opened,
                None => return Err(Error::NotFound),
            };
            Ok(directory.read())
        })
    }

    pub fn get_meta_data<'a>(&self, path: NormalizedPath) -> Future<'a, DirectoryEntryKind> {
        match path.split() {
            PathSplitResult::Root => {
                Box::pin(std::future::ready(Ok(DirectoryEntryKind::Directory)))
            }
            PathSplitResult::Leaf(leaf) => {
                Box::pin(std::future::ready(match self.root.names.get(&leaf) {
                    Some(found) => Ok(found.clone()),
                    None => Err(Error::NotFound),
                }))
            }
            PathSplitResult::Directory(directory_name, _) => Box::pin(std::future::ready(
                match self.root.names.get(&directory_name) {
                    Some(found) => Ok(found.clone()),
                    None => Err(Error::NotFound),
                },
            )),
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
