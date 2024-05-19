#![deny(warnings)]
use async_stream::stream;
use std::{pin::Pin, sync::Arc};

#[derive(Clone, Debug)]
pub enum Error {
    NotFound,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Future<'a, T> = Pin<Box<dyn core::future::Future<Output = Result<T>> + Send + 'a>>;
pub type Stream<T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send>>;

#[derive(Clone, Debug)]
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

pub struct TreeEditor {
    root: Arc<OpenDirectory>,
}

impl TreeEditor {
    pub fn new() -> TreeEditor {
        TreeEditor {
            root: Arc::new(OpenDirectory {
                cached_entries: vec![],
            }),
        }
    }

    fn open_directory<'t>(
        relative_root: &'t OpenDirectory,
        path: &relative_path::RelativePath,
    ) -> Option<&'t OpenDirectory> {
        let mut components = path.components();
        match components.next() {
            Some(_subdirectory) => {
                todo!();
            }
            None => Some(&relative_root),
        }
    }

    pub fn read_directory<'a>(
        &self,
        path: &relative_path::RelativePath,
    ) -> Future<'a, Stream<DirectoryEntry>> {
        let root = self.root.clone();
        let path_cloned = path.to_relative_path_buf();
        Box::pin(async move {
            let directory = match TreeEditor::open_directory(&root, &path_cloned) {
                Some(opened) => opened,
                None => return Err(Error::NotFound),
            };
            Ok(directory.read())
        })
    }
}

#[tokio::test]
async fn test_read_empty_root_directory() {
    use futures::StreamExt;
    let editor = TreeEditor::new();
    let mut directory = editor
        .read_directory(relative_path::RelativePath::new("/"))
        .await
        .unwrap();
    let end = directory.next().await;
    assert!(end.is_none());
}
