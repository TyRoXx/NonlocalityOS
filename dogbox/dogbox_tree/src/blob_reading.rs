use crate::reading::{EntryAccessor, ReadDirectory, ReadFile, Stream};
use crate::serialization::{DirectoryTree, FileName};
use async_stream::stream;
use async_trait::async_trait;
use dogbox_blob_layer::{BlobDigest, MemoryBlobStore};
use futures_util::StreamExt;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

struct BlobReadFile {
    digest: BlobDigest,
    read_blob: Arc<dyn dogbox_blob_layer::ReadBlob>,
}

struct BlobOpenFile {
    content: Vec<u8>,
    cursor: usize,
}

impl crate::reading::AsyncReadBlob for BlobOpenFile {}

impl tokio::io::AsyncRead for BlobOpenFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let copying = usize::min(buf.remaining(), self.content.len() - self.cursor);
        let start = self.content.split_at(self.cursor).1;
        let limited = start.split_at(copying).0;
        buf.put_slice(limited);
        self.cursor += copying;
        std::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncSeek for BlobOpenFile {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        todo!()
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}

#[async_trait]
impl ReadFile for BlobReadFile {
    async fn open(&self) -> crate::reading::Result<Box<dyn crate::reading::AsyncReadBlob>> {
        match self.read_blob.read_blob(&self.digest).await {
            Some(success) => Ok(Box::new(BlobOpenFile {
                content: success,
                cursor: 0,
            })),
            None => Err(crate::reading::Error::DataUnavailable),
        }
    }
}

struct BlobReadDirectory {
    digest: BlobDigest,
    tree: tokio::sync::Mutex<Option<Arc<DirectoryTree>>>,
    read_blob: Arc<dyn dogbox_blob_layer::ReadBlob>,
}

fn parse_directory_blob(data: &[u8]) -> Option<DirectoryTree> {
    match postcard::from_bytes(data) {
        Ok(success) => Some(success),
        Err(_) => None,
    }
}

impl BlobReadDirectory {
    async fn require_tree(&self) -> crate::reading::Result<Arc<DirectoryTree>> {
        let mut tree_locked = self.tree.lock().await;
        match tree_locked.as_ref() {
            Some(exists) => Ok(exists.clone()),
            None => match self.read_blob.read_blob(&self.digest).await {
                Some(blob_content) => match parse_directory_blob(&blob_content) {
                    Some(parsed) => {
                        let result = Arc::new(parsed);
                        *tree_locked = Some(result.clone());
                        Ok(result)
                    }
                    None => Err(crate::reading::Error::DataIncompatible),
                },
                None => Err(crate::reading::Error::DataUnavailable),
            },
        }
    }
}

fn convert_entry_info(
    from: &crate::serialization::DirectoryEntryKind,
) -> crate::reading::DirectoryEntryInfo {
    match from {
        crate::serialization::DirectoryEntryKind::Directory => {
            crate::reading::DirectoryEntryInfo::Directory
        }
        crate::serialization::DirectoryEntryKind::File(size) => {
            crate::reading::DirectoryEntryInfo::File(*size)
        }
    }
}

#[async_trait]
impl ReadDirectory for BlobReadDirectory {
    async fn enumerate<'t>(
        &'t self,
    ) -> crate::reading::Result<Stream<'t, crate::reading::DirectoryEntry>> {
        let tree = self.require_tree().await?;
        Ok(Box::pin(stream! {
            for child in &tree.children {
                yield crate::reading::DirectoryEntry::new(child.0.clone(), convert_entry_info(&child.1.kind));
            }
        }))
    }

    async fn access_entry(&self, name: &FileName) -> crate::reading::Result<Option<EntryAccessor>> {
        let tree = self.require_tree().await?;
        match tree.children.get(name) {
            Some(found) => match found.kind {
                crate::serialization::DirectoryEntryKind::Directory => Ok(Some(
                    EntryAccessor::Directory(Box::new(BlobReadDirectory {
                        digest: found.digest,
                        tree: Mutex::new(None),
                        read_blob: self.read_blob.clone(),
                    })),
                )),
                crate::serialization::DirectoryEntryKind::File(_) => {
                    Ok(Some(EntryAccessor::File(Box::new(BlobReadFile {
                        digest: found.digest,
                        read_blob: self.read_blob.clone(),
                    }))))
                }
            },
            None => Ok(None),
        }
    }
}

pub struct DoNotUse {}

#[async_trait]
impl dogbox_blob_layer::ReadBlob for DoNotUse {
    async fn read_blob(&self, _digest: &BlobDigest) -> Option<Vec<u8>> {
        panic!()
    }
}

#[tokio::test]
async fn test_blob_read_directory_enumerate_empty() {
    let directory = BlobReadDirectory {
        digest: BlobDigest::hash(&[]),
        tree: Mutex::new(Some(Arc::new(DirectoryTree::new(BTreeMap::new())))),
        read_blob: Arc::new(DoNotUse {}),
    };
    let mut entries = directory.enumerate().await.unwrap();
    let entry = entries.next().await;
    assert!(entry.is_none());
}

#[tokio::test]
async fn test_blob_read_directory_enumerate_non_empty() {
    let dir_name = FileName::try_from("dir".to_string()).unwrap();
    let file_name = FileName::try_from("file.txt".to_string()).unwrap();
    let file_size = 123;
    let directory = BlobReadDirectory {
        digest: BlobDigest::hash(&[]),
        tree: Mutex::new(Some(Arc::new(DirectoryTree::new(BTreeMap::from([
            (
                dir_name.clone(),
                crate::serialization::DirectoryEntry::new(
                    crate::serialization::DirectoryEntryKind::Directory,
                    BlobDigest::hash(&[]),
                ),
            ),
            (
                file_name.clone(),
                crate::serialization::DirectoryEntry::new(
                    crate::serialization::DirectoryEntryKind::File(file_size),
                    BlobDigest::hash(&[]),
                ),
            ),
        ]))))),
        read_blob: Arc::new(DoNotUse {}),
    };
    let mut entries = directory.enumerate().await.unwrap();
    assert_eq!(
        crate::reading::DirectoryEntry::new(
            dir_name,
            crate::reading::DirectoryEntryInfo::Directory
        ),
        entries.next().await.unwrap()
    );
    assert_eq!(
        crate::reading::DirectoryEntry::new(
            file_name,
            crate::reading::DirectoryEntryInfo::File(file_size)
        ),
        entries.next().await.unwrap()
    );
    assert!(entries.next().await.is_none());
}

#[tokio::test]
async fn test_blob_read_directory_access_entry_that_doesnt_exist() {
    let directory = BlobReadDirectory {
        digest: BlobDigest::hash(&[]),
        tree: Mutex::new(Some(Arc::new(DirectoryTree::new(BTreeMap::new())))),
        read_blob: Arc::new(DoNotUse {}),
    };
    let nothing = directory
        .access_entry(&FileName::try_from("does not exist").unwrap())
        .await
        .unwrap();
    assert!(nothing.is_none());
}

#[tokio::test]
async fn test_blob_read_directory_access_entry_file() {
    let mut blob_store = MemoryBlobStore::new();
    let file_content = "hello".as_bytes();
    use dogbox_blob_layer::WriteBlob;
    let file_digest = blob_store.write_blob(file_content).await;
    assert_eq!(BlobDigest::hash(file_content), file_digest);
    let file_name = FileName::try_from("file.txt".to_string()).unwrap();
    let file_size = file_content.len();

    let tree = DirectoryTree::new(BTreeMap::from([(
        file_name.clone(),
        crate::serialization::DirectoryEntry::new(
            crate::serialization::DirectoryEntryKind::File(file_size as u64),
            file_digest,
        ),
    )]));

    use postcard::to_allocvec;
    let dir_digest = blob_store.write_blob(&to_allocvec(&tree).unwrap()).await;

    let directory = BlobReadDirectory {
        digest: dir_digest,
        tree: Mutex::new(None),
        read_blob: Arc::new(blob_store),
    };
    let accessor = directory.access_entry(&file_name).await.unwrap().unwrap();
    match accessor {
        EntryAccessor::Directory(_) => panic!(),
        EntryAccessor::File(read_file) => {
            let mut opened = read_file.open().await.unwrap();
            let mut buffer = Vec::new();
            assert_eq!(file_size, opened.read_to_end(&mut buffer).await.unwrap());
        }
    }
}
