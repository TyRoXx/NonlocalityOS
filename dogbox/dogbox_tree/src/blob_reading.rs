use crate::reading::{EntryAccessor, ReadDirectory, Stream};
use crate::serialization::{DirectoryTree, FileName};
use async_stream::stream;
use async_trait::async_trait;
use dogbox_blob_layer::BlobDigest;
use futures_util::StreamExt;
use std::collections::BTreeMap;

struct BlobReadDirectory {
    tree: DirectoryTree,
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
    async fn enumerate<'t>(&'t self) -> Stream<'t, crate::reading::DirectoryEntry> {
        Box::pin(stream! {
            for child in &self.tree.children {
                yield crate::reading::DirectoryEntry::new(child.0.clone(), convert_entry_info(&child.1.kind));
            }
        })
    }

    async fn access_entry(&self, _name: &FileName) -> Option<EntryAccessor> {
        todo!()
    }
}

#[tokio::test]
async fn test_blob_read_directory_enumerate_empty() {
    let directory = BlobReadDirectory {
        tree: DirectoryTree::new(BTreeMap::new()),
    };
    let mut entries = directory.enumerate().await;
    let entry = entries.next().await;
    assert!(entry.is_none());
}

#[tokio::test]
async fn test_blob_read_directory_enumerate_non_empty() {
    let dir_name = FileName::try_from("dir".to_string()).unwrap();
    let file_name = FileName::try_from("file.txt".to_string()).unwrap();
    let file_size = 123;
    let directory = BlobReadDirectory {
        tree: DirectoryTree::new(BTreeMap::from([
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
        ])),
    };
    let mut entries = directory.enumerate().await;
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
