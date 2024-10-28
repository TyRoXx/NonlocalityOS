#[cfg(test)]
use crate::serialization::DirectoryTree;
#[cfg(test)]
use astraea::storage::LoadValue;
#[cfg(test)]
use astraea::tree::BlobDigest;
use std::pin::Pin;
#[cfg(test)]
use std::sync::Arc;

struct BlobOpenFile {
    content: Vec<u8>,
    cursor: usize,
}

impl crate::reading::AsyncReadBlob for BlobOpenFile {}

impl tokio::io::AsyncRead for BlobOpenFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
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
    fn start_seek(self: Pin<&mut Self>, _position: std::io::SeekFrom) -> std::io::Result<()> {
        todo!()
    }

    fn poll_complete(
        self: Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        reading::{ReadDirectory, Stream},
        serialization::FileName,
    };
    use astraea::tree::{Reference, ReferenceIndex};
    use async_stream::stream;
    use async_trait::async_trait;
    use std::collections::BTreeMap;
    use tokio::sync::Mutex;

    struct BlobReadDirectory<'t> {
        digest: BlobDigest,
        tree: tokio::sync::Mutex<Option<Arc<DirectoryTree>>>,
        read_blob: &'t (dyn LoadValue + Sync),
    }

    fn parse_directory_blob(data: &[u8]) -> Option<DirectoryTree> {
        match postcard::from_bytes(data) {
            Ok(success) => Some(success),
            Err(_) => None,
        }
    }

    impl<'t> BlobReadDirectory<'t> {
        async fn require_tree(&self) -> crate::reading::Result<Arc<DirectoryTree>> {
            let mut tree_locked = self.tree.lock().await;
            match tree_locked.as_ref() {
                Some(exists) => Ok(exists.clone()),
                None => match self.read_blob.load_value(&Reference::new(self.digest)) {
                    Some(blob_content) => {
                        match parse_directory_blob(blob_content.blob.as_slice()) {
                            Some(parsed) => {
                                let result = Arc::new(parsed);
                                *tree_locked = Some(result.clone());
                                Ok(result)
                            }
                            None => Err(crate::reading::Error::DataIncompatible),
                        }
                    }
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
    impl<'a> ReadDirectory for BlobReadDirectory<'a> {
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
    }

    pub struct DoNotUse {}

    impl LoadValue for DoNotUse {
        fn load_value(&self, _reference: &Reference) -> Option<Arc<astraea::tree::Value>> {
            panic!()
        }
    }

    #[tokio::test]
    async fn test_blob_read_directory_enumerate_empty() {
        use std::collections::BTreeMap;
        let directory = BlobReadDirectory {
            digest: BlobDigest::hash(&[]),
            tree: Mutex::new(Some(Arc::new(DirectoryTree::new(BTreeMap::new())))),
            read_blob: &DoNotUse {},
        };
        let mut entries = directory.enumerate().await.unwrap();
        let entry = futures_util::StreamExt::next(&mut entries).await;
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
                        crate::serialization::ReferenceIndexOrInlineContent::Indirect(
                            ReferenceIndex(0),
                        ),
                    ),
                ),
                (
                    file_name.clone(),
                    crate::serialization::DirectoryEntry::new(
                        crate::serialization::DirectoryEntryKind::File(file_size),
                        crate::serialization::ReferenceIndexOrInlineContent::Indirect(
                            ReferenceIndex(1),
                        ),
                    ),
                ),
            ]))))),
            read_blob: &DoNotUse {},
        };
        let mut entries = directory.enumerate().await.unwrap();
        assert_eq!(
            crate::reading::DirectoryEntry::new(
                dir_name,
                crate::reading::DirectoryEntryInfo::Directory
            ),
            futures_util::StreamExt::next(&mut entries).await.unwrap()
        );
        assert_eq!(
            crate::reading::DirectoryEntry::new(
                file_name,
                crate::reading::DirectoryEntryInfo::File(file_size)
            ),
            futures_util::StreamExt::next(&mut entries).await.unwrap()
        );
        assert!(futures_util::StreamExt::next(&mut entries).await.is_none());
    }
}
