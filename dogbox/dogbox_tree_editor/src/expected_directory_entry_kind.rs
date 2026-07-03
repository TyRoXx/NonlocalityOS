use crate::{FileCreationMode, OpenDirectory, OpenFile};
use astraea::tree::TREE_BLOB_MAX_LENGTH;
use bytes::Bytes;
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use futures::StreamExt;
use std::{collections::BTreeMap, sync::Arc};
use tracing::error;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ExpectedDirectoryEntryKind {
    Directory(BTreeMap<FileName, ExpectedDirectoryEntryKind>),
    File(Bytes),
}

async fn read_to_end(open_file: &OpenFile) -> std::io::Result<Bytes> {
    let read_permission = open_file.get_read_permission();
    let file_size = open_file.size().await;
    let mut total_bytes_read = 0u64;
    let mut result = Vec::new();
    while total_bytes_read < file_size {
        let bounded_size = usize::try_from(std::cmp::min(
            file_size - total_bytes_read,
            TREE_BLOB_MAX_LENGTH as u64,
        ))
        .unwrap();
        let buffer = open_file
            .read_bytes(&read_permission, total_bytes_read, bounded_size)
            .await
            .map_err(|e| {
                error!("Error reading file at offset {}: {e}", total_bytes_read);
                std::io::Error::other(format!(
                    "Failed to read file at offset {}: {e}",
                    total_bytes_read
                ))
            })?;
        if buffer.is_empty() {
            return Err(std::io::Error::other(format!(
                "Unexpected end of file: read 0 bytes at offset {} but file size is {}",
                total_bytes_read, file_size
            )));
        }
        total_bytes_read += buffer.len() as u64;
        result.extend_from_slice(&buffer);
    }
    Ok(Bytes::from(result))
}

async fn read_directory_recursively(
    open_directory: &Arc<OpenDirectory>,
) -> BTreeMap<FileName, ExpectedDirectoryEntryKind> {
    let mut directory_reader = open_directory.read().await;
    let mut entries = BTreeMap::new();
    while let Some(entry) = directory_reader.next().await {
        let kind = match entry.kind {
            DirectoryEntryKind::Directory => {
                let open_subdirectory = open_directory
                    .clone()
                    .open_subdirectory(entry.name.clone())
                    .await
                    .expect("Failed to open subdirectory");
                let sub_entries = Box::pin(read_directory_recursively(&open_subdirectory)).await;
                ExpectedDirectoryEntryKind::Directory(sub_entries)
            }
            DirectoryEntryKind::File(size) => {
                let open_file = open_directory
                    .clone()
                    .open_file(&entry.name, FileCreationMode::open_existing())
                    .await
                    .expect("Failed to open file");
                assert_eq!(size, open_file.size().await);
                let read_content = read_to_end(&open_file)
                    .await
                    .expect("Failed to read file content");
                ExpectedDirectoryEntryKind::File(read_content)
            }
        };
        entries.insert(entry.name.clone(), kind);
    }
    entries
}

pub async fn assert_directory_contents(
    open_directory: &Arc<OpenDirectory>,
    expected_entries: &BTreeMap<FileName, ExpectedDirectoryEntryKind>,
) {
    let entries = read_directory_recursively(open_directory).await;
    assert_eq!(entries, *expected_entries);
}
