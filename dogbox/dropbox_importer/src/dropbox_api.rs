use crate::dropbox_content_hash::DropboxContentHasher;
use astraea::{
    storage::{LoadStoreTree, StrongReference},
    tree::TREE_BLOB_MAX_LENGTH,
};
use async_stream::stream;
use async_trait::async_trait;
use bytes::Bytes;
use dogbox_tree_editor::{
    OpenFileContentBuffer, OptimizedWriteBuffer, TreeEditor, DEFAULT_WRITE_BUFFER_IN_BLOCKS,
};
use dropbox_sdk::{async_routes::files, default_async_client::UserAuthDefaultClient};
use futures::io::AsyncReadExt;
use sha2::Sha256;
use std::{pin::Pin, sync::Arc};
use tracing::{error, info, warn};

pub type Sha256Digest = sha2::digest::Output<Sha256>;

pub fn parse_sha256_hex(content_hash_string: &str) -> Option<Sha256Digest> {
    match hex::decode(content_hash_string) {
        Ok(success) => Sha256Digest::try_from(&success[..]).ok(),
        Err(error) => {
            info!("Failed to decode hex string: {}", error);
            None
        }
    }
}

pub fn join_dropbox_path(parent: &str, child: &str) -> String {
    let child = child.trim_start_matches('/');
    let parent = parent.trim_end_matches('/');
    if parent.is_empty() {
        format!("/{}", child)
    } else {
        format!("{parent}/{child}")
    }
}

async fn download_file_impl(
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_file_path: &str,
    dropbox_file_rev: &files::Rev,
    dropbox_content_hash: &Sha256Digest,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) -> std::io::Result<(StrongReference, u64)> {
    // download the file from Dropbox in pieces:
    // Start a download session for the file from Dropbox
    info!("Starting download for {}", dropbox_file_path);
    let download_arg =
        files::DownloadArg::new(dropbox_file_path.to_string()).with_rev(dropbox_file_rev.clone());
    let response = match files::download(dropbox_client.as_ref(), &download_arg, None, None).await {
        Ok(res) => res,
        Err(e) => {
            return Err(std::io::Error::other(format!(
                "Failed to download file {}: {e}",
                dropbox_file_path
            )));
        }
    };

    let file_size = match response.content_length {
        Some(size) => size,
        None => {
            return Err(std::io::Error::other(format!(
                "Content length is missing for file {}",
                dropbox_file_path
            )));
        }
    };

    let empty_file_reference = TreeEditor::store_empty_file(storage.clone())
        .await
        .map_err(|e| {
            std::io::Error::other(format!(
                "Failed to store empty file for {}: {e}",
                dropbox_file_path
            ))
        })?;
    let mut open_file_content_buffer = OpenFileContentBuffer::NotLoaded {
        reference: empty_file_reference,
        size: 0,
        write_buffer_in_blocks: DEFAULT_WRITE_BUFFER_IN_BLOCKS,
    };
    let mut dropbox_hasher = DropboxContentHasher::new();
    let mut total_bytes_read = 0;
    let mut stream = match response.body {
        Some(stream) => stream,
        None => {
            return Err(std::io::Error::other(format!(
                "Failed to get response body for file {}",
                dropbox_file_path
            )));
        }
    };
    loop {
        let remaining_bytes = file_size - total_bytes_read;
        if remaining_bytes == 0 {
            break;
        }
        let chunk_size = std::cmp::min(
            remaining_bytes,
            /*use chunk size preferred by Dogbox for efficiency*/
            TREE_BLOB_MAX_LENGTH as u64,
        ) as usize;
        let mut buffer = vec![0u8; chunk_size];
        let bytes_read = stream.read(&mut buffer).await.map_err(|e| {
            std::io::Error::other(format!(
                "Failed to read download stream for file {}: {e}",
                dropbox_file_path
            ))
        })?;
        if bytes_read == 0 {
            return Err(std::io::Error::other(format!(
                    "Unexpected end of stream while downloading file {}: expected {} bytes, got {} bytes",
                    dropbox_file_path, file_size, total_bytes_read)));
        }
        buffer.truncate(bytes_read);

        dropbox_hasher.update(&buffer);

        let read_size = buffer.len() as u64;
        assert!(read_size <= chunk_size as u64);
        open_file_content_buffer
            .write(
                total_bytes_read,
                OptimizedWriteBuffer::from_bytes(total_bytes_read, Bytes::from(buffer)).await,
                storage.clone(),
            )
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to write to file {}: {e}",
                    dropbox_file_path
                ))
            })?;

        total_bytes_read += read_size;
        assert!(total_bytes_read <= file_size);
    }

    // we should never break the loop unless the buffer is completely filled
    assert_eq!(file_size, open_file_content_buffer.size());

    info!("Downloaded {} bytes for {}", file_size, dropbox_file_path);

    let calculated_dropbox_content_hash: Sha256Digest = dropbox_hasher.finalize();
    if dropbox_content_hash != &calculated_dropbox_content_hash {
        return Err(std::io::Error::other(format!(
            "Content hash mismatch for file {}: expected {}, got {}",
            dropbox_file_path,
            hex::encode(dropbox_content_hash),
            hex::encode(calculated_dropbox_content_hash)
        )));
    }

    open_file_content_buffer
        .store_all(storage)
        .await
        .map_err(|e| {
            std::io::Error::other(format!(
                "Failed to store content buffer for file {}: {e}",
                dropbox_file_path
            ))
        })?;
    let (digest_status, size, reference) = open_file_content_buffer.last_known_digest();
    assert_eq!(file_size, size);
    assert!(digest_status.is_digest_up_to_date);
    Ok((reference, file_size))
}

async fn list_folder_impl(
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_folder_path: &str,
) -> std::io::Result<Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>>
{
    let dropbox_client = dropbox_client.clone();
    let dropbox_folder_path = dropbox_folder_path.to_string();
    let mut list_folder_result = match files::list_folder(
        dropbox_client.as_ref(),
        &files::ListFolderArg::new(dropbox_folder_path.clone()).with_recursive(false),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            return Err(std::io::Error::other(format!(
                "Failed to list_folder {}: {e}",
                dropbox_folder_path
            )));
        }
    };
    Ok(Box::pin(stream! {
        let mut cursor = list_folder_result.cursor;
        loop {
            info!("Directory entries: {}", list_folder_result.entries.len());
            for entry in list_folder_result.entries {
                match entry {
                    files::Metadata::Folder(entry) => {
                        info!("Folder entry: {}", entry.name);
                        yield Ok(DropboxFolderEntry { name: entry.name, kind: DropboxFolderEntryKind::Folder });
                    }
                    files::Metadata::File(entry) => {
                        info!("File entry: {}", entry.name);
                        yield Ok(DropboxFolderEntry { name: entry.name, kind: DropboxFolderEntryKind::File {
                            metadata: DropboxFileMetaData {
                            content_hash: entry.content_hash,
                            rev: entry.rev,
                        }}});
                    }
                    files::Metadata::Deleted(entry) => {
                        info!("Ignoring deleted entry: {:?}", entry);
                    }
                }
            }
            if !list_folder_result.has_more {
                break;
            }
            list_folder_result = match files::list_folder_continue(
                dropbox_client.as_ref(),
                &files::ListFolderContinueArg::new(cursor.clone()),
            )
            .await
            {
                Ok(result) => result,
                Err(e) => {
                    error!("Error from list_folder_continue: {e}");
                    yield Err(std::io::Error::other(format!(
                        "Failed to list_folder_continue {}: {e}",
                        dropbox_folder_path
                    )));
                    break;
                }
            };
            if cursor != list_folder_result.cursor {
                warn!("Dropbox list_folder_continue cursor changed. Normally it doesn't change.");
            }
            cursor = list_folder_result.cursor;
        }
    }))
}

pub struct DropboxFileMetaData {
    pub content_hash: Option<String>,
    pub rev: String,
}

pub enum DropboxFolderEntryKind {
    File { metadata: DropboxFileMetaData },
    Folder,
}

pub struct DropboxFolderEntry {
    pub name: String,
    pub kind: DropboxFolderEntryKind,
}

#[async_trait]
pub trait DropboxApi {
    async fn download_file(
        &self,
        dropbox_file_path: &str,
        dropbox_file_rev: &files::Rev,
        dropbox_content_hash: &Sha256Digest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)>;

    async fn list_folder(
        &self,
        dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    >;
}

pub struct RealDropboxApi {
    pub dropbox_client: Arc<UserAuthDefaultClient>,
}

#[async_trait]
impl DropboxApi for RealDropboxApi {
    async fn download_file(
        &self,
        dropbox_file_path: &str,
        dropbox_file_rev: &files::Rev,
        dropbox_content_hash: &Sha256Digest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        download_file_impl(
            &self.dropbox_client,
            dropbox_file_path,
            dropbox_file_rev,
            dropbox_content_hash,
            storage,
        )
        .await
    }

    async fn list_folder(
        &self,
        dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        list_folder_impl(&self.dropbox_client, dropbox_folder_path).await
    }
}
