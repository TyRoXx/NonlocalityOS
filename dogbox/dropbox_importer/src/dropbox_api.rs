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

pub fn calculate_range_end(offset: u64, length_to_download: u64) -> std::io::Result<u64> {
    if length_to_download == 0 {
        return Err(std::io::Error::other(
            "Download range must be at least 1 byte long",
        ));
    }
    offset
        .checked_add(length_to_download)
        .and_then(|end| end.checked_sub(1))
        .ok_or_else(|| std::io::Error::other("Invalid download offset or length"))
}

async fn download_file_impl(
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_file_path: &str,
    download_request: &DownloadRequest,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) -> std::io::Result<(StrongReference, u64)> {
    // download the file from Dropbox in pieces:
    // Start a download session for the file from Dropbox
    info!(
        "Starting download for {} (rev:{}) at offset {} for length {}",
        dropbox_file_path,
        download_request.dropbox_rev,
        download_request.offset,
        download_request.length_to_download
    );
    // https://www.dropbox.com/developers/documentation/http/documentation#files-download
    let download_arg = files::DownloadArg::new(format!("rev:{}", download_request.dropbox_rev));
    // -1 because of HTTP Range (https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Range_requests)
    // TODO: sanity check this calculation
    let range_end =
        calculate_range_end(download_request.offset, download_request.length_to_download)?;
    let response = match files::download(
        dropbox_client.as_ref(),
        &download_arg,
        Some(download_request.offset),
        Some(range_end),
    )
    .await
    {
        Ok(res) => res,
        Err(e) => {
            return Err(std::io::Error::other(format!(
                "Failed to download file {}: {e}",
                dropbox_file_path
            )));
        }
    };

    info!(
        "Download file content length: {:?}, result: {:?}",
        response.content_length, response.result
    );
    let download_size = download_request.length_to_download;
    match &response.content_length {
        // Just a sanity check.
        Some(content_length) if download_size != *content_length => {
            return Err(std::io::Error::other(format!(
                "Content length mismatch for file {}: requested {}, got {}",
                dropbox_file_path, download_size, content_length
            )));
        }
        Some(_) => {}
        None => {
            // response.content_length is suddenly sometimes None (2026-07-03) even though it had been Some before.
        }
    }
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
        let remaining_bytes = download_size - total_bytes_read;
        if remaining_bytes == 0 {
            break;
        }
        let chunk_size = std::cmp::min(
            remaining_bytes,
            /*use chunk size preferred by Dogbox for efficiency*/
            TREE_BLOB_MAX_LENGTH as u64,
        ) as usize;
        // TODO: read more at once for greater efficiency
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
                    dropbox_file_path, download_size, total_bytes_read)));
        }
        buffer.truncate(bytes_read);

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
        assert!(total_bytes_read <= download_size);
    }

    // we should never break the loop unless the buffer is completely filled
    assert_eq!(download_size, open_file_content_buffer.size());

    info!(
        "Downloaded {} bytes for {}",
        download_size, dropbox_file_path
    );

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
    assert_eq!(download_size, size);
    assert!(digest_status.is_digest_up_to_date);
    Ok((reference, download_size))
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
                            size: entry.size,
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
    pub size: u64,
}

pub enum DropboxFolderEntryKind {
    File { metadata: DropboxFileMetaData },
    Folder,
}

pub struct DropboxFolderEntry {
    pub name: String,
    pub kind: DropboxFolderEntryKind,
}

pub struct DownloadRequest {
    pub dropbox_rev: files::Rev,
    pub dropbox_content_hash: Sha256Digest,
    pub offset: u64,
    pub length_to_download: u64,
}

#[async_trait]
pub trait DropboxApi {
    async fn download_file(
        &self,
        dropbox_file_path: &str,
        download_request: &DownloadRequest,
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
        download_request: &DownloadRequest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        // We call this function because code coverage doesn't work for async_traits.
        download_file_impl(
            &self.dropbox_client,
            dropbox_file_path,
            download_request,
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
        // We call this function because code coverage doesn't work for async_traits.
        list_folder_impl(&self.dropbox_client, dropbox_folder_path).await
    }
}
