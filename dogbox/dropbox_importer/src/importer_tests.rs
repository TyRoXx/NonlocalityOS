use crate::{
    dropbox_api::{
        DownloadRequest, DropboxApi, DropboxFileMetaData, DropboxFolderEntry,
        DropboxFolderEntryKind,
    },
    dropbox_content_hash::{format_dropbox_content_hash, DropboxContentHasher},
    file_cache::{
        FileCacheMap, PersistableFileCacheEntry, Sha256ChunkCacheKey, DEFAULT_CHUNK_SIZE,
    },
    importer::{import_directory, import_file, DropboxImporter, ImportFileOutcome},
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadStoreTree, StrongReference},
    tree::TREE_BLOB_MAX_LENGTH,
};
use async_trait::async_trait;
use bytes::Bytes;
use dogbox_tree::serialization::FileName;
use dogbox_tree_editor::{
    expected_directory_entry_kind::{assert_directory_contents, ExpectedDirectoryEntryKind},
    DigestStatus, OpenDirectory, OpenDirectoryStatus, OpenFileContentBuffer, OpenFileStats,
    OptimizedWriteBuffer, TreeEditor, DEFAULT_WRITE_BUFFER_IN_BLOCKS,
};
use dropbox_sdk::async_routes::files;
use futures::StreamExt;
use relative_path::{RelativePath, RelativePathBuf};
use std::{collections::BTreeMap, pin::Pin, sync::Arc};

struct UnreachableDropboxApi {}

#[async_trait]
impl DropboxApi for UnreachableDropboxApi {
    async fn download_file(
        &self,
        _dropbox_file_path: &str,
        _download_request: &DownloadRequest,
        _storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        unreachable!()
    }

    async fn list_folder(
        &self,
        _dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        unreachable!()
    }
}

struct FailingDropboxApi {}

#[async_trait]
impl DropboxApi for FailingDropboxApi {
    async fn download_file(
        &self,
        _dropbox_file_path: &str,
        _download_request: &DownloadRequest,
        _storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        Err(std::io::Error::other("Simulated download failure"))
    }

    async fn list_folder(
        &self,
        _dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        Err(std::io::Error::other("Simulated folder listing failure"))
    }
}

struct SucceedingDropboxApiFileContentWithRev {
    content: Bytes,
    rev: files::Rev,
}

struct SucceedingDropboxApiDirectory {
    entries: BTreeMap<String, SucceedingDropboxApiDirectoryEntry>,
}

impl SucceedingDropboxApiDirectory {
    pub fn new(entries: BTreeMap<String, SucceedingDropboxApiDirectoryEntry>) -> Self {
        Self { entries }
    }

    pub async fn download_file(
        &self,
        relative_path: &RelativePath,
        dropbox_file_path: &str,
        download_request: &DownloadRequest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        // TODO: support subdirectories in the mock API
        if relative_path.components().count() == 0 {
            unreachable!("Cannot download a directory");
        }
        match self
            .entries
            .get(relative_path.components().next().unwrap().as_str())
        {
            Some(found) => match found {
                SucceedingDropboxApiDirectoryEntry::File(file_content) => {
                    if relative_path.components().count() > 1 {
                        return Err(std::io::Error::other(format!(
                            "Expected directory at path {}, but found a file",
                            dropbox_file_path
                        )));
                    }
                    assert_eq!(download_request.dropbox_rev, file_content.rev);
                    let mut hasher = crate::dropbox_content_hash::DropboxContentHasher::new();
                    hasher.update(&file_content.content);
                    let calculated_dropbox_content_hash = hasher.finalize();
                    if download_request.dropbox_content_hash != calculated_dropbox_content_hash {
                        return Err(std::io::Error::other(format!(
                            "Content hash mismatch for file {}: expected {}, got {}",
                            dropbox_file_path,
                            format_dropbox_content_hash(&download_request.dropbox_content_hash),
                            format_dropbox_content_hash(&calculated_dropbox_content_hash)
                        )));
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
                    let tail = file_content
                        .content
                        .clone()
                        .split_off(download_request.offset as usize);
                    let chunk = tail
                        .clone()
                        .split_to(download_request.length_to_download as usize);
                    assert_eq!(chunk.len(), download_request.length_to_download as usize);
                    let piece = OptimizedWriteBuffer::from_bytes(0, chunk).await;
                    open_file_content_buffer
                        .write(0, piece, storage.clone())
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "Failed to write file content for {}: {e}",
                                dropbox_file_path
                            ))
                        })?;
                    open_file_content_buffer
                        .store_all(storage)
                        .await
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "Failed to store file content for {}: {e}",
                                dropbox_file_path
                            ))
                        })?;
                    let (status, size, reference) = open_file_content_buffer.last_known_digest();
                    assert!(status.is_digest_up_to_date);
                    assert_eq!(status.last_known_digest.digest(), reference.digest());
                    assert_eq!(size, download_request.length_to_download);
                    Ok((reference, size))
                }
                SucceedingDropboxApiDirectoryEntry::Directory(subdirectory) => {
                    if relative_path.components().count() == 1 {
                        return Err(std::io::Error::other(format!(
                            "Expected file at path {}, but found a directory",
                            dropbox_file_path
                        )));
                    }
                    let sub_path = relative_path.components().skip(1).fold(
                        RelativePathBuf::new(),
                        |mut path, component| {
                            path.push(component);
                            path
                        },
                    );
                    Box::pin(subdirectory.download_file(
                        &sub_path,
                        dropbox_file_path,
                        download_request,
                        storage,
                    ))
                    .await
                }
            },
            None => Err(std::io::Error::other(format!(
                "File not found at path {}",
                dropbox_file_path
            ))),
        }
    }

    pub fn list_folder(
        &self,
        relative_path: &RelativePath,
    ) -> std::io::Result<Vec<DropboxFolderEntry>> {
        if relative_path.components().count() == 0 {
            Ok(self
                .entries
                .iter()
                .map(|(name, entry)| {
                    let kind = match entry {
                        SucceedingDropboxApiDirectoryEntry::File(file_content) => {
                            DropboxFolderEntryKind::File {
                                metadata: DropboxFileMetaData {
                                    content_hash: Some(format_dropbox_content_hash(&{
                                        let mut hasher =
                                            crate::dropbox_content_hash::DropboxContentHasher::new(
                                            );
                                        hasher.update(&file_content.content);
                                        hasher.finalize()
                                    })),
                                    rev: file_content.rev.clone(),
                                    size: file_content.content.len() as u64,
                                },
                            }
                        }
                        SucceedingDropboxApiDirectoryEntry::Directory(_) => {
                            DropboxFolderEntryKind::Folder
                        }
                    };
                    DropboxFolderEntry {
                        name: name.clone(),
                        kind,
                    }
                })
                .collect())
        } else {
            let first_component = relative_path.components().next().unwrap();
            match self.entries.get(first_component.as_str()) {
                Some(found) => match found {
                    SucceedingDropboxApiDirectoryEntry::File(_) => {
                        Err(std::io::Error::other("Path is a file, not a directory"))
                    }
                    SucceedingDropboxApiDirectoryEntry::Directory(subdirectory) => subdirectory
                        .list_folder(&relative_path.components().skip(1).fold(
                            RelativePathBuf::new(),
                            |mut path, component| {
                                path.push(component);
                                path
                            },
                        )),
                },
                None => Err(std::io::Error::other("Path not found")),
            }
        }
    }
}

enum SucceedingDropboxApiDirectoryEntry {
    File(SucceedingDropboxApiFileContentWithRev),
    Directory(SucceedingDropboxApiDirectory),
}

struct SucceedingDropboxApi {
    root: SucceedingDropboxApiDirectory,
}

impl SucceedingDropboxApi {
    pub fn new(root: SucceedingDropboxApiDirectory) -> Self {
        Self { root }
    }
}

#[async_trait]
impl DropboxApi for SucceedingDropboxApi {
    async fn download_file(
        &self,
        dropbox_file_path: &str,
        download_request: &DownloadRequest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        let relative_path = RelativePath::new(dropbox_file_path);
        self.root
            .download_file(relative_path, dropbox_file_path, download_request, storage)
            .await
    }

    async fn list_folder(
        &self,
        dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        let relative_path = RelativePath::new(dropbox_folder_path);
        let entries = self.root.list_folder(relative_path)?;
        let stream = futures::stream::iter(entries.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }
}

#[test_log::test(tokio::test)]
async fn test_import_file_missing_content_hash() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let modified = clock();
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let dropbox_api = UnreachableDropboxApi {};
    let outcome = import_file(
        "/test",
        "file.txt",
        &DropboxFileMetaData {
            content_hash: None,
            rev: "1".to_string(),
            size: 0,
        },
        &open_directory,
        storage.clone(),
        &dropbox_api,
        &download_cache,
    )
    .await
    .unwrap();
    assert_eq!(outcome, ImportFileOutcome::MissingContentHash);
    let status = open_directory
        .request_save()
        .await
        .expect("Failed to save directory");
    // the digest doesn't really matter here
    let new_reference = status.digest.last_known_digest.clone();
    assert_eq!(
        status,
        OpenDirectoryStatus::new(
            DigestStatus::new(new_reference, true),
            1,
            0,
            OpenFileStats::new(0, 0, 0, 0, 0),
            modified,
        ),
    )
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_dropbox_failure() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_directory_reference = open_directory.latest_reference();
    let dropbox_api = FailingDropboxApi {};
    let importer = DropboxImporter::new(
        storage.clone(),
        &empty_directory_reference,
        &dropbox_api,
        &download_cache,
    );
    let content_hash = format_dropbox_content_hash(&{
        let hasher = DropboxContentHasher::new();
        hasher.finalize()
    });
    let error = importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(content_hash),
                        rev: "1".to_string(),
                        size: 1,
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Failed to download chunk 0 of /file.txt: Simulated download failure"
    );
    let mut entries = open_directory.read().await;
    if entries.next().await.is_some() {
        panic!("Unexpected directory entry")
    }
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_file_success_small() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_directory_reference = open_directory.latest_reference();
    let content = Bytes::from_static(b"Hello, world!");
    let rev = "1";
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([(
            "file.txt".to_string(),
            SucceedingDropboxApiDirectoryEntry::File(SucceedingDropboxApiFileContentWithRev {
                content: content.clone(),
                rev: rev.to_string(),
            }),
        )]),
    });
    let importer = DropboxImporter::new(
        storage.clone(),
        &empty_directory_reference,
        &dropbox_api,
        &download_cache,
    );
    let content_hash = format_dropbox_content_hash(&{
        let mut hasher = DropboxContentHasher::new();
        hasher.update(&content);
        hasher.finalize()
    });
    importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(content_hash),
                        rev: rev.to_string(),
                        size: content.len() as u64,
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap();
    assert_directory_contents(
        &open_directory,
        &BTreeMap::from([(
            FileName::try_from("file.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(content),
        )]),
    )
    .await;
}

fn random_bytes(len: usize) -> Bytes {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    Bytes::from_iter((0..len).map(|_| small_rng.gen()))
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_file_success_large() {
    // Download a file that requires multiple file cache chunks.
    // We keep the chunk size much smaller than the default to keep this test quick.
    let chunk_size = (TREE_BLOB_MAX_LENGTH as u64) * 2;
    let content = random_bytes((chunk_size * 2 + 1) as usize);
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_directory_reference = open_directory.latest_reference();
    let rev = "1";
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([(
            "file.txt".to_string(),
            SucceedingDropboxApiDirectoryEntry::File(SucceedingDropboxApiFileContentWithRev {
                content: content.clone(),
                rev: rev.to_string(),
            }),
        )]),
    });
    let importer = DropboxImporter::new(
        storage.clone(),
        &empty_directory_reference,
        &dropbox_api,
        &download_cache,
    );
    let content_hash = format_dropbox_content_hash(&{
        let mut hasher = DropboxContentHasher::new();
        hasher.update(&content);
        hasher.finalize()
    });
    importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(content_hash),
                        rev: rev.to_string(),
                        size: content.len() as u64,
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap();
    assert_directory_contents(
        &open_directory,
        &BTreeMap::from([(
            FileName::try_from("file.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(content),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_subdirectory_success() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_directory_reference = open_directory.latest_reference();
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([(
            "subdir".to_string(),
            SucceedingDropboxApiDirectoryEntry::Directory(SucceedingDropboxApiDirectory::new(
                BTreeMap::new(),
            )),
        )]),
    });
    let importer = DropboxImporter::new(
        storage.clone(),
        &empty_directory_reference,
        &dropbox_api,
        &download_cache,
    );
    importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "subdir".to_string(),
                kind: DropboxFolderEntryKind::Folder,
            },
            &open_directory,
        )
        .await
        .unwrap();
    assert_directory_contents(
        &open_directory,
        &BTreeMap::from([(
            FileName::try_from("subdir").unwrap(),
            ExpectedDirectoryEntryKind::Directory(BTreeMap::new()),
        )]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_import_directory_dropbox_failure() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = Arc::new(
        OpenDirectory::create_directory(
            std::path::PathBuf::from("/"),
            storage.clone(),
            clock.clone(),
            1,
        )
        .await
        .unwrap(),
    );
    let dropbox_api = FailingDropboxApi {};
    let error = import_directory("/", storage.clone(), clock, &dropbox_api, &download_cache)
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Simulated folder listing failure");
    let mut entries = open_directory.read().await;
    if entries.next().await.is_some() {
        panic!("Unexpected directory entry")
    }
}

#[test_log::test(tokio::test)]
async fn test_import_directory_simple_success() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let content = Bytes::from_static(b"Hello, world!");
    let rev = "1";
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([
            (
                "file.txt".to_string(),
                SucceedingDropboxApiDirectoryEntry::File(SucceedingDropboxApiFileContentWithRev {
                    content: content.clone(),
                    rev: rev.to_string(),
                }),
            ),
            (
                "subdir".to_string(),
                SucceedingDropboxApiDirectoryEntry::Directory(SucceedingDropboxApiDirectory::new(
                    BTreeMap::new(),
                )),
            ),
        ]),
    });
    let imported_directory =
        import_directory("/", storage.clone(), clock, &dropbox_api, &download_cache)
            .await
            .unwrap();
    assert_directory_contents(
        &imported_directory,
        &BTreeMap::from([
            (
                FileName::try_from("file.txt").unwrap(),
                ExpectedDirectoryEntryKind::File(content),
            ),
            (
                FileName::try_from("subdir").unwrap(),
                ExpectedDirectoryEntryKind::Directory(BTreeMap::new()),
            ),
        ]),
    )
    .await;
}

#[test_log::test(tokio::test)]
async fn test_import_directory_recursive_success() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = DEFAULT_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([
            (
                "file.txt".to_string(),
                SucceedingDropboxApiDirectoryEntry::File(SucceedingDropboxApiFileContentWithRev {
                    content: Bytes::from_static(b"Hello, world!"),
                    rev: "1".to_string(),
                }),
            ),
            (
                "a".to_string(),
                SucceedingDropboxApiDirectoryEntry::Directory(SucceedingDropboxApiDirectory::new(
                    BTreeMap::from([(
                        "b".to_string(),
                        SucceedingDropboxApiDirectoryEntry::Directory(
                            SucceedingDropboxApiDirectory::new(BTreeMap::from([
                                (
                                    "c".to_string(),
                                    SucceedingDropboxApiDirectoryEntry::File(
                                        SucceedingDropboxApiFileContentWithRev {
                                            content: Bytes::from_static(b"Nested file content"),
                                            rev: "2".to_string(),
                                        },
                                    ),
                                ),
                                (
                                    "d".to_string(),
                                    SucceedingDropboxApiDirectoryEntry::Directory(
                                        SucceedingDropboxApiDirectory::new(BTreeMap::new()),
                                    ),
                                ),
                            ])),
                        ),
                    )]),
                )),
            ),
        ]),
    });
    let imported_directory =
        import_directory("/", storage.clone(), clock, &dropbox_api, &download_cache)
            .await
            .unwrap();
    assert_directory_contents(
        &imported_directory,
        &BTreeMap::from([
            (
                FileName::try_from("file.txt").unwrap(),
                ExpectedDirectoryEntryKind::File(Bytes::from(b"Hello, world!".to_vec())),
            ),
            (
                FileName::try_from("a").unwrap(),
                ExpectedDirectoryEntryKind::Directory(BTreeMap::from([(
                    FileName::try_from("b").unwrap(),
                    ExpectedDirectoryEntryKind::Directory(BTreeMap::from([
                        (
                            FileName::try_from("c").unwrap(),
                            ExpectedDirectoryEntryKind::File(Bytes::from(
                                b"Nested file content".to_vec(),
                            )),
                        ),
                        (
                            FileName::try_from("d").unwrap(),
                            ExpectedDirectoryEntryKind::Directory(BTreeMap::new()),
                        ),
                    ])),
                )])),
            ),
        ]),
    )
    .await;
}
