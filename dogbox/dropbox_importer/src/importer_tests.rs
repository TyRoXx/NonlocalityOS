use crate::{
    dropbox_api::{
        DropboxApi, DropboxFileMetaData, DropboxFolderEntry, DropboxFolderEntryKind, Sha256Digest,
    },
    dropbox_content_hash::format_dropbox_content_hash,
    file_cache::{FileCacheMap, PersistableFileCacheEntry, Sha256CacheKey},
    importer::{import_directory, import_file, DropboxImporter, ImportFileOutcome},
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{LoadStoreTree, StoreTree, StrongReference},
    tree::{HashedTree, Tree, TreeBlob, TreeChildren},
};
use async_trait::async_trait;
use bytes::Bytes;
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use dogbox_tree_editor::{
    DigestStatus, MutableDirectoryEntry, OpenDirectory, OpenDirectoryStatus, OpenFileContentBuffer,
    OpenFileStats, OptimizedWriteBuffer, TreeEditor, DEFAULT_WRITE_BUFFER_IN_BLOCKS,
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
        _dropbox_file_rev: &files::Rev,
        _dropbox_content_hash: &Sha256Digest,
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
        _dropbox_file_rev: &files::Rev,
        _dropbox_content_hash: &Sha256Digest,
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
    content: Vec<u8>,
    rev: files::Rev,
}

struct SucceedingDropboxApiDirectory {
    entries: BTreeMap<String, SucceedingDropboxApiDirectoryEntry>,
}

impl SucceedingDropboxApiDirectory {
    pub fn new(entries: BTreeMap<String, SucceedingDropboxApiDirectoryEntry>) -> Self {
        Self { entries }
    }

    pub fn list(&self, relative_path: &RelativePath) -> std::io::Result<Vec<DropboxFolderEntry>> {
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
                        .list(&relative_path.components().skip(1).fold(
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
        dropbox_file_rev: &files::Rev,
        dropbox_content_hash: &Sha256Digest,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
    ) -> std::io::Result<(StrongReference, u64)> {
        let relative_path = RelativePath::new(dropbox_file_path);
        // TODO: support subdirectories in the mock API
        assert_eq!(1, relative_path.components().count());
        match self
            .root
            .entries
            .get(relative_path.components().next().unwrap().as_str())
        {
            Some(found) => match found {
                SucceedingDropboxApiDirectoryEntry::File(file_content) => {
                    assert_eq!(dropbox_file_rev, &file_content.rev);
                    let mut hasher = crate::dropbox_content_hash::DropboxContentHasher::new();
                    hasher.update(&file_content.content);
                    let calculated_dropbox_content_hash = hasher.finalize();
                    if dropbox_content_hash != &calculated_dropbox_content_hash {
                        return Err(std::io::Error::other(format!(
                            "Content hash mismatch for file {}: expected {}, got {}",
                            dropbox_file_path,
                            format_dropbox_content_hash(dropbox_content_hash),
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
                    let piece = OptimizedWriteBuffer::from_bytes(
                        0,
                        Bytes::from(file_content.content.clone()),
                    )
                    .await;
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
                    assert_eq!(size, file_content.content.len() as u64);
                    Ok((reference, size))
                }
                SucceedingDropboxApiDirectoryEntry::Directory(_) => {
                    Err(std::io::Error::other(format!(
                        "Expected file at path {}, but found a directory",
                        dropbox_file_path
                    )))
                }
            },
            None => Err(std::io::Error::other(format!(
                "File not found at path {}",
                dropbox_file_path
            ))),
        }
    }

    async fn list_folder(
        &self,
        dropbox_folder_path: &str,
    ) -> std::io::Result<
        Pin<Box<dyn futures::Stream<Item = std::io::Result<DropboxFolderEntry>> + Send>>,
    > {
        let relative_path = RelativePath::new(dropbox_folder_path);
        let entries = self.root.list(&relative_path)?;
        let stream = futures::stream::iter(entries.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }
}

#[test_log::test(tokio::test)]
async fn test_import_file_missing_content_hash() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let original_content_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let modified = clock();
    let open_directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(original_content_reference.clone(), false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        clock,
        1,
    ));
    let dropbox_api = UnreachableDropboxApi {};
    let outcome = import_file(
        "/test",
        "file.txt",
        &DropboxFileMetaData {
            content_hash: None,
            rev: "1".to_string(),
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
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
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
    let error = importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(
                            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
                                .to_string(),
                        ),
                        rev: "1".to_string(),
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Failed to download /file.txt: Simulated download failure"
    );
    let mut entries = open_directory.read().await;
    if entries.next().await.is_some() {
        panic!("Unexpected directory entry")
    }
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_file_success() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let modified = clock();
    let open_directory = Arc::new(
        OpenDirectory::create_directory(std::path::PathBuf::from("/"), storage.clone(), clock, 1)
            .await
            .unwrap(),
    );
    let empty_directory_reference = open_directory.latest_reference();
    let content = b"Hello, world!";
    let rev = "1";
    let dropbox_api = SucceedingDropboxApi::new(SucceedingDropboxApiDirectory {
        entries: BTreeMap::from([(
            "file.txt".to_string(),
            SucceedingDropboxApiDirectoryEntry::File(SucceedingDropboxApiFileContentWithRev {
                content: content.to_vec(),
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
    importer
        .import_directory_entry(
            "/",
            &DropboxFolderEntry {
                name: "file.txt".to_string(),
                kind: DropboxFolderEntryKind::File {
                    metadata: DropboxFileMetaData {
                        content_hash: Some(
                            "6246efc88ae4aa025e48c9c7adc723d5c97171a1fa6233623c7251ab8e57602f"
                                .to_string(),
                        ),
                        rev: rev.to_string(),
                    },
                },
            },
            &open_directory,
        )
        .await
        .unwrap();
    let expected_entries = [MutableDirectoryEntry::new(
        FileName::try_from("file.txt").unwrap(),
        DirectoryEntryKind::File(content.len() as u64),
        modified,
    )];
    let actual_entries = open_directory.read().await.collect::<Vec<_>>().await;
    assert_eq!(actual_entries, expected_entries);
}

#[test_log::test(tokio::test)]
async fn test_import_directory_entry_subdirectory_success() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let modified = clock();
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
    let expected_entries = [MutableDirectoryEntry::new(
        FileName::try_from("subdir").unwrap(),
        DirectoryEntryKind::Directory,
        modified,
    )];
    let actual_entries = open_directory.read().await.collect::<Vec<_>>().await;
    assert_eq!(actual_entries, expected_entries);
}

#[test_log::test(tokio::test)]
async fn test_import_directory_dropbox_failure() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256CacheKey,
        PersistableFileCacheEntry,
    >::new();
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage);
    let original_content_reference = storage
        .store_tree(&HashedTree::from(Arc::new(Tree::new(
            TreeBlob::empty(),
            TreeChildren::empty(),
        ))))
        .await
        .unwrap();
    let modified = clock();
    let open_directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(original_content_reference.clone(), false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        clock.clone(),
        1,
    ));
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
