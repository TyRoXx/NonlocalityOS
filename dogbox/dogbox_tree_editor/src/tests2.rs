use crate::{
    AccessOrderLowerIsMoreRecent, DigestStatus, DirectoryEntryKind, DirectoryEntryMetaData, Error,
    MutableDirectoryEntry, NamedEntry, NormalizedPath, OpenDirectory, OpenDirectoryStatus,
    OpenFileContentBlock, OpenFileContentBuffer, OpenFileStats, OptimizedWriteBuffer, Prefetcher,
    StreakDirection, TreeEditor,
};
use astraea::storage::{DelayedHashedTree, InMemoryTreeStorage, LoadTree, StoreError, StoreTree};
use astraea::tree::{calculate_reference, TreeChildren};
use astraea::{
    storage::LoadStoreTree,
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TREE_BLOB_MAX_LENGTH},
};
use async_trait::async_trait;
use dogbox_tree::serialization::FileName;
use lazy_static::lazy_static;
use pretty_assertions::assert_eq;
use pretty_assertions::assert_ne;
use std::collections::BTreeMap;
use std::{
    collections::{BTreeSet, VecDeque},
    sync::Arc,
};
use test_case::{test_case, test_matrix};
use tokio::runtime::Runtime;

#[test_log::test(test)]
fn test_normalized_path_from() {
    assert_eq!(
        NormalizedPath::root(),
        NormalizedPath::try_from(relative_path::RelativePath::new("")).unwrap()
    );
}

#[test_log::test(test)]
fn test_streak_direction() {
    assert_eq!(
        StreakDirection::Neither,
        StreakDirection::detect_from_block_access_order(&[])
    );
    assert_eq!(
        StreakDirection::Neither,
        StreakDirection::detect_from_block_access_order(&[AccessOrderLowerIsMoreRecent(0)])
    );
    assert_eq!(
        StreakDirection::Up,
        StreakDirection::detect_from_block_access_order(&[
            AccessOrderLowerIsMoreRecent(1),
            AccessOrderLowerIsMoreRecent(0)
        ])
    );
    assert_eq!(
        StreakDirection::Down,
        StreakDirection::detect_from_block_access_order(&[
            AccessOrderLowerIsMoreRecent(0),
            AccessOrderLowerIsMoreRecent(1)
        ])
    );
    assert_eq!(
        StreakDirection::Up,
        StreakDirection::detect_from_block_access_order(&[
            AccessOrderLowerIsMoreRecent(2),
            AccessOrderLowerIsMoreRecent(1),
            AccessOrderLowerIsMoreRecent(0)
        ])
    );
    assert_eq!(
        StreakDirection::Up,
        StreakDirection::detect_from_block_access_order(&[
            AccessOrderLowerIsMoreRecent(3),
            AccessOrderLowerIsMoreRecent(1),
            AccessOrderLowerIsMoreRecent(0),
            AccessOrderLowerIsMoreRecent(2),
        ])
    );
}

#[test_log::test(test)]
fn test_find_blocks_to_prefetch_up() {
    let mut prefetcher = Prefetcher::new();
    let total_block_count: u64 = 10;
    prefetcher.add_explicitly_requested_block(0);
    assert_eq!(
        BTreeSet::from([]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(1);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(2);
    assert_eq!(
        BTreeSet::from([3, 4, 5, 6, 7, 8]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(3);
    assert_eq!(
        BTreeSet::from([4, 5, 6, 7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(4);
    assert_eq!(
        BTreeSet::from([5, 6, 7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(5);
    assert_eq!(
        BTreeSet::from([6, 7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(6);
    assert_eq!(
        BTreeSet::from([7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
}

#[test_log::test(test)]
fn test_find_blocks_to_prefetch_down() {
    let mut prefetcher = Prefetcher::new();
    let total_block_count: u64 = 10;
    prefetcher.add_explicitly_requested_block(9);
    assert_eq!(
        BTreeSet::from([]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(8);
    assert_eq!(
        BTreeSet::from([4, 5, 6, 7]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(7);
    assert_eq!(
        BTreeSet::from([1, 2, 3, 4, 5, 6]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(6);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(5);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(4);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(3);
    assert_eq!(
        BTreeSet::from([0, 1, 2]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
}

#[test_log::test(test)]
fn test_find_blocks_to_prefetch_two_streaks() {
    let mut prefetcher = Prefetcher::new();
    let total_block_count: u64 = 20;
    prefetcher.add_explicitly_requested_block(0);
    assert_eq!(
        BTreeSet::from([]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(9);
    assert_eq!(
        BTreeSet::from([]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(1);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(8);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5, 6, 7]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(4);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5, 6, 7]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(5);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5, 6, 7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(7);
    assert_eq!(
        BTreeSet::from([1, 2, 3, 4, 5, 6, 7, 8, 9]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(6);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(3);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(10);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(11);
    assert_eq!(
        BTreeSet::from([0, 1, 2, 3, 4, 5]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
    prefetcher.add_explicitly_requested_block(12);
    assert_eq!(
        BTreeSet::from([2, 3, 4, 5, 13, 14, 15, 16, 17, 18, 19]),
        prefetcher.find_blocks_to_prefetch(total_block_count)
    );
}

#[test_log::test(test)]
fn test_analyze_streak_0() {
    let blocks_to_prefetch = Prefetcher::analyze_streak(1000, &[], 2000);
    let expected = BTreeSet::from([]);
    assert_eq!(expected, blocks_to_prefetch);
}

#[test_log::test(test)]
fn test_analyze_streak_1() {
    let blocks_to_prefetch =
        Prefetcher::analyze_streak(1000, [AccessOrderLowerIsMoreRecent(0)].as_ref(), 2000);
    let expected = BTreeSet::from([]);
    assert_eq!(expected, blocks_to_prefetch);
}

#[test_log::test(test)]
fn test_analyze_streak_2() {
    let blocks_to_prefetch = Prefetcher::analyze_streak(
        1001,
        [1, 0].map(AccessOrderLowerIsMoreRecent).as_ref(),
        2000,
    );
    let expected = BTreeSet::from([1002, 1003, 1004, 1005]);
    assert_eq!(expected, blocks_to_prefetch);
}

#[test_log::test(test)]
fn test_analyze_streak_16() {
    let blocks_to_prefetch = Prefetcher::analyze_streak(
        1015,
        [15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0]
            .map(AccessOrderLowerIsMoreRecent)
            .as_ref(),
        2000,
    );
    let expected = BTreeSet::from([
        1016, 1017, 1018, 1019, 1020, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030,
        1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039,
    ]);
    assert_eq!(expected, blocks_to_prefetch);
}

fn test_clock() -> std::time::SystemTime {
    std::time::SystemTime::UNIX_EPOCH
}

lazy_static! {
    static ref DUMMY_DIGEST: BlobDigest = BlobDigest::new(&[
        104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108, 6, 169, 103, 74,
        159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205, 186, 47, 210, 169, 3, 196, 19, 48,
        211, 86, 202, 96, 177, 113, 146, 195, 171, 48, 102, 23, 244, 236, 205, 2, 38, 202, 233, 41,
        2, 52, 27,
    ]);
}

#[test_log::test(tokio::test)]
async fn test_open_directory_get_meta_data() {
    let modified = test_clock();
    let expected = DirectoryEntryMetaData::new(DirectoryEntryKind::File(12), modified);
    let directory = OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        BTreeMap::from([(
            FileName::try_from("test.txt".to_string()).unwrap(),
            NamedEntry::NotOpen(expected, BlobDigest::hash(&[])),
        )]),
        Arc::new(NeverUsedStorage {}),
        modified,
        test_clock,
        1,
    );
    let meta_data = directory
        .get_meta_data(&FileName::try_from("test.txt".to_string()).unwrap())
        .await
        .unwrap();
    assert_eq!(expected, meta_data);
}

#[test_log::test(tokio::test)]
async fn test_open_directory_nothing_happens() {
    let modified = test_clock();
    let expected = DirectoryEntryMetaData::new(DirectoryEntryKind::File(12), modified);
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let directory = OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        BTreeMap::from([(
            FileName::try_from("test.txt".to_string()).unwrap(),
            NamedEntry::NotOpen(expected, BlobDigest::hash(&[])),
        )]),
        storage.clone(),
        modified,
        test_clock,
        1,
    );
    let mut receiver = directory.watch().await;
    let result =
        tokio::time::timeout(std::time::Duration::from_millis(10), receiver.changed()).await;
    assert_eq!("deadline has elapsed", format!("{}", result.unwrap_err()));
    let status = *receiver.borrow();
    assert_eq!(
        OpenDirectoryStatus::new(
            DigestStatus::new(
                BlobDigest::new(&[
                    104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108, 6,
                    169, 103, 74, 159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205, 186, 47,
                    210, 169, 3, 196, 19, 48, 211, 86, 202, 96, 177, 113, 146, 195, 171, 48, 102,
                    23, 244, 236, 205, 2, 38, 202, 233, 41, 2, 52, 27
                ]),
                false
            ),
            1,
            0,
            OpenFileStats::new(0, 0, 0, 0, 0),
        ),
        status
    );
    assert_eq!(0, storage.number_of_trees().await);
}

#[test_log::test(tokio::test)]
async fn test_open_directory_open_file() {
    let modified = test_clock();
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        test_clock,
        1,
    ));
    let file_name = FileName::try_from("test.txt".to_string()).unwrap();
    let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
    let opened = directory
        .clone()
        .open_file(&file_name, &empty_file_digest)
        .await
        .unwrap();
    opened.flush().await.unwrap();
    assert_eq!(
        DirectoryEntryMetaData::new(DirectoryEntryKind::File(0), modified),
        directory.get_meta_data(&file_name).await.unwrap()
    );
    use futures::StreamExt;
    let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
    assert_eq!(
        &[MutableDirectoryEntry {
            name: file_name,
            kind: DirectoryEntryKind::File(0),
            modified,
        }][..],
        &directory_entries[..]
    );
}

#[test_log::test(tokio::test)]
async fn test_read_directory_after_file_write() {
    let modified = test_clock();
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        test_clock,
        1,
    ));
    let file_name = FileName::try_from("test.txt".to_string()).unwrap();
    let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
    let opened = directory
        .clone()
        .open_file(&file_name, &empty_file_digest)
        .await
        .unwrap();
    let write_permission = opened.get_write_permission();
    let file_content = &b"hello world"[..];
    opened
        .write_bytes(&write_permission, 0, file_content.into())
        .await
        .unwrap();
    use futures::StreamExt;
    let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
    assert_eq!(
        &[MutableDirectoryEntry {
            name: file_name,
            kind: DirectoryEntryKind::File(file_content.len() as u64),
            modified,
        }][..],
        &directory_entries[..]
    );
}

#[test_log::test(tokio::test)]
async fn test_get_meta_data_after_file_write() {
    let modified = test_clock();
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let directory = Arc::new(OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        BTreeMap::new(),
        storage.clone(),
        modified,
        test_clock,
        1,
    ));
    let file_name = FileName::try_from("test.txt".to_string()).unwrap();
    let empty_file_digest = TreeEditor::store_empty_file(storage).await.unwrap();
    let opened = directory
        .clone()
        .open_file(&file_name, &empty_file_digest)
        .await
        .unwrap();
    let write_permission = opened.get_write_permission();
    let file_content = &b"hello world"[..];
    opened
        .write_bytes(&write_permission, 0, file_content.into())
        .await
        .unwrap();
    assert_eq!(
        DirectoryEntryMetaData::new(
            DirectoryEntryKind::File(file_content.len() as u64),
            modified
        ),
        directory.get_meta_data(&file_name).await.unwrap()
    );
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryEntry {
    pub name: FileName,
    pub kind: DirectoryEntryKind,
    pub digest: BlobDigest,
}

fn open_directory_from_entries(
    entries: Vec<DirectoryEntry>,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) -> OpenDirectory {
    let modified = test_clock();
    OpenDirectory::new(
        std::path::PathBuf::from("/"),
        DigestStatus::new(*DUMMY_DIGEST, false),
        entries
            .iter()
            .map(|entry| {
                (entry.name.clone(), {
                    NamedEntry::NotOpen(
                        DirectoryEntryMetaData::new(entry.kind, modified),
                        entry.digest,
                    )
                })
            })
            .collect(),
        storage,
        modified,
        test_clock,
        1,
    )
}

#[test_log::test(tokio::test)]
async fn test_read_empty_root() {
    use futures::StreamExt;
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let mut directory = editor
        .read_directory(NormalizedPath::try_from(relative_path::RelativePath::new("/")).unwrap())
        .await
        .unwrap();
    let end = directory.next().await;
    assert!(end.is_none());
}

#[derive(Debug)]
struct NeverUsedStorage {}

#[async_trait]
impl LoadTree for NeverUsedStorage {
    async fn load_tree(&self, _reference: &astraea::tree::BlobDigest) -> Option<DelayedHashedTree> {
        panic!()
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        panic!()
    }
}

#[async_trait]
impl StoreTree for NeverUsedStorage {
    async fn store_tree(
        &self,
        _tree: &HashedTree,
    ) -> std::result::Result<astraea::tree::BlobDigest, StoreError> {
        panic!()
    }
}

impl LoadStoreTree for NeverUsedStorage {}

#[test_log::test(tokio::test)]
async fn test_get_meta_data_of_root() {
    let modified = test_clock();
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let meta_data = editor
        .get_meta_data(NormalizedPath::try_from(relative_path::RelativePath::new("/")).unwrap())
        .await
        .unwrap();
    assert_eq!(
        DirectoryEntryMetaData::new(DirectoryEntryKind::Directory, modified),
        meta_data
    );
}

#[test_log::test(tokio::test)]
async fn test_get_meta_data_of_non_normalized_path() {
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let error = editor
        .get_meta_data(
            NormalizedPath::try_from(relative_path::RelativePath::new("unknown.txt")).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        Error::NotFound(FileName::try_from("unknown.txt".to_string()).unwrap()),
        error
    );
}

#[test_log::test(tokio::test)]
async fn test_get_meta_data_of_unknown_path() {
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let error = editor
        .get_meta_data(
            NormalizedPath::try_from(relative_path::RelativePath::new("/unknown.txt")).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        Error::NotFound(FileName::try_from("unknown.txt".to_string()).unwrap()),
        error
    );
}

#[test_log::test(tokio::test)]
async fn test_get_meta_data_of_unknown_path_in_unknown_directory() {
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let error = editor
        .get_meta_data(
            NormalizedPath::try_from(relative_path::RelativePath::new("/unknown/file.txt"))
                .unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        Error::NotFound(FileName::try_from("unknown".to_string()).unwrap()),
        error
    );
}

#[test_log::test(tokio::test)]
async fn test_read_directory_on_closed_regular_file() {
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![DirectoryEntry {
                name: FileName::try_from("test.txt".to_string()).unwrap(),
                kind: DirectoryEntryKind::File(4),
                digest: BlobDigest::hash(b"TEST"),
            }],
            Arc::new(NeverUsedStorage {}),
        )),
        None,
    );
    let result = editor
        .read_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test.txt")).unwrap(),
        )
        .await;
    assert_eq!(
        Some(Error::CannotOpenRegularFileAsDirectory(
            FileName::try_from("test.txt".to_string()).unwrap()
        )),
        result.err()
    );
}

#[test_log::test(tokio::test)]
async fn test_read_directory_on_open_regular_file() {
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let editor = TreeEditor::new(
        Arc::new(open_directory_from_entries(
            vec![DirectoryEntry {
                name: FileName::try_from("test.txt".to_string()).unwrap(),
                kind: DirectoryEntryKind::File(0),
                digest: BlobDigest::hash(b""),
            }],
            storage,
        )),
        None,
    );
    let _open_file = editor
        .open_file(NormalizedPath::try_from(relative_path::RelativePath::new("/test.txt")).unwrap())
        .await
        .unwrap();
    let result = editor
        .read_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test.txt")).unwrap(),
        )
        .await;
    assert_eq!(
        Some(Error::CannotOpenRegularFileAsDirectory(
            FileName::try_from("test.txt".to_string()).unwrap()
        )),
        result.err()
    );
}

#[test_log::test(tokio::test)]
async fn test_create_directory() {
    use futures::StreamExt;
    let modified = test_clock();
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let editor = TreeEditor::new(Arc::new(open_directory_from_entries(vec![], storage)), None);
    editor
        .create_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test")).unwrap(),
        )
        .await
        .unwrap();
    let mut reading = editor
        .read_directory(NormalizedPath::try_from(relative_path::RelativePath::new("/")).unwrap())
        .await
        .unwrap();
    let entry: MutableDirectoryEntry = reading.next().await.unwrap();
    assert_eq!(
        MutableDirectoryEntry {
            name: FileName::try_from("test".to_string()).unwrap(),
            kind: DirectoryEntryKind::Directory,
            modified,
        },
        entry
    );
    let end = reading.next().await;
    assert!(end.is_none());
}

#[test_log::test(tokio::test)]
async fn test_read_created_directory() {
    use futures::StreamExt;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let editor = TreeEditor::new(Arc::new(open_directory_from_entries(vec![], storage)), None);
    editor
        .create_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test")).unwrap(),
        )
        .await
        .unwrap();
    let mut reading = editor
        .read_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test")).unwrap(),
        )
        .await
        .unwrap();
    let end = reading.next().await;
    assert!(end.is_none());
}

#[test_log::test(tokio::test)]
async fn test_nested_create_directory() {
    use futures::StreamExt;
    let modified = test_clock();
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let editor = TreeEditor::new(Arc::new(open_directory_from_entries(vec![], storage)), None);
    editor
        .create_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test")).unwrap(),
        )
        .await
        .unwrap();
    editor
        .create_directory(
            NormalizedPath::try_from(relative_path::RelativePath::new("/test/subdir")).unwrap(),
        )
        .await
        .unwrap();
    {
        let mut reading = editor
            .read_directory(
                NormalizedPath::try_from(relative_path::RelativePath::new("/test/subdir")).unwrap(),
            )
            .await
            .unwrap();
        let end = reading.next().await;
        assert!(end.is_none());
    }
    {
        let mut reading = editor
            .read_directory(
                NormalizedPath::try_from(relative_path::RelativePath::new("/test")).unwrap(),
            )
            .await
            .unwrap();
        let entry: MutableDirectoryEntry = reading.next().await.unwrap();
        assert_eq!(
            MutableDirectoryEntry {
                name: FileName::try_from("subdir".to_string()).unwrap(),
                kind: DirectoryEntryKind::Directory,
                modified,
            },
            entry
        );
        let end = reading.next().await;
        assert!(end.is_none());
    }
    {
        let mut reading = editor
            .read_directory(
                NormalizedPath::try_from(relative_path::RelativePath::new("/")).unwrap(),
            )
            .await
            .unwrap();
        let entry: MutableDirectoryEntry = reading.next().await.unwrap();
        assert_eq!(
            MutableDirectoryEntry {
                name: FileName::try_from("test".to_string()).unwrap(),
                kind: DirectoryEntryKind::Directory,
                modified,
            },
            entry
        );
        let end = reading.next().await;
        assert!(end.is_none());
    }
}

#[test_log::test(tokio::test)]
async fn optimized_write_buffer_empty() {
    for write_position in [0, 1, 10, 100, 1000, u64::MAX] {
        let buffer = OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::new()).await;
        assert_eq!(bytes::Bytes::new(), buffer.prefix());
        assert_eq!(Vec::<HashedTree>::new(), *buffer.full_blocks());
        assert_eq!(bytes::Bytes::new(), buffer.suffix());
    }
}

#[test_log::test(tokio::test)]
async fn optimized_write_buffer_prefix_only() {
    for write_position in [
        0,
        1,
        10,
        100,
        1000,
        TREE_BLOB_MAX_LENGTH as u64,
        TREE_BLOB_MAX_LENGTH as u64 - 1,
        TREE_BLOB_MAX_LENGTH as u64 + 1,
        u64::MAX - 1,
    ] {
        let buffer =
            OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::copy_from_slice(b"x"))
                .await;
        assert_eq!(bytes::Bytes::copy_from_slice(b"x"), buffer.prefix());
        assert_eq!(Vec::<HashedTree>::new(), *buffer.full_blocks());
        assert_eq!(bytes::Bytes::new(), buffer.suffix());
    }
}

#[test_log::test(tokio::test)]
async fn optimized_write_buffer_prefix_and_suffix_only() {
    for block_index in [0, 1, 1000] {
        for prefix_length in [1, 1000, TREE_BLOB_MAX_LENGTH as u64 - 1] {
            for suffix_length in [1, 1000, TREE_BLOB_MAX_LENGTH as u64 - 1] {
                let position_in_block: u64 = TREE_BLOB_MAX_LENGTH as u64 - prefix_length;
                let write_position =
                    (block_index * TREE_BLOB_MAX_LENGTH as u64) + position_in_block;
                let prefix =
                    bytes::Bytes::from_iter(std::iter::repeat_n(b'p', prefix_length as usize));
                let suffix =
                    bytes::Bytes::from_iter(std::iter::repeat_n(b's', suffix_length as usize));
                let write_data = bytes::Bytes::from_iter(
                    prefix.clone().into_iter().chain(suffix.clone().into_iter()),
                );
                let buffer = OptimizedWriteBuffer::from_bytes(write_position, write_data).await;
                assert_eq!(prefix, buffer.prefix());
                assert_eq!(Vec::<HashedTree>::new(), *buffer.full_blocks());
                assert_eq!(suffix, buffer.suffix());
            }
        }
    }
}

#[test_matrix(
        [1, 10, 63_999],
        [1, 10, 63_999],
        [1, 2]
    )]
#[test_log::test(tokio::test)]
async fn optimized_write_buffer_full_blocks(
    prefix_length: u64,
    suffix_length: u64,
    full_block_count: usize,
) {
    //TODO: use more interesting content for prefix
    let prefix = bytes::Bytes::from_iter(std::iter::repeat_n(b'p', prefix_length as usize));
    //TODO: use more interesting content for suffix
    let suffix = bytes::Bytes::from_iter(std::iter::repeat_n(b's', suffix_length as usize));
    let position_in_block: u64 = TREE_BLOB_MAX_LENGTH as u64 - prefix_length;
    let write_data = bytes::Bytes::from_iter(
        prefix
            .clone()
            .into_iter()
            //TODO: use more interesting content for full_blocks
            .chain(std::iter::repeat_n(
                b'f',
                full_block_count * TREE_BLOB_MAX_LENGTH,
            ))
            .chain(suffix.clone().into_iter()),
    );
    for block_index in [0, 100] {
        let write_position = (block_index * TREE_BLOB_MAX_LENGTH as u64) + position_in_block;
        let buffer = OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
        assert_eq!(prefix, buffer.prefix());
        assert_eq!(full_block_count, buffer.full_blocks().len());
        assert!(buffer.full_blocks().iter().all(|full_block| {
            full_block
                .tree()
                .blob()
                .as_slice()
                .iter()
                .all(|&byte| byte == b'f')
        }));
        assert_eq!(suffix, buffer.suffix());
    }
}

#[test_log::test(tokio::test)]
async fn open_file_content_buffer_write_fill_zero_block() {
    let data = Vec::new();
    let last_known_digest = calculate_reference(&Tree::new(
        TreeBlob::try_from(bytes::Bytes::copy_from_slice(&data[..])).unwrap(),
        TreeChildren::empty(),
    ));
    let last_known_digest_file_size = data.len();
    let mut buffer = OpenFileContentBuffer::from_data(
        data,
        last_known_digest,
        last_known_digest_file_size as u64,
        1,
    )
    .unwrap();
    let write_position = TREE_BLOB_MAX_LENGTH as u64;
    let write_data = "a";
    let write_buffer =
        OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::from(write_data)).await;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let _write_result: () = buffer
        .write(write_position, write_buffer, storage.clone())
        .await
        .unwrap();
    let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
        size: TREE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
        blocks: vec![
            OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                vec![0; TREE_BLOB_MAX_LENGTH],
            )),
            OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                write_data.as_bytes().to_vec(),
            )),
        ],
        digest: crate::DigestStatus {
            last_known_digest,
            is_digest_up_to_date: false,
        },
        last_known_digest_file_size: last_known_digest_file_size as u64,
        dirty_blocks: VecDeque::from([0, 1]),
        write_buffer_in_blocks: 1,
        prefetcher: Prefetcher::new(),
    });
    assert_eq!(expected_buffer, buffer);
    let expected_digests = BTreeSet::from_iter(
        [concat!(
            "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
            "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
        )]
        .map(BlobDigest::parse_hex_string)
        .map(Option::unwrap),
    );
    assert_eq!(expected_digests, storage.digests().await);
}

fn random_bytes(len: usize, seed: u64) -> Vec<u8> {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(seed);
    (0..len).map(|_| small_rng.random()).collect()
}

#[test_log::test(tokio::test)]
async fn open_file_content_buffer_overwrite_full_block() {
    let original_data = random_bytes(TREE_BLOB_MAX_LENGTH, 123);
    let last_known_digest_file_size = original_data.len();
    let write_data = bytes::Bytes::from(random_bytes(last_known_digest_file_size, 124));
    let write_data_digest = BlobDigest::hash(&write_data);
    assert_eq!(
        &BlobDigest::parse_hex_string(concat!(
            "d22943da0befa7ca73ed859895034da55129eca5381fdc61517707697e6d55b3",
            "d72b239bec3109f98c08cbdba15ab2e9ec40b280f8d34eed785dc5a68d69fa85"
        ))
        .unwrap(),
        &write_data_digest,
    );
    assert_ne!(&original_data[..], &write_data[..]);
    let last_known_digest = calculate_reference(&Tree::new(
        TreeBlob::try_from(bytes::Bytes::copy_from_slice(&original_data)).unwrap(),
        TreeChildren::empty(),
    ));
    assert_eq!(
        &BlobDigest::parse_hex_string(concat!(
            "c0b6004d4fbd33c339eee2c99f92af59b617aae8de8f0b3a213819246f94cca2",
            "b8305673ecbbfcd468d38433dd7c09f6dbc96df150993bb108f6155a78a2b4ac"
        ))
        .unwrap(),
        &last_known_digest,
    );
    let mut buffer = OpenFileContentBuffer::from_data(
        original_data,
        last_known_digest,
        last_known_digest_file_size as u64,
        1,
    )
    .unwrap();
    let write_position = 0_u64;
    let write_buffer = OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let _write_result: () = buffer
        .write(write_position, write_buffer, storage.clone())
        .await
        .unwrap();
    let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
        size: last_known_digest_file_size as u64,
        blocks: vec![OpenFileContentBlock::Loaded(
            crate::LoadedBlock::KnownDigest(HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(write_data.clone()).unwrap(),
                TreeChildren::empty(),
            )))),
        )],
        digest: crate::DigestStatus {
            last_known_digest,
            is_digest_up_to_date: false,
        },
        last_known_digest_file_size: last_known_digest_file_size as u64,
        dirty_blocks: VecDeque::from([0]),
        write_buffer_in_blocks: 1,
        prefetcher: Prefetcher::new(),
    });
    assert_eq!(expected_buffer, buffer);
    let expected_digests = BTreeSet::from([last_known_digest]);
    assert_eq!(expected_digests, storage.digests().await);
}

#[test_case(0)]
#[test_case(1)]
#[test_case(2_000)]
#[test_case(64_000)]
#[test_case(200_000)]
fn open_file_content_buffer_write_zero_bytes(write_position: u64) {
    Runtime::new().unwrap().block_on(async {
        let original_content = random_bytes(TREE_BLOB_MAX_LENGTH, 123);
        let last_known_digest = BlobDigest::hash(&original_content);
        let last_known_digest_file_size = original_content.len();
        let mut buffer = OpenFileContentBuffer::from_data(
            original_content.clone(),
            last_known_digest,
            last_known_digest_file_size as u64,
            1,
        )
        .unwrap();
        let write_data = bytes::Bytes::new();
        let write_buffer =
            OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
        let storage = Arc::new(InMemoryTreeStorage::empty());
        let _write_result: () = buffer
            .write(write_position, write_buffer, storage.clone())
            .await
            .unwrap();
        let expected_size = std::cmp::max(write_position, last_known_digest_file_size as u64);
        assert_eq!(expected_size, buffer.size());
        let zeroes = expected_size as usize - original_content.len();
        let expected_content = bytes::Bytes::from_iter(
            original_content
                .into_iter()
                .chain(std::iter::repeat_n(0u8, zeroes)),
        );
        check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
    });
}

#[test_log::test(tokio::test)]
async fn open_file_content_buffer_store() {
    let data = Vec::new();
    let last_known_digest = calculate_reference(&Tree::new(
        TreeBlob::try_from(bytes::Bytes::copy_from_slice(&data[..])).unwrap(),
        TreeChildren::empty(),
    ));
    let last_known_digest_file_size = data.len();
    let mut buffer = OpenFileContentBuffer::from_data(
        data,
        last_known_digest,
        last_known_digest_file_size as u64,
        1,
    )
    .unwrap();
    let write_position = TREE_BLOB_MAX_LENGTH as u64;
    let write_data = "a";
    let write_buffer =
        OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::from(write_data)).await;
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let _write_result: () = buffer
        .write(write_position, write_buffer, storage.clone())
        .await
        .unwrap();
    buffer.store_all(storage.clone()).await.unwrap();
    let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
        size: TREE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
        blocks: vec![
            OpenFileContentBlock::NotLoaded(
                calculate_reference(&Tree::new(
                    TreeBlob::try_from(bytes::Bytes::from(vec![0; TREE_BLOB_MAX_LENGTH])).unwrap(),
                    TreeChildren::empty(),
                )),
                TREE_BLOB_MAX_LENGTH as u16,
            ),
            OpenFileContentBlock::NotLoaded(
                calculate_reference(&Tree::new(
                    TreeBlob::try_from(bytes::Bytes::copy_from_slice(write_data.as_bytes()))
                        .unwrap(),
                    TreeChildren::empty(),
                )),
                write_data.len() as u16,
            ),
        ],
        digest: crate::DigestStatus {
            last_known_digest: BlobDigest::parse_hex_string(concat!(
                "f770468c4e5b38323c05f83229aadcb680a0c3fed112fffdbb7650bc92f26a7e",
                "e15e77fca5371b75463401b3bc2893c5aa667ff54d2aa4332ea445352697df99"
            ))
            .unwrap(),
            is_digest_up_to_date: true,
        },
        last_known_digest_file_size: TREE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
        dirty_blocks: VecDeque::new(),
        write_buffer_in_blocks: 1,
        prefetcher: Prefetcher::new(),
    });
    assert_eq!(expected_buffer, buffer);

    let expected_digests = BTreeSet::from_iter(
        [
            concat!(
                "713ddcb3450de2b0b98f2e8b69dbb1a5736b10db787eae6274ea4673b467e692",
                "6415c7a14651bdfdaa973eaabbcf1814993bdc991e2891a72df2c7de4f8322c5"
            ),
            concat!(
                "708d4258f26a6d99a6cc10532bd66134f46fd537d51db057c5a083cf8994f07e",
                "740534b6f795c49aa35513a65e3da7a5518fe163da200e24af0701088b290daa"
            ),
            concat!(
                "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf",
                "2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909"
            ),
            concat!(
                "f770468c4e5b38323c05f83229aadcb680a0c3fed112fffdbb7650bc92f26a7e",
                "e15e77fca5371b75463401b3bc2893c5aa667ff54d2aa4332ea445352697df99"
            ),
        ]
        .map(BlobDigest::parse_hex_string)
        .map(Option::unwrap),
    );

    assert_eq!(expected_digests, storage.digests().await);
}

async fn check_open_file_content_buffer(
    buffer: &mut OpenFileContentBuffer,
    expected_content: bytes::Bytes,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
) {
    let mut checked = 0;
    while checked < expected_content.len() {
        let read_result = buffer
            .read(
                checked as u64,
                expected_content.len() - checked,
                storage.clone(),
            )
            .await;
        let read_bytes = read_result.unwrap();
        let expected_piece = expected_content.slice(checked..(checked + read_bytes.len()));
        assert_eq!(expected_piece.len(), read_bytes.len());
        assert!(expected_piece == read_bytes);
        checked += read_bytes.len();
    }
    assert_eq!(expected_content.len(), checked);
}

#[test_case(0)]
#[test_case(1)]
#[test_case(20)]
#[test_case(2_000)]
#[test_case(200_000)]
fn open_file_content_buffer_sizes(size: usize) {
    Runtime::new().unwrap().block_on(async {
        let initial_content = Vec::new();
        let last_known_digest = BlobDigest::hash(&initial_content);
        let last_known_digest_file_size = initial_content.len();
        let mut buffer = OpenFileContentBuffer::from_data(
            initial_content,
            last_known_digest,
            last_known_digest_file_size as u64,
            1,
        )
        .unwrap();
        let new_content = bytes::Bytes::from(random_bytes(size, 123));
        let storage = Arc::new(InMemoryTreeStorage::empty());
        buffer
            .write(
                0,
                OptimizedWriteBuffer::from_bytes(0, new_content.clone()).await,
                storage.clone(),
            )
            .await
            .unwrap();
        check_open_file_content_buffer(&mut buffer, new_content, storage).await;
    });
}

#[test_case(1)]
#[test_case(2_000)]
#[test_case(63_999)]
fn open_file_content_buffer_write_completes_a_block(write_position: u16) {
    Runtime::new().unwrap().block_on(async {
        let original_content = random_bytes(write_position as usize, 123);
        let last_known_digest = BlobDigest::hash(&original_content);
        let last_known_digest_file_size = original_content.len();
        let mut buffer = OpenFileContentBuffer::from_data(
            original_content.clone(),
            last_known_digest,
            last_known_digest_file_size as u64,
            1,
        )
        .unwrap();
        let write_size = TREE_BLOB_MAX_LENGTH - write_position as usize;
        let write_data = bytes::Bytes::from(random_bytes(write_size, 123));
        let write_buffer =
            OptimizedWriteBuffer::from_bytes(write_position as u64, write_data.clone()).await;
        assert_eq!(write_size, write_buffer.prefix().len());
        let storage = Arc::new(InMemoryTreeStorage::empty());
        let _write_result: () = buffer
            .write(write_position as u64, write_buffer, storage.clone())
            .await
            .unwrap();
        let expected_size = TREE_BLOB_MAX_LENGTH as u64;
        assert_eq!(expected_size, buffer.size());
        let expected_content = bytes::Bytes::from_iter(
            original_content
                .into_iter()
                .chain(write_data.iter().copied()),
        );
        check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
    });
}

#[test_case(1)]
#[test_case(2_000)]
#[test_case(63_999)]
fn open_file_content_buffer_write_creates_full_block_with_zero_fill(write_position: u16) {
    Runtime::new().unwrap().block_on(async {
        let original_content: Vec<u8> = std::iter::repeat_n(1u8, TREE_BLOB_MAX_LENGTH).collect();
        let last_known_digest = BlobDigest::hash(&original_content);
        let last_known_digest_file_size = original_content.len();
        let mut buffer = OpenFileContentBuffer::from_data(
            original_content.clone(),
            last_known_digest,
            last_known_digest_file_size as u64,
            1,
        )
        .unwrap();
        let write_size = TREE_BLOB_MAX_LENGTH - write_position as usize;
        let write_data = bytes::Bytes::from(random_bytes(write_size, 123));
        let write_buffer =
            OptimizedWriteBuffer::from_bytes(write_position as u64, write_data.clone()).await;
        assert_eq!(write_size, write_buffer.prefix().len());
        let storage = Arc::new(InMemoryTreeStorage::empty());
        let _write_result: () = buffer
            .write(
                original_content.len() as u64 + write_position as u64,
                write_buffer,
                storage.clone(),
            )
            .await
            .unwrap();
        let expected_size = original_content.len() as u64 + TREE_BLOB_MAX_LENGTH as u64;
        assert_eq!(expected_size, buffer.size());
        let expected_content = bytes::Bytes::from_iter(
            original_content
                .iter()
                .copied()
                .chain(std::iter::repeat_n(0u8, write_position as usize))
                .chain(write_data.iter().copied()),
        );
        check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
    });
}
