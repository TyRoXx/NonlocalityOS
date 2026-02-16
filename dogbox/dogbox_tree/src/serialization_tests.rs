use crate::serialization::{
    deserialize_directory, serialize_directory, DirectoryEntryKind, DirectoryEntryMetaData,
    FileName, FileNameContent, FileNameError,
};
use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::{StoreTree, StrongReference},
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_MAX_CHILDREN},
};
use pretty_assertions::assert_eq;
use std::{collections::BTreeMap, sync::Arc};

#[test_log::test]
fn test_file_name_content_from() {
    assert_eq!(
        Err(FileNameError::Empty),
        FileNameContent::from("".to_string())
    );
    assert_eq!(
        String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        ))
        .as_str(),
        FileNameContent::from(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        )))
        .unwrap()
        .as_str()
    );
    assert_eq!(
        Err(FileNameError::TooLong),
        FileNameContent::from(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES + 1
        )))
    );
    assert_eq!(
        Err(FileNameError::Null),
        FileNameContent::from("\0".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x01".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x1e".to_string())
    );
    assert_eq!(
        Err(FileNameError::AsciiControlCharacter),
        FileNameContent::from("\x1f".to_string())
    );
    assert_eq!(
        " ",
        FileNameContent::from("\x20".to_string()).unwrap().as_str()
    );
    assert_eq!(
        Err(FileNameError::WindowsSpecialCharacter),
        FileNameContent::from("<".to_string())
    );
    assert_eq!(
        Err(FileNameError::WindowsSpecialCharacter),
        FileNameContent::from("*".to_string())
    );
    assert_eq!(
        " ",
        FileNameContent::from(" ".to_string()).unwrap().as_str()
    );
    assert_eq!(
        "a",
        FileNameContent::from("a".to_string()).unwrap().as_str()
    );
    assert_eq!(
        "aaaaaaaaaaaaaaaaaaaaaaa",
        FileNameContent::from("aaaaaaaaaaaaaaaaaaaaaaa".to_string())
            .unwrap()
            .as_str()
    );
}

#[test_log::test(tokio::test)]
async fn test_serialize_directory_empty() {
    let storage = InMemoryTreeStorage::empty();
    let reference = serialize_directory(&BTreeMap::from([]), &storage)
        .await
        .unwrap();
    assert_eq!(1, storage.number_of_trees().await);
    assert_eq!(
        &BlobDigest::parse_hex_string(concat!(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a27",
            "01a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ))
        .unwrap(),
        reference.digest()
    );
}

#[test_log::test(tokio::test)]
async fn test_deserialize_directory() {
    let storage = InMemoryTreeStorage::empty();
    // Directories can have more than TREE_MAX_CHILDREN entries now.
    let number_of_entries = TREE_MAX_CHILDREN as u32 + 10;
    let mut file_contents: Vec<(StrongReference, usize)> = Vec::new();
    for i in 0..number_of_entries {
        let content = bytes::Bytes::from_owner(i.to_be_bytes());
        let size = content.len();
        let reference = storage
            .store_tree(&HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(content).unwrap(),
                TreeChildren::empty(),
            ))))
            .await
            .unwrap();
        file_contents.push((reference, size));
    }
    let original: BTreeMap<FileName, (DirectoryEntryMetaData, StrongReference)> = file_contents
        .into_iter()
        .enumerate()
        .map(|(i, (reference, size))| {
            (FileName::try_from(format!("{}", i)).unwrap(), {
                let modified = std::time::SystemTime::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(i as u64))
                    .unwrap();
                (
                    DirectoryEntryMetaData::new(
                        if i.is_multiple_of(3) {
                            DirectoryEntryKind::Directory
                        } else {
                            DirectoryEntryKind::File(size as u64)
                        },
                        modified,
                    ),
                    reference,
                )
            })
        })
        .collect();
    let reference = serialize_directory(&original, &storage).await.unwrap();
    assert_eq!(1019, storage.number_of_trees().await);
    assert_eq!(
        &BlobDigest::parse_hex_string(
            "61c5287b0b70a5c38501873bdc5006e90f7af87293a66b40e1006bb2025baed4efec75cae71dc3cb13b3f1d1cf512be447ea805652fedeacc8e2ad5d4cff8b8c"
        )
        .unwrap(),
        reference.digest()
    );
    let deserialized = deserialize_directory(&storage, reference.digest())
        .await
        .unwrap();
    assert_eq!(original, deserialized);
}
