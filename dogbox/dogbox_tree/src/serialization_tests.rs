use crate::serialization::{
    deserialize_directory, serialize_directory, DirectoryEntryKind, DirectoryEntryMetaData,
    FileName, FileNameContent, FileNameError,
};
use astraea::tree::{BlobDigest, TREE_MAX_CHILDREN};
use pretty_assertions::assert_eq;
use std::collections::BTreeMap;
use tokio::sync::Mutex;

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
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    let digest = serialize_directory(&BTreeMap::from([]), &storage)
        .await
        .unwrap();
    assert_eq!(1, storage.number_of_trees().await);
    assert_eq!(
        BlobDigest::parse_hex_string(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a2701a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        )
        .unwrap(),
        digest
    );
}

#[test_log::test(tokio::test)]
async fn test_deserialize_directory() {
    let storage = astraea::storage::InMemoryTreeStorage::new(Mutex::new(BTreeMap::new()));
    // Directories can have more than TREE_MAX_CHILDREN entries now.
    let number_of_entries = TREE_MAX_CHILDREN as u32 + 10;
    let original = (0..number_of_entries)
        .map(|i: u32| {
            (FileName::try_from(format!("{}", i)).unwrap(), {
                let content = i.to_be_bytes();
                let digest = BlobDigest::hash(&content);
                let modified = std::time::SystemTime::UNIX_EPOCH
                    .checked_add(std::time::Duration::from_secs(i as u64))
                    .unwrap();
                (
                    DirectoryEntryMetaData::new(
                        if i.is_multiple_of(3) {
                            DirectoryEntryKind::Directory
                        } else {
                            DirectoryEntryKind::File(content.len() as u64)
                        },
                        modified,
                    ),
                    digest,
                )
            })
        })
        .collect();
    let digest = serialize_directory(&original, &storage).await.unwrap();
    assert_eq!(9, storage.number_of_trees().await);
    assert_eq!(
        BlobDigest::parse_hex_string(
            "e5abae670a46da24d421474487f82466bcd0f7f097282a9a2b6804577c4827164035c713e11ad64732d350222a0b17088d8cfbee6aeadc2e6a1516488a34b66c"
        )
        .unwrap(),
        digest
    );
    let deserialized = deserialize_directory(&storage, &digest).await.unwrap();
    assert_eq!(original, deserialized);
}
