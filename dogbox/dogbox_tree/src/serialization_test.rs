use crate::serialization::{serialize_directory, FileNameContent, FileNameError};
use astraea::tree::BlobDigest;
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
    let result = serialize_directory(&BTreeMap::from([]), &storage)
        .await
        .unwrap();
    assert_eq!(1, storage.number_of_trees().await);
    assert_eq!(
        BlobDigest::parse_hex_string(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a2701a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        )
        .unwrap(),
        result
    );
}
