#![deny(warnings)]
use dogbox_blob_layer::BlobDigest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(try_from = "String")]
struct FileNameContent(String);

/// forbidden characters on Linux and Windows according to https://stackoverflow.com/a/31976060
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FileNameError {
    /// empty file names make no sense
    Empty,
    ///
    TooLong,
    /// NULL byte (Linux)
    Null,
    /// ASCII control characters 1-31 (Windows)
    AsciiControlCharacter,
    /// < (less than)
    /// > (greater than)
    /// : (colon - sometimes works, but is actually NTFS Alternate Data Streams)
    /// " (double quote)
    /// / (forward slash)
    /// \ (backslash)
    /// | (vertical bar or pipe)
    /// ? (question mark)
    /// * (asterisk)
    WindowsSpecialCharacter,
}

impl std::fmt::Display for FileNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FileNameContent {
    const MAX_LENGTH_IN_BYTES: usize = 4096;

    fn from(content: String) -> std::result::Result<FileNameContent, FileNameError> {
        if content.is_empty() {
            return Err(FileNameError::Empty);
        }
        if content.as_bytes().len() > FileNameContent::MAX_LENGTH_IN_BYTES {
            return Err(FileNameError::TooLong);
        }
        for character in content.bytes() {
            match character {
                0 => return Err(FileNameError::Null),
                1..=31 => return Err(FileNameError::AsciiControlCharacter),
                b'<' | b'>' | b':' | b'"' | b'/' | b'\\' | b'|' | b'?' | b'*' => {
                    return Err(FileNameError::WindowsSpecialCharacter)
                }
                _ => { /* anything else is ok */ }
            }
        }
        return Ok(FileNameContent(content));
    }
}

#[test]
fn test_file_name_content_from() {
    assert_eq!(
        Err(FileNameError::Empty),
        FileNameContent::from("".to_string())
    );
    assert_eq!(
        Ok(FileNameContent(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        )))),
        FileNameContent::from(String::from_iter(std::iter::repeat_n(
            'a',
            FileNameContent::MAX_LENGTH_IN_BYTES
        )))
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
        Ok(FileNameContent(" ".to_string())),
        FileNameContent::from("\x20".to_string())
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
        Ok(FileNameContent(" ".to_string())),
        FileNameContent::from(" ".to_string())
    );
    assert_eq!(
        Ok(FileNameContent("a".to_string())),
        FileNameContent::from("a".to_string())
    );
    assert_eq!(
        Ok(FileNameContent("aaaaaaaaaaaaaaaaaaaaaaa".to_string())),
        FileNameContent::from("aaaaaaaaaaaaaaaaaaaaaaa".to_string())
    );
}

impl TryFrom<String> for FileNameContent {
    type Error = FileNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        FileNameContent::from(value)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileName {
    content: FileNameContent,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DirectoryEntryKind {
    Directory,
    File,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectoryEntry {
    pub kind: DirectoryEntryKind,
    pub digest: BlobDigest,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectoryTree {
    pub children: std::collections::BTreeMap<FileName, DirectoryEntry>,
}
