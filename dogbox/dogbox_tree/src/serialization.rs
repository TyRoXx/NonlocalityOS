use astraea::tree::ReferenceIndex;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(try_from = "String")]
pub struct FileNameContent(String);

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
    /// > : (colon - sometimes works, but is actually NTFS Alternate Data Streams)
    /// > " (double quote)
    /// > / (forward slash)
    /// > \ (backslash)
    /// > | (vertical bar or pipe)
    /// > ? (question mark)
    /// * (asterisk)
    WindowsSpecialCharacter,
}

impl std::fmt::Display for FileNameError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FileNameContent {
    pub const MAX_LENGTH_IN_BYTES: usize = 4096;

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn from(content: String) -> std::result::Result<FileNameContent, FileNameError> {
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
        Ok(FileNameContent(content))
    }
}

impl TryFrom<String> for FileNameContent {
    type Error = FileNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        FileNameContent::from(value)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct FileName {
    content: FileNameContent,
}

impl TryFrom<String> for FileName {
    type Error = FileNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        FileNameContent::try_from(value).map(|content| FileName { content })
    }
}

impl TryFrom<&str> for FileName {
    type Error = FileNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        FileName::try_from(value.to_string())
    }
}

impl Into<String> for FileName {
    fn into(self) -> String {
        self.content.0
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum DirectoryEntryKind {
    Directory,
    /// the size is duplicated here so that you can enumerate directories and get the file sizes without having to access every file's blob
    File(u64),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum ReferenceIndexOrInlineContent {
    Indirect(ReferenceIndex),
    Direct(Vec<u8>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectoryEntry {
    pub kind: DirectoryEntryKind,
    pub content: ReferenceIndexOrInlineContent,
}

impl DirectoryEntry {
    pub fn new(kind: DirectoryEntryKind, content: ReferenceIndexOrInlineContent) -> DirectoryEntry {
        DirectoryEntry { kind, content }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectoryTree {
    pub children: std::collections::BTreeMap<FileName, DirectoryEntry>,
}

impl DirectoryTree {
    pub fn new(children: std::collections::BTreeMap<FileName, DirectoryEntry>) -> DirectoryTree {
        DirectoryTree { children }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentedBlob {
    // redundant size info to detect inconsistencies
    pub size_in_bytes: u64,
}
