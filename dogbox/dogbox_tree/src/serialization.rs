use std::{collections::BTreeMap, sync::Arc};

use astraea::{
    storage::LoadStoreTree,
    tree::{BlobDigest, ReferenceIndex},
};
use serde::{Deserialize, Serialize};
use tracing::debug;

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
#[serde(try_from = "String")]
pub struct FileNameContent(String);

/// forbidden characters on Linux and Windows according to https://stackoverflow.com/a/31976060
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FileNameError {
    /// empty file names make no sense
    Empty,
    /// overly long files names are not supported
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
        write!(f, "{self:?}")
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
        if content.len() > FileNameContent::MAX_LENGTH_IN_BYTES {
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

impl From<FileName> for String {
    fn from(val: FileName) -> Self {
        val.content.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DeserializationError {
    MissingTree(BlobDigest),
    Postcard(postcard::Error),
    ReferenceIndexOutOfRange,
}

impl std::fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub async fn deserialize_directory(
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
    digest: &BlobDigest,
) -> Result<BTreeMap<String, (DirectoryEntryKind, BlobDigest)>, DeserializationError> {
    let delayed_loaded = match storage.load_tree(digest).await {
        Some(delayed_loaded) => delayed_loaded,
        None => return Err(DeserializationError::MissingTree(*digest)),
    };
    let loaded = delayed_loaded.hash().unwrap(/*TODO*/);
    let parsed_directory: DirectoryTree =
        match postcard::from_bytes(loaded.tree().blob().as_slice()) {
            Ok(success) => success,
            Err(error) => return Err(DeserializationError::Postcard(error)),
        };
    debug!(
        "Loading directory with {} entries",
        parsed_directory.children.len()
    );
    let mut result = BTreeMap::new();
    for child in parsed_directory.children {
        match &child.1.content {
            ReferenceIndexOrInlineContent::Indirect(reference_index) => {
                let index: usize = usize::try_from(reference_index.0)
                    .map_err(|_error| DeserializationError::ReferenceIndexOutOfRange)?;
                if index >= loaded.tree().references().len() {
                    return Err(DeserializationError::ReferenceIndexOutOfRange);
                }
                let digest = loaded.tree().references()[index];
                result.insert(child.0.clone().into(), (child.1.kind, digest));
            }
            ReferenceIndexOrInlineContent::Direct(_vec) => todo!(),
        }
    }
    Ok(result)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentedBlob {
    // redundant size info to detect inconsistencies
    pub size_in_bytes: u64,
}
