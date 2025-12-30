use astraea::{storage::LoadStoreTree, tree::BlobDigest};
use serde::{Deserialize, Serialize};
use sorted_tree::prolly_tree_editable_node::{self, Iterator};
use std::collections::BTreeMap;
use tracing::{info, instrument};

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

impl FileName {
    pub fn as_str(&self) -> &str {
        self.content.as_str()
    }
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

impl std::fmt::Display for FileName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.content.as_str())
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq)]
pub enum DirectoryEntryKind {
    Directory,
    /// the size is duplicated here so that you can enumerate directories and get the file sizes without having to access every file's blob
    File(u64),
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub kind: DirectoryEntryKind,
    pub child: sorted_tree::sorted_tree::TreeReference,
}

impl sorted_tree::sorted_tree::NodeValue for DirectoryEntry {
    type Content = DirectoryEntryKind;

    fn has_child(_content: &Self::Content) -> bool {
        // Each directory entry points to either a file or a subdirectory. Both are represented by a child reference.
        true
    }

    fn from_content(content: Self::Content, child: &Option<BlobDigest>) -> Self {
        match child {
            Some(reference) => DirectoryEntry {
                kind: content,
                child: sorted_tree::sorted_tree::TreeReference::new(*reference),
            },
            None => unreachable!("DirectoryEntry must have a child reference"),
        }
    }

    fn to_content(&self) -> Self::Content {
        self.kind
    }

    fn get_reference(&self) -> Option<BlobDigest> {
        Some(*self.child.reference())
    }
}

impl DirectoryEntry {
    pub fn new(
        kind: DirectoryEntryKind,
        content: sorted_tree::sorted_tree::TreeReference,
    ) -> DirectoryEntry {
        DirectoryEntry {
            kind,
            child: content,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DeserializationError {
    MissingTree(BlobDigest),
    Postcard(postcard::Error),
    ReferenceIndexOutOfRange,
    Inconsistency(String),
}

impl std::fmt::Display for DeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for DeserializationError {}

type ProllyTree = prolly_tree_editable_node::EditableNode<FileName, DirectoryEntry>;

#[instrument(skip_all)]
pub async fn serialize_directory(
    entries: &BTreeMap<FileName, (DirectoryEntryKind, BlobDigest)>,
    storage: &(dyn LoadStoreTree + Send + Sync),
) -> std::result::Result<BlobDigest, Box<dyn std::error::Error>> {
    let mut prolly_tree = ProllyTree::new();
    for (name, (kind, digest)) in entries.iter() {
        prolly_tree
            .insert(
                name.clone(),
                DirectoryEntry::new(*kind, sorted_tree::sorted_tree::TreeReference::new(*digest)),
                storage,
            )
            .await?;
    }
    info!("Serializing directory with {} entries", entries.len());
    prolly_tree.save(storage).await
}

#[instrument(skip_all)]
pub async fn deserialize_directory(
    storage: &(dyn LoadStoreTree + Send + Sync),
    digest: &BlobDigest,
) -> Result<BTreeMap<FileName, (DirectoryEntryKind, BlobDigest)>, Box<dyn std::error::Error>> {
    let mut prolly_tree = ProllyTree::load(digest, storage).await?;
    let mut result = BTreeMap::new();
    let mut iterator = Iterator::new(&mut prolly_tree, storage);
    while let Some((name, entry)) = iterator.next().await? {
        result.insert(name, (entry.kind, *entry.child.reference()));
    }
    info!("Deserialized directory with {} entries", result.len());
    Ok(result)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentedBlob {
    // redundant size info to detect inconsistencies
    pub size_in_bytes: u64,
}
