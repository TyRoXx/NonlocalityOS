use crate::serialization::FileName;
use async_trait::async_trait;
use std::pin::Pin;

pub trait AsyncReadBlob: tokio::io::AsyncSeek + tokio::io::AsyncRead {}

#[async_trait]
pub trait ReadFile {
    fn open(&self) -> Result<Box<dyn AsyncReadBlob>>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub enum DirectoryEntryInfo {
    Directory,
    File(u64),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct DirectoryEntry {
    pub name: FileName,
    pub info: DirectoryEntryInfo,
}

impl DirectoryEntry {
    pub fn new(name: FileName, info: DirectoryEntryInfo) -> DirectoryEntry {
        DirectoryEntry {
            name: name,
            info: info,
        }
    }
}

pub enum EntryAccessor {
    Directory(Box<dyn ReadDirectory>),
    File(Box<dyn ReadFile>),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Error {
    Unknown,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Stream<'t, T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send + 't>>;

#[async_trait]
pub trait ReadDirectory {
    async fn enumerate<'t>(&'t self) -> Stream<'t, DirectoryEntry>;
    async fn access_entry(&self, name: &FileName) -> Option<EntryAccessor>;
}
