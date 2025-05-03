use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_512};
use std::{fmt::Display, sync::Arc};

/// SHA3-512 hash. Supports Serde because we will need this type a lot in network protocols and file formats.
#[derive(Serialize, Deserialize, PartialEq, PartialOrd, Ord, Eq, Clone, Copy, Hash)]
pub struct BlobDigest(
    /// data is split into two parts because Serde doesn't support 64-element arrays
    pub ([u8; 32], [u8; 32]),
);

impl BlobDigest {
    pub fn new(value: &[u8; 64]) -> BlobDigest {
        let (first, second) = value.split_at(32);
        BlobDigest((first.try_into().unwrap(), second.try_into().unwrap()))
    }

    pub fn parse_hex_string(input: &str) -> Option<BlobDigest> {
        let mut result = [0u8; 64];
        hex::decode_to_slice(input, &mut result).ok()?;
        Some(BlobDigest::new(&result))
    }

    pub fn hash(input: &[u8]) -> BlobDigest {
        let mut hasher = Sha3_512::new();
        hasher.update(input);
        let result = hasher.finalize().into();
        BlobDigest::new(&result)
    }
}

impl std::fmt::Debug for BlobDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BlobDigest")
            .field(&format!("{}", self))
            .finish()
    }
}

impl std::fmt::Display for BlobDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            &hex::encode(&self.0 .0),
            &hex::encode(&self.0 .1)
        )
    }
}

impl std::convert::From<BlobDigest> for [u8; 64] {
    fn from(val: BlobDigest) -> Self {
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(&val.0 .0);
        result[32..].copy_from_slice(&val.0 .1);
        result
    }
}

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Debug, Copy, Serialize, Deserialize)]
pub struct ReferenceIndex(pub u64);

impl Display for ReferenceIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub const VALUE_BLOB_MAX_LENGTH: usize = 64_000;

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct TreeBlob {
    pub content: bytes::Bytes,
}

impl TreeBlob {
    pub fn empty() -> TreeBlob {
        Self {
            content: bytes::Bytes::new(),
        }
    }

    pub fn try_from(content: bytes::Bytes) -> Option<TreeBlob> {
        if content.len() > VALUE_BLOB_MAX_LENGTH {
            return None;
        }
        Some(Self { content: content })
    }

    pub fn as_slice<'t>(&'t self) -> &'t [u8] {
        assert!(self.content.len() <= VALUE_BLOB_MAX_LENGTH);
        &self.content
    }

    pub fn len(&self) -> u16 {
        assert!(self.content.len() <= VALUE_BLOB_MAX_LENGTH);
        self.content.len() as u16
    }
}

impl std::fmt::Debug for TreeBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueBlob")
            .field("content.len()", &self.content.len())
            .finish()
    }
}

#[derive(Debug)]
pub enum TreeSerializationError {
    Postcard(postcard::Error),
    BlobTooLong,
}

impl std::fmt::Display for TreeSerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for TreeSerializationError {}

#[derive(Debug)]
pub enum TreeDeserializationError {
    ReferencesNotAllowed,
    Postcard(postcard::Error),
    BlobUnavailable(BlobDigest),
}

impl std::fmt::Display for TreeDeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for TreeDeserializationError {}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct Tree {
    pub blob: TreeBlob,
    pub references: Vec<BlobDigest>,
}

impl Tree {
    pub fn new(blob: TreeBlob, references: Vec<BlobDigest>) -> Tree {
        Tree {
            blob,
            references: references,
        }
    }

    pub fn blob(&self) -> &TreeBlob {
        &self.blob
    }

    pub fn references(&self) -> &[BlobDigest] {
        &self.references
    }

    pub fn from_string(value: &str) -> Option<Tree> {
        TreeBlob::try_from(bytes::Bytes::copy_from_slice(value.as_bytes())).map(|blob| Tree {
            blob,
            references: Vec::new(),
        })
    }

    pub fn empty() -> Tree {
        Tree {
            blob: TreeBlob::empty(),
            references: Vec::new(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct HashedValue {
    value: Arc<Tree>,
    digest: BlobDigest,
}

impl HashedValue {
    pub fn from(value: Arc<Tree>) -> HashedValue {
        let digest = calculate_reference(&value);
        Self { value, digest }
    }

    pub fn value(&self) -> &Arc<Tree> {
        &self.value
    }

    pub fn digest(&self) -> &BlobDigest {
        &self.digest
    }
}

impl Display for HashedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.digest)
    }
}

pub fn calculate_digest_fixed<D>(referenced: &Tree) -> sha3::digest::Output<D>
where
    D: sha3::Digest,
{
    let mut hasher = D::new();
    hasher.update(&(referenced.blob.len() as u64).to_be_bytes());
    hasher.update(referenced.blob.as_slice());
    hasher.update(&(referenced.references.len() as u64).to_be_bytes());
    for item in &referenced.references {
        hasher.update(&item.0 .0);
        hasher.update(&item.0 .1);
    }
    hasher.finalize()
}

pub fn calculate_digest_extendable<D>(
    referenced: &Tree,
) -> <D as sha3::digest::ExtendableOutput>::Reader
where
    D: core::default::Default + sha3::digest::Update + sha3::digest::ExtendableOutput,
{
    let mut hasher = D::default();
    hasher.update(&(referenced.blob.len() as u64).to_be_bytes());
    hasher.update(referenced.blob.as_slice());
    hasher.update(&(referenced.references.len() as u64).to_be_bytes());
    for item in &referenced.references {
        hasher.update(&item.0 .0);
        hasher.update(&item.0 .1);
    }
    hasher.finalize_xof()
}

pub fn calculate_reference(referenced: &Tree) -> BlobDigest {
    let result: [u8; 64] = calculate_digest_fixed::<sha3::Sha3_512>(referenced).into();
    BlobDigest::new(&result)
}
