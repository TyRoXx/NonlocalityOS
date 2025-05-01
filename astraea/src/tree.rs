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

#[test]
fn blob_digest_parse_hex_string() {
    let correct_input = "98b682d4ed7cae2d71b52b0548f37eb5e1243077b4bf5cc43dd7c0dfe50ef462a41d0d70ec41abdd31ef4a2bce79d29b9bafee45ffde2154a61590932c9c92d7";
    assert_eq!(None, BlobDigest::parse_hex_string(""));
    let too_short = correct_input.split_at(correct_input.len() - 1).0;
    assert_eq!(None, BlobDigest::parse_hex_string(too_short));
    let too_long = format!("{}0", correct_input);
    assert_eq!(None, BlobDigest::parse_hex_string(&too_long));
    assert_eq!(
        Some(BlobDigest::new(&[
            152, 182, 130, 212, 237, 124, 174, 45, 113, 181, 43, 5, 72, 243, 126, 181, 225, 36, 48,
            119, 180, 191, 92, 196, 61, 215, 192, 223, 229, 14, 244, 98, 164, 29, 13, 112, 236, 65,
            171, 221, 49, 239, 74, 43, 206, 121, 210, 155, 155, 175, 238, 69, 255, 222, 33, 84,
            166, 21, 144, 147, 44, 156, 146, 215
        ])),
        BlobDigest::parse_hex_string(correct_input)
    );
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

#[test]
fn test_display_reference_index() {
    let index = ReferenceIndex(123);
    assert_eq!(format!("{}", index), "123");
}

pub const VALUE_BLOB_MAX_LENGTH: usize = 64_000;

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct ValueBlob {
    pub content: bytes::Bytes,
}

impl ValueBlob {
    pub fn empty() -> ValueBlob {
        Self {
            content: bytes::Bytes::new(),
        }
    }

    pub fn try_from(content: bytes::Bytes) -> Option<ValueBlob> {
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

impl std::fmt::Debug for ValueBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueBlob")
            .field("content.len()", &self.content.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::tree::{ValueBlob, VALUE_BLOB_MAX_LENGTH};
    use proptest::proptest;

    #[test]
    fn test_debug_value_blob() {
        let blob = ValueBlob::empty();
        assert_eq!(format!("{:?}", blob), "ValueBlob { content.len(): 0 }");
    }

    proptest! {
        #[test]
        fn value_blob_try_from_success(length in 0usize..VALUE_BLOB_MAX_LENGTH) {
            let content = bytes::Bytes::from_iter(std::iter::repeat_n(0u8, length));
            let value_blob = ValueBlob::try_from(content.clone()).unwrap();
            assert_eq!(content, value_blob.content);
        }

        #[test]
        fn value_blob_try_from_failure(length in (VALUE_BLOB_MAX_LENGTH + 1)..(VALUE_BLOB_MAX_LENGTH * 3) /*We don't want to allocate too much memory here.*/) {
            let content = bytes::Bytes::from_iter(std::iter::repeat_n(0u8, length));
            let result = ValueBlob::try_from(content.clone());
            assert_eq!(None, result);
        }
    }
}

#[derive(Debug)]
pub enum ValueSerializationError {
    Postcard(postcard::Error),
    BlobTooLong,
}

impl std::fmt::Display for ValueSerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[test]
fn test_display_value_serialization_error() {
    let error = ValueSerializationError::BlobTooLong;
    assert_eq!(format!("{}", error), "BlobTooLong");
}

impl std::error::Error for ValueSerializationError {}

#[derive(Debug)]
pub enum ValueDeserializationError {
    ReferencesNotAllowed,
    Postcard(postcard::Error),
    BlobUnavailable(BlobDigest),
}

impl std::fmt::Display for ValueDeserializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[test]
fn test_display_value_deserialization_error() {
    assert_eq!(
        format!("{}", ValueDeserializationError::ReferencesNotAllowed),
        "ReferencesNotAllowed"
    );
    assert_eq!(
        format!(
            "{}",
            ValueDeserializationError::Postcard(postcard::Error::DeserializeUnexpectedEnd)
        ),
        "Postcard(DeserializeUnexpectedEnd)"
    );
    assert_eq!(
        format!(
            "{}",
            ValueDeserializationError::BlobUnavailable(BlobDigest::new(&[0u8; 64]),)
        ),
        "BlobUnavailable(BlobDigest(\"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"))"
    );
}

impl std::error::Error for ValueDeserializationError {}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct Value {
    pub blob: ValueBlob,
    pub references: Vec<BlobDigest>,
}

impl Value {
    pub fn new(blob: ValueBlob, references: Vec<BlobDigest>) -> Value {
        Value {
            blob,
            references: references,
        }
    }

    pub fn blob(&self) -> &ValueBlob {
        &self.blob
    }

    pub fn references(&self) -> &[BlobDigest] {
        &self.references
    }

    pub fn from_string(value: &str) -> Option<Value> {
        ValueBlob::try_from(bytes::Bytes::copy_from_slice(value.as_bytes())).map(|blob| Value {
            blob,
            references: Vec::new(),
        })
    }

    pub fn empty() -> Value {
        Value {
            blob: ValueBlob::empty(),
            references: Vec::new(),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub struct HashedValue {
    value: Arc<Value>,
    digest: BlobDigest,
}

impl HashedValue {
    pub fn from(value: Arc<Value>) -> HashedValue {
        let digest = calculate_reference(&value);
        Self { value, digest }
    }

    pub fn value(&self) -> &Arc<Value> {
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

#[test]
fn test_display_hashed_value() {
    let value = Arc::new(Value::empty());
    let hashed_value = HashedValue::from(value.clone());
    assert_eq!(
        format!("{}", hashed_value),
        format!("{}", hashed_value.digest)
    );
}

// TypeId doesn't exist anymore, but we still have them in the digest for backwards compatibility.
// TODO: remove it to make hashing slightly faster
const DEPRECATED_TYPE_ID_IN_DIGEST: u64 = 0;

pub fn calculate_digest_fixed<D>(referenced: &Value) -> sha3::digest::Output<D>
where
    D: sha3::Digest,
{
    let mut hasher = D::new();
    hasher.update(referenced.blob.as_slice());
    for item in &referenced.references {
        hasher.update(&DEPRECATED_TYPE_ID_IN_DIGEST.to_be_bytes());
        hasher.update(&item.0 .0);
        hasher.update(&item.0 .1);
    }
    hasher.finalize()
}

pub fn calculate_digest_extendable<D>(
    referenced: &Value,
) -> <D as sha3::digest::ExtendableOutput>::Reader
where
    D: core::default::Default + sha3::digest::Update + sha3::digest::ExtendableOutput,
{
    let mut hasher = D::default();
    hasher.update(referenced.blob.as_slice());
    for item in &referenced.references {
        hasher.update(&DEPRECATED_TYPE_ID_IN_DIGEST.to_be_bytes());
        hasher.update(&item.0 .0);
        hasher.update(&item.0 .1);
    }
    hasher.finalize_xof()
}

pub fn calculate_reference(referenced: &Value) -> BlobDigest {
    let result: [u8; 64] = calculate_digest_fixed::<sha3::Sha3_512>(referenced).into();
    BlobDigest::new(&result)
}
