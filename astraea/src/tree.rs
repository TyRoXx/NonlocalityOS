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

#[test]
fn test_calculate_reference_blob_no_references_0() {
    let value = Arc::new(Value::empty());
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26").unwrap());
}

#[test]
fn test_calculate_reference_blob_yes_references_0() {
    let value = Arc::new(Value::new(
        ValueBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        Vec::new(),
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "8e47f1185ffd014d238fabd02a1a32defe698cbf38c037a90e3c0a0a32370fb52cbd641250508502295fcabcbf676c09470b27443868c8e5f70e26dc337288af").unwrap());
}

#[test]
fn test_calculate_reference_blob_no_references_1() {
    let value = Arc::new(Value::new(
        ValueBlob::empty(),
        vec![BlobDigest(([0u8; 32], [0u8; 32]))],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "f8d76fdd8a082a67eaab47b5518ac486cb9a90dcb9f3c9efcfd86d5c8b3f1831601d3c8435f84b9e56da91283d5b98040e6e7b2c8dd9aa5bd4ebdf1823a7cf29").unwrap());
}

#[test]
fn test_calculate_reference_blob_yes_references_1() {
    let value = Arc::new(Value::new(
        ValueBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        vec![BlobDigest(([0u8; 32], [0u8; 32]))],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "9bcc4c3988a38c854a4bacd9e09e438655453e12f1b5196771c52bd87e280b28012145c21a7488c05c2c7909003c1b694532b22ec38f6cb1d3b7067defe2b58d").unwrap());
}

#[test]
fn test_calculate_reference_blob_no_references_2() {
    let value = Arc::new(Value::new(
        ValueBlob::empty(),
        vec![
            BlobDigest(([0u8; 32], [0u8; 32])),
            BlobDigest(([1u8; 32], [1u8; 32])),
        ],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "92a55955611e3ba7f935fe2e518504c7e37dfd9f9809faa1be93cd91e3c14a5914acd44ea9f442389a8ee3649de60ede71a8d9b85d3560d8ced78216597db304").unwrap());
}

#[test]
fn test_calculate_reference_blob_yes_references_2() {
    let value = Arc::new(Value::new(
        ValueBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        vec![
            BlobDigest(([0u8; 32], [0u8; 32])),
            BlobDigest(([1u8; 32], [1u8; 32])),
        ],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "a8f64b8aabe17bc2091f8a801d67501f69c26b8a9164890c4e185a03ff82cf98c72baed1c78a4281aed776ca757cbe5d573e3874bacf8cd45cec5c3c5d0b1279").unwrap());
}

#[test]
fn test_calculate_reference_collision() {
    let digest = BlobDigest(([0u8; 32], [0u8; 32]));
    let digest_in_reference = Arc::new(Value::new(ValueBlob::empty(), vec![digest.clone()]));
    let digest_in_blob = Arc::new(Value::new(
        ValueBlob::try_from(
            [
                &DEPRECATED_TYPE_ID_IN_DIGEST.to_be_bytes(),
                &digest.0 .0[..],
                &digest.0 .1[..],
            ]
            .concat()
            .into(),
        )
        .unwrap(),
        vec![],
    ));
    let digest_in_reference_digest = calculate_reference(&digest_in_reference);
    assert_eq!(
        digest_in_reference_digest,
        BlobDigest::parse_hex_string(
            "f8d76fdd8a082a67eaab47b5518ac486cb9a90dcb9f3c9efcfd86d5c8b3f1831601d3c8435f84b9e56da91283d5b98040e6e7b2c8dd9aa5bd4ebdf1823a7cf29").unwrap());
    let digest_in_blob_digest = calculate_reference(&digest_in_blob);
    assert_eq!(
        digest_in_blob_digest,
        BlobDigest::parse_hex_string(
            "f8d76fdd8a082a67eaab47b5518ac486cb9a90dcb9f3c9efcfd86d5c8b3f1831601d3c8435f84b9e56da91283d5b98040e6e7b2c8dd9aa5bd4ebdf1823a7cf29").unwrap());
    // this collision is a design error and should be corrected
    assert_eq!(digest_in_reference_digest, digest_in_blob_digest);
}
