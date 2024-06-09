#![feature(array_chunks)]
#[deny(warnings)]
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_512};

/// SHA3-512 hash. Supports Serde because we will need this type a lot in network protocols and file formats.
#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct BlobDigest(
    /// data is split into two parts because Serde doesn't support 64-element arrays
    pub ([u8; 32], [u8; 32]),
);

impl BlobDigest {
    pub fn new(value: &[u8; 64]) -> BlobDigest {
        let (first, second) = value.split_at(32);
        BlobDigest((first.try_into().unwrap(), second.try_into().unwrap()))
    }

    pub fn hash(input: &[u8]) -> BlobDigest {
        let mut hasher = Sha3_512::new();
        hasher.update(input);
        let result = hasher.finalize();
        let slice: &[u8] = result.as_slice();
        let mut chunks: std::slice::ArrayChunks<u8, 64> = slice.array_chunks();
        let chunk = chunks.next().unwrap();
        assert!(chunks.remainder().is_empty());
        BlobDigest::new(chunk)
    }
}

impl std::convert::Into<[u8; 64]> for BlobDigest {
    fn into(self) -> [u8; 64] {
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(&self.0 .0);
        result[32..].copy_from_slice(&self.0 .1);
        result
    }
}

#[test]
fn test_calculate_digest_empty() {
    // empty input digest matches example from https://en.wikipedia.org/wiki/SHA-3#Examples_of_SHA-3_variants
    let digest: [u8; 64] = BlobDigest::hash(&[]).into();
    assert_eq!("a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26",hex::encode( &digest  ));
}

#[test]
fn test_calculate_digest_non_empty() {
    let digest: [u8; 64] = BlobDigest::hash("Hello, world!".as_bytes()).into();
    assert_eq!("8e47f1185ffd014d238fabd02a1a32defe698cbf38c037a90e3c0a0a32370fb52cbd641250508502295fcabcbf676c09470b27443868c8e5f70e26dc337288af",hex::encode( &digest  ));
}
