use std::sync::Arc;

use crate::tree::{
    calculate_reference, BlobDigest, HashedTree, ReferenceIndex, Tree, TreeBlob,
    TreeDeserializationError, TreeSerializationError, VALUE_BLOB_MAX_LENGTH,
};
use proptest::proptest;

#[test_log::test]
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

#[test_log::test]
fn test_display_reference_index() {
    let index = ReferenceIndex(123);
    assert_eq!(format!("{}", index), "123");
}

#[test_log::test]
fn test_debug_tree_blob() {
    let blob = TreeBlob::empty();
    assert_eq!(format!("{:?}", blob), "ValueBlob { content.len(): 0 }");
}

proptest! {
    #[test_log::test]
    fn tree_blob_try_from_success(length in 0usize..VALUE_BLOB_MAX_LENGTH) {
        let content = bytes::Bytes::from_iter(std::iter::repeat_n(0u8, length));
        let tree_blob = TreeBlob::try_from(content.clone()).unwrap();
        assert_eq!(content, tree_blob.content);
    }

    #[test_log::test]
    fn tree_blob_try_from_failure(length in (VALUE_BLOB_MAX_LENGTH + 1)..(VALUE_BLOB_MAX_LENGTH * 3) /*We don't want to allocate too much memory here.*/) {
        let content = bytes::Bytes::from_iter(std::iter::repeat_n(0u8, length));
        let result = TreeBlob::try_from(content.clone());
        assert_eq!(None, result);
    }
}

#[test_log::test]
fn test_display_value_serialization_error() {
    let error = TreeSerializationError::BlobTooLong;
    assert_eq!(format!("{}", error), "BlobTooLong");
}

#[test_log::test]
fn test_display_value_deserialization_error() {
    assert_eq!(
        format!("{}", TreeDeserializationError::ReferencesNotAllowed),
        "ReferencesNotAllowed"
    );
    assert_eq!(
        format!(
            "{}",
            TreeDeserializationError::Postcard(postcard::Error::DeserializeUnexpectedEnd)
        ),
        "Postcard(DeserializeUnexpectedEnd)"
    );
    assert_eq!(
        format!(
            "{}",
            TreeDeserializationError::BlobUnavailable(BlobDigest::new(&[0u8; 64]),)
        ),
        "BlobUnavailable(BlobDigest(\"00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\"))"
    );
}

#[test_log::test]
fn test_display_hashed_value() {
    let value = Arc::new(Tree::empty());
    let hashed_value = HashedTree::from(value.clone());
    assert_eq!(
        format!("{}", hashed_value),
        format!("{}", hashed_value.digest())
    );
}

#[test_log::test]
fn test_calculate_reference_blob_no_references_0() {
    let value = Arc::new(Tree::empty());
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap());
}

#[test_log::test]
fn test_calculate_reference_blob_yes_references_0() {
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        Vec::new(),
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "f671d56d459e4cc29611ca33f39d4f9dc500d23d69a6b07540dca1a0313057b0a48a4e8859fbcc76242b6fa6bc8179d37201384ea96b7c2bbc61c0bd89b9f7d2").unwrap());
}

#[test_log::test]
fn test_calculate_reference_blob_no_references_1() {
    let value = Arc::new(Tree::new(
        TreeBlob::empty(),
        vec![BlobDigest(([0u8; 32], [0u8; 32]))],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "e32b9bb31183fcfe17c1a29367ad4e5dabd5b73ab1679fc0244ad627f63312edd74c6e0ebc767d2f9d97f3acf07fb4c5b83b75c98599413b3e8b8db4a69dac19").unwrap());
}

#[test_log::test]
fn test_calculate_reference_blob_yes_references_1() {
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        vec![BlobDigest(([0u8; 32], [0u8; 32]))],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "9cc8ca04fdc28c56986b8f3c690a80691035536f02f45a571ae09f845bc29b6e7671592eb1bcfd15013676eac61db04b33ba3fc23950adf2b29e9eabaf985f67").unwrap());
}

#[test_log::test]
fn test_calculate_reference_blob_no_references_2() {
    let value = Arc::new(Tree::new(
        TreeBlob::empty(),
        vec![
            BlobDigest(([0u8; 32], [0u8; 32])),
            BlobDigest(([1u8; 32], [1u8; 32])),
        ],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "075df311abbec692910aa752e93ed32049a55793235558a8a741c5dfdcbb9e7e7b6fa06b0060e29c7d4c3ea2e89200638b1dc2925db7c81ae99e11957c53b11f").unwrap());
}

#[test_log::test]
fn test_calculate_reference_blob_yes_references_2() {
    let value = Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from("Hello, world!")).unwrap(),
        vec![
            BlobDigest(([0u8; 32], [0u8; 32])),
            BlobDigest(([1u8; 32], [1u8; 32])),
        ],
    ));
    let reference = calculate_reference(&value);
    assert_eq!(
        reference,
        BlobDigest::parse_hex_string(
            "ed2f76ba42ecee524b9cbdd10a8eedd879b0a2a1a8f51f633c40a8293fee31f2d75c8b07b95f1f4696ddb3b9aef71b9a1fe45e04347224f2ae405b6bb3a96124").unwrap());
}
