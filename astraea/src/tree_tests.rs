use std::sync::Arc;

use crate::tree::{
    calculate_reference, BlobDigest, HashedValue, ReferenceIndex, Value, ValueBlob,
    ValueDeserializationError, ValueSerializationError, DEPRECATED_TYPE_ID_IN_DIGEST,
    VALUE_BLOB_MAX_LENGTH,
};
use proptest::proptest;

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

#[test]
fn test_display_reference_index() {
    let index = ReferenceIndex(123);
    assert_eq!(format!("{}", index), "123");
}

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

#[test]
fn test_display_value_serialization_error() {
    let error = ValueSerializationError::BlobTooLong;
    assert_eq!(format!("{}", error), "BlobTooLong");
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

#[test]
fn test_display_hashed_value() {
    let value = Arc::new(Value::empty());
    let hashed_value = HashedValue::from(value.clone());
    assert_eq!(
        format!("{}", hashed_value),
        format!("{}", hashed_value.digest())
    );
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
