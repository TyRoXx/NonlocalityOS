use crate::dropbox_api::{calculate_range_end, join_dropbox_path, parse_sha256_hex};
use hex_literal::hex;
use pretty_assertions::assert_eq;

#[test_log::test]
fn test_parse_sha256_hex() {
    let valid_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    let invalid_hex = "invalid_hex_string";
    let too_short_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcd";
    let too_long_hex = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9aa";
    assert_eq!(
        hex!("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"),
        *parse_sha256_hex(valid_hex).unwrap()
    );
    assert_eq!(None, parse_sha256_hex(invalid_hex));
    assert_eq!(None, parse_sha256_hex(too_short_hex));
    assert_eq!(None, parse_sha256_hex(too_long_hex));
}

#[test_log::test]
fn test_join_dropbox_path() {
    assert_eq!(join_dropbox_path("/parent", "child"), "/parent/child");
    assert_eq!(join_dropbox_path("/parent", "/child"), "/parent/child");
    assert_eq!(join_dropbox_path("/parent", "/child/"), "/parent/child/");
    assert_eq!(join_dropbox_path("/parent", "child/"), "/parent/child/");
    assert_eq!(join_dropbox_path("/parent/", "child"), "/parent/child");
    assert_eq!(join_dropbox_path("/parent/", "/child"), "/parent/child");
    assert_eq!(join_dropbox_path("/parent/", "/child/"), "/parent/child/");
    assert_eq!(join_dropbox_path("", "child"), "/child");
    assert_eq!(join_dropbox_path("", "/child"), "/child");
    assert_eq!(join_dropbox_path("", ""), "/");
}

#[test_log::test]
fn test_calculate_range_end() {
    assert_eq!(
        "Download range must be at least 1 byte long",
        calculate_range_end(0, 0).unwrap_err().to_string()
    );
    assert_eq!(
        "Download range must be at least 1 byte long",
        calculate_range_end(1, 0).unwrap_err().to_string()
    );
    assert_eq!(
        "Download range must be at least 1 byte long",
        calculate_range_end(u64::MAX, 0).unwrap_err().to_string()
    );
    assert_eq!(
        "Invalid download offset or length",
        calculate_range_end(u64::MAX, 1).unwrap_err().to_string()
    );
    assert_eq!(
        "Invalid download offset or length",
        calculate_range_end(1, u64::MAX).unwrap_err().to_string()
    );
    assert_eq!(0, calculate_range_end(0, 1).unwrap());
    assert_eq!(2, calculate_range_end(1, 2).unwrap());
    assert_eq!(299999, calculate_range_end(200000, 100000).unwrap());
    assert_eq!(u64::MAX - 1, calculate_range_end(0, u64::MAX).unwrap());
    assert_eq!(u64::MAX - 1, calculate_range_end(u64::MAX - 1, 1).unwrap());
}
