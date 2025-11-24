use crate::yt_dlp::hash_file;
use astraea::tree::BlobDigest;

#[test_log::test]
fn test_hash_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, "Hello, world!").expect("Failed to write test file");
    let digest = hash_file(&file_path).expect("Failed to hash file");
    assert_eq!(digest, BlobDigest::hash(b"Hello, world!"));
}
