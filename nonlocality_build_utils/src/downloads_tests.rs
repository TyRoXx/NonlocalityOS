use crate::downloads::{install_from_downloaded_archive, Compression};

#[test_log::test]
fn host_not_reachable() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    let unpacked = directory.path().join("unpacked");
    let result =
        install_from_downloaded_archive("https://0.0.0.0:9999", &file, &unpacked, Compression::Gz);
    assert!(result.is_err());
    let entries: Vec<std::io::Result<std::fs::DirEntry>> =
        directory.path().read_dir().unwrap().collect();
    assert!(entries.is_empty());
}
