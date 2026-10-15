use crate::yt_dlp::hash_file;
use astraea::tree::BlobDigest;
use mxc_sdk::v1::RunOptions;

#[test_log::test]
fn test_hash_file() {
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, "Hello, world!").expect("Failed to write test file");
    let digest = hash_file(&file_path).expect("Failed to hash file");
    assert_eq!(digest, BlobDigest::hash(b"Hello, world!"));
}

#[test_log::test(tokio::test)]
async fn test_mxc() {
    // We want to sandbox yt_dlp at some point. mxc could be used for this purpose.
    use mxc_sdk::v1::{self, ContainerRequest, FilesystemPolicy, WaitResult};
    let command = if cfg!(windows) {
        "cmd.exe /C echo hello"
    } else {
        "echo hello"
    };
    let readonly_path = if cfg!(windows) { r"C:\Windows" } else { "/usr" };
    let request = ContainerRequest {
        timeout_ms: Some(10_000),
        filesystem: Some(FilesystemPolicy {
            readonly_paths: vec![readonly_path.into()],
            ..Default::default()
        }),
        ..ContainerRequest::new(command)
    };
    let run_options = RunOptions { telemetry: None };
    let output = v1::run(request, run_options).unwrap();
    assert_eq!("", String::from_utf8_lossy(&output.stderr));
    assert_eq!("hello", String::from_utf8_lossy(&output.stdout).trim());
    assert_eq!(WaitResult::Exited(0), output.outcome);
    assert_eq!(Vec::<String>::new(), output.warnings);
}
