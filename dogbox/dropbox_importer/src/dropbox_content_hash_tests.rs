use crate::dropbox_content_hash::DropboxContentHasher;
use hex_literal::hex;
use pretty_assertions::assert_eq;
use std::io::Read;

#[test_log::test]
fn test_dropbox_content_hasher_empty_file() {
    let expected = hex!("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    let hasher = DropboxContentHasher::new();
    let result = hasher.finalize();
    assert_eq!(expected, *result);
}

#[test_log::test]
fn test_dropbox_content_hasher_small_file() {
    let data = b"hello world";
    let expected = hex!("bc62d4b80d9e36da29c16c5d4d9f11731f36052c72401a76c23c0fb5a9b74423");
    let mut hasher = DropboxContentHasher::new();
    hasher.update(data);
    let result = hasher.finalize();
    assert_eq!(hex::encode(expected), hex::encode(&*result));
}

#[test_log::test]
fn test_dropbox_content_hasher_large_file() {
    // example file from the Dropbox docs: https://www.dropbox.com/developers/reference/content-hash?_tk=guides_lp&_ad=deepdive3&_camp=content_hash
    let path = std::env::current_dir()
        .unwrap()
        .join("test_data/milky-way-nasa.jpg");
    let mut file = std::fs::File::open(&path)
        .unwrap_or_else(|_| panic!("Couldn't open test file at path: {:?}", path));
    let expected = hex!("485291fa0ee50c016982abbfa943957bcd231aae0492ccbaa22c58e3997b35e0");
    let mut hasher = DropboxContentHasher::new();
    loop {
        let mut buffer = [0u8; 32 * 1024];
        let bytes_read = file
            .read(&mut buffer)
            .expect("Couldn't read from test file");
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    let result = hasher.finalize();
    assert_eq!(expected, *result);
}
