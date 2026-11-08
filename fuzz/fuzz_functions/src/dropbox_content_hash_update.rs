use arbitrary::Unstructured;
use dropbox_importer::dropbox_content_hash::DropboxContentHasher;
use pretty_assertions::assert_eq;

pub fn fuzz_function(data: &[u8]) -> bool {
    let mut unstructured = Unstructured::new(data);
    let pieces: Vec<Vec<u8>> = match unstructured.arbitrary() {
        Ok(success) => success,
        Err(_) => return false,
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let parallel_digest = {
                let mut hasher = DropboxContentHasher::new();
                hasher
                    .update(
                        pieces
                            .iter()
                            .map(|element| element.as_slice())
                            .collect::<Vec<&[u8]>>()
                            .as_slice(),
                    )
                    .await;
                hasher.finalize()
            };
            let serial_digest = {
                let mut hasher = DropboxContentHasher::new();
                for piece in &pieces {
                    hasher.update(&[piece.as_slice()]).await;
                }
                hasher.finalize()
            };
            assert_eq!(parallel_digest, serial_digest);
            let concatenated_digest = {
                let mut hasher = DropboxContentHasher::new();
                let concatenated = pieces.concat();
                hasher.update(&[concatenated.as_slice()]).await;
                hasher.finalize()
            };
            assert_eq!(parallel_digest, concatenated_digest);
        });
    true
}
