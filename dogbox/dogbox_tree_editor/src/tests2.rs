use crate::{OpenFileContentBlock, OpenFileContentBuffer, OptimizedWriteBuffer};
use astraea::{
    storage::InMemoryValueStorage,
    tree::{BlobDigest, VALUE_BLOB_MAX_LENGTH},
};
use pretty_assertions::assert_eq;
use std::{collections::BTreeSet, sync::Arc};

#[tokio::test]
async fn open_file_content_buffer_write_fill_zero_block() {
    let data = Vec::new();
    let last_known_digest = BlobDigest::hash(&data);
    let last_known_digest_file_size = data.len();
    let mut buffer = OpenFileContentBuffer::from_data(
        data,
        last_known_digest,
        last_known_digest_file_size as u64,
    )
    .unwrap();
    let write_position = VALUE_BLOB_MAX_LENGTH as u64;
    let write_data = "a";
    let write_buffer =
        OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::from(write_data)).await;
    let storage = Arc::new(InMemoryValueStorage::empty());
    let _write_result: () = buffer
        .write(write_position, write_buffer, storage.clone())
        .await
        .unwrap();
    assert_eq!(
        OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
            size: VALUE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
            blocks: vec![
                OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                    vec![0; VALUE_BLOB_MAX_LENGTH]
                )),
                OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                    write_data.as_bytes().to_vec()
                ))
            ],
            digest: crate::DigestStatus {
                last_known_digest: last_known_digest,
                is_digest_up_to_date: false
            },
            last_known_digest_file_size: last_known_digest_file_size as u64,
            number_of_bytes_written_since_last_save: VALUE_BLOB_MAX_LENGTH as u64
                + write_data.len() as u64,
        }),
        buffer
    );
    assert_eq!(BTreeSet::new(), storage.digests());
}
