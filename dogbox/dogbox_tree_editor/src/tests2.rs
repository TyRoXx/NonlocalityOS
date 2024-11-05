#[cfg(test)]
mod tests {
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
        let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
            size: VALUE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
            blocks: vec![
                OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                    vec![0; VALUE_BLOB_MAX_LENGTH],
                )),
                OpenFileContentBlock::Loaded(crate::LoadedBlock::UnknownDigest(
                    write_data.as_bytes().to_vec(),
                )),
            ],
            digest: crate::DigestStatus {
                last_known_digest: last_known_digest,
                is_digest_up_to_date: false,
            },
            last_known_digest_file_size: last_known_digest_file_size as u64,
            number_of_bytes_written_since_last_save: VALUE_BLOB_MAX_LENGTH as u64
                + write_data.len() as u64,
        });
        assert_eq!(expected_buffer, buffer);
        assert_eq!(BTreeSet::new(), storage.digests());
    }

    #[tokio::test]
    async fn open_file_content_buffer_store() {
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
        buffer.store(storage.clone()).await.unwrap();
        let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
            size: VALUE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
            blocks: vec![
                OpenFileContentBlock::NotLoaded(
                    BlobDigest::hash(&vec![0; VALUE_BLOB_MAX_LENGTH]),
                    VALUE_BLOB_MAX_LENGTH as u16
                ),
                OpenFileContentBlock::NotLoaded(
                    BlobDigest::hash(write_data.as_bytes()),
                    write_data.len() as u16
                )
            ],
            digest: crate::DigestStatus {
                last_known_digest: BlobDigest::parse_hex_string("842a5f571599b6ccaa2b5aee1fc46e95ffd32a8392e33c1c6b6aabfe78392a0c0bb3c1fa29056b093f784c4a1bd9eb6a6d30494d9e5105a1b8131214be40eae5").unwrap(),
                is_digest_up_to_date: true
            },
            last_known_digest_file_size: VALUE_BLOB_MAX_LENGTH as u64 + write_data.len() as u64,
            number_of_bytes_written_since_last_save:  0,
        });
        assert_eq!(expected_buffer, buffer);

        // cargo fmt silently refuses to format this for an unknown reason:
        let expected_digests =
        BTreeSet::from_iter (["697f2d856172cb8309d6b8b97dac4de344b549d4dee61edfb4962d8698b7fa803f4f93ff24393586e28b5b957ac3d1d369420ce53332712f997bd336d09ab02a",
    "36708536177e3b63fe3cc7a9ab2e93c26394d2e00933b243c9f3ab93c245a8253a731314365fbd5094ad33d64a083bf1b63b8471c55aab7a7efb4702d7e75459"
    ,
       "842a5f571599b6ccaa2b5aee1fc46e95ffd32a8392e33c1c6b6aabfe78392a0c0bb3c1fa29056b093f784c4a1bd9eb6a6d30494d9e5105a1b8131214be40eae5"
    ].map(BlobDigest::parse_hex_string).map(Option::unwrap));

        assert_eq!(expected_digests, storage.digests());
    }
}
