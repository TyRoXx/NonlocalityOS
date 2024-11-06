#[cfg(test)]
mod tests {
    use crate::{OpenFileContentBlock, OpenFileContentBuffer, OptimizedWriteBuffer};
    use astraea::{
        storage::InMemoryValueStorage,
        tree::{BlobDigest, HashedValue, Value, ValueBlob, VALUE_BLOB_MAX_LENGTH},
    };
    use pretty_assertions::assert_eq;
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::Arc,
    };

    #[tokio::test]
    async fn optimized_write_buffer_empty() {
        for write_position in [0, 1, 10, 100, 1000, u64::MAX] {
            let buffer =
                OptimizedWriteBuffer::from_bytes(write_position, bytes::Bytes::new()).await;
            assert_eq!(bytes::Bytes::new(), buffer.prefix());
            assert_eq!(Vec::<HashedValue>::new(), *buffer.full_blocks());
            assert_eq!(bytes::Bytes::new(), buffer.suffix());
        }
    }

    #[tokio::test]
    async fn optimized_write_buffer_prefix_only() {
        for write_position in [
            0,
            1,
            10,
            100,
            1000,
            VALUE_BLOB_MAX_LENGTH as u64,
            VALUE_BLOB_MAX_LENGTH as u64 - 1,
            VALUE_BLOB_MAX_LENGTH as u64 + 1,
            u64::MAX - 1,
        ] {
            let buffer = OptimizedWriteBuffer::from_bytes(
                write_position,
                bytes::Bytes::copy_from_slice(&[b'x']),
            )
            .await;
            assert_eq!(bytes::Bytes::copy_from_slice(&[b'x']), buffer.prefix());
            assert_eq!(Vec::<HashedValue>::new(), *buffer.full_blocks());
            assert_eq!(bytes::Bytes::new(), buffer.suffix());
        }
    }

    #[tokio::test]
    async fn optimized_write_buffer_prefix_and_suffix_only() {
        for block_index in [0, 1, 10, 100, 1000] {
            for prefix_length in [1, 10, 100, 1000, VALUE_BLOB_MAX_LENGTH as u64 - 1] {
                for suffix_length in [1, 10, 100, 1000, VALUE_BLOB_MAX_LENGTH as u64 - 1] {
                    let position_in_block: u64 = VALUE_BLOB_MAX_LENGTH as u64 - prefix_length;
                    let write_position =
                        (block_index * VALUE_BLOB_MAX_LENGTH as u64) + position_in_block;
                    let prefix =
                        bytes::Bytes::from_iter(std::iter::repeat_n(b'p', prefix_length as usize));
                    let suffix =
                        bytes::Bytes::from_iter(std::iter::repeat_n(b's', suffix_length as usize));
                    let write_data = bytes::Bytes::from_iter(
                        prefix.clone().into_iter().chain(suffix.clone().into_iter()),
                    );
                    let buffer = OptimizedWriteBuffer::from_bytes(write_position, write_data).await;
                    assert_eq!(prefix, buffer.prefix());
                    assert_eq!(Vec::<HashedValue>::new(), *buffer.full_blocks());
                    assert_eq!(suffix, buffer.suffix());
                }
            }
        }
    }

    #[tokio::test]
    async fn optimized_write_buffer_full_blocks() {
        //TODO: reduce nesting
        futures::future::join_all([1, 10, VALUE_BLOB_MAX_LENGTH as u64 - 1].iter().map(
            |&prefix_length| {
                tokio::task::spawn(async move {
                    //TODO: use more interesting content for prefix
                    let prefix =
                        bytes::Bytes::from_iter(std::iter::repeat_n(b'p', prefix_length as usize));
                    futures::future::join_all(
                        [1, 10, VALUE_BLOB_MAX_LENGTH as u64 - 1]
                            .iter()
                            .map(|&suffix_length| {
                                tokio::task::spawn({
                                    let prefix = prefix.clone();
                                    async move {
                                        {
                                            //TODO: use more interesting content for suffix
                                            let suffix = bytes::Bytes::from_iter(
                                                std::iter::repeat_n(b's', suffix_length as usize),
                                            );
                                            for full_block_count in [1, 2, 5] {
                                                let position_in_block: u64 =
                                                    VALUE_BLOB_MAX_LENGTH as u64 - prefix_length;
                                                let write_data = bytes::Bytes::from_iter(
                                                    prefix
                                                        .clone()
                                                        .into_iter()
                                                        //TODO: use more interesting content for full_blocks
                                                        .chain(std::iter::repeat_n(
                                                            b'f',
                                                            (full_block_count
                                                                * VALUE_BLOB_MAX_LENGTH)
                                                                as usize,
                                                        ))
                                                        .chain(suffix.clone().into_iter()),
                                                );
                                                for block_index in [0, 100] {
                                                    let write_position = (block_index
                                                        * VALUE_BLOB_MAX_LENGTH as u64)
                                                        + position_in_block;
                                                    let buffer = OptimizedWriteBuffer::from_bytes(
                                                        write_position,
                                                        write_data.clone(),
                                                    )
                                                    .await;
                                                    assert_eq!(prefix, buffer.prefix());
                                                    assert_eq!(
                                                        full_block_count,
                                                        buffer.full_blocks().len()
                                                    );
                                                    assert!(buffer.full_blocks().iter().all(
                                                        |full_block| full_block
                                                            .value()
                                                            .blob()
                                                            .as_slice()
                                                            .iter()
                                                            .all(|&byte| byte == b'f')
                                                    ));
                                                    assert_eq!(suffix, buffer.suffix());
                                                }
                                            }
                                        }
                                    }
                                })
                            }),
                    )
                    .await;
                })
            },
        ))
        .await;
    }

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
            dirty_blocks: VecDeque::from([0, 1]),
        });
        assert_eq!(expected_buffer, buffer);
        // cargo fmt silently refuses to format this for an unknown reason:
        let expected_digests =
        BTreeSet::from_iter (["a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26",
       ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
        assert_eq!(expected_digests, storage.digests());
    }

    fn random_bytes(len: usize) -> Vec<u8> {
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut small_rng = SmallRng::seed_from_u64(123);
        (0..len).map(|_| small_rng.gen()).collect()
    }

    #[tokio::test]
    async fn open_file_content_buffer_overwrite_full_block() {
        let data = random_bytes(VALUE_BLOB_MAX_LENGTH);
        let last_known_digest = BlobDigest::hash(&data);
        let last_known_digest_file_size = data.len();
        let mut buffer = OpenFileContentBuffer::from_data(
            data,
            last_known_digest,
            last_known_digest_file_size as u64,
        )
        .unwrap();
        let write_position = 0 as u64;
        let write_data = bytes::Bytes::from(random_bytes(last_known_digest_file_size));
        let write_buffer =
            OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
        let storage = Arc::new(InMemoryValueStorage::empty());
        let _write_result: () = buffer
            .write(write_position, write_buffer, storage.clone())
            .await
            .unwrap();
        let expected_buffer = OpenFileContentBuffer::Loaded(crate::OpenFileContentBufferLoaded {
            size: last_known_digest_file_size as u64,
            blocks: vec![OpenFileContentBlock::Loaded(
                crate::LoadedBlock::KnownDigest(HashedValue::from(Arc::new(Value::new(
                    ValueBlob::try_from(write_data.clone()).unwrap(),
                    Vec::new(),
                )))),
            )],
            digest: crate::DigestStatus {
                last_known_digest: last_known_digest,
                is_digest_up_to_date: false,
            },
            last_known_digest_file_size: last_known_digest_file_size as u64,
            dirty_blocks: VecDeque::from([0]),
        });
        assert_eq!(expected_buffer, buffer);
        // cargo fmt silently refuses to format this for an unknown reason:
        let expected_digests =
        BTreeSet::from_iter (["23f3c29d5ead1d624ce6a64c730d6bb84acd6f9e6a51d411e189d396825ae4e393cdf18ddbe5a23b820c975f9efaa96d25cbfa14af369f5665fce583b44abc25",
       ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
        assert_eq!(expected_digests, storage.digests());
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
        buffer.store_all(storage.clone()).await.unwrap();
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
            dirty_blocks: VecDeque::new(),
        });
        assert_eq!(expected_buffer, buffer);

        // cargo fmt silently refuses to format this for an unknown reason:
        let expected_digests =
        BTreeSet::from_iter (["697f2d856172cb8309d6b8b97dac4de344b549d4dee61edfb4962d8698b7fa803f4f93ff24393586e28b5b957ac3d1d369420ce53332712f997bd336d09ab02a",
    "36708536177e3b63fe3cc7a9ab2e93c26394d2e00933b243c9f3ab93c245a8253a731314365fbd5094ad33d64a083bf1b63b8471c55aab7a7efb4702d7e75459"    ,
       "842a5f571599b6ccaa2b5aee1fc46e95ffd32a8392e33c1c6b6aabfe78392a0c0bb3c1fa29056b093f784c4a1bd9eb6a6d30494d9e5105a1b8131214be40eae5",
       "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26",
    ].map(BlobDigest::parse_hex_string).map(Option::unwrap));

        assert_eq!(expected_digests, storage.digests());
    }
}
