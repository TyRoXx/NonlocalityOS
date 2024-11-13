#[cfg(test)]
mod tests {
    use crate::{OpenFileContentBlock, OpenFileContentBuffer, OptimizedWriteBuffer};
    use astraea::{
        storage::{InMemoryValueStorage, LoadStoreValue},
        tree::{BlobDigest, HashedValue, Value, ValueBlob, VALUE_BLOB_MAX_LENGTH},
    };
    use pretty_assertions::assert_eq;
    use std::{
        collections::{BTreeSet, VecDeque},
        sync::Arc,
    };
    use test_case::{test_case, test_matrix};
    use tokio::runtime::Runtime;

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

    #[test_matrix(
        [1, 10, 63_999],
        [1, 10, 63_999],
        [1, 2]
    )]
    #[tokio::test]
    async fn optimized_write_buffer_full_blocks(
        prefix_length: u64,
        suffix_length: u64,
        full_block_count: usize,
    ) {
        //TODO: use more interesting content for prefix
        let prefix = bytes::Bytes::from_iter(std::iter::repeat_n(b'p', prefix_length as usize));
        //TODO: use more interesting content for suffix
        let suffix = bytes::Bytes::from_iter(std::iter::repeat_n(b's', suffix_length as usize));
        let position_in_block: u64 = VALUE_BLOB_MAX_LENGTH as u64 - prefix_length;
        let write_data = bytes::Bytes::from_iter(
            prefix
                .clone()
                .into_iter()
                //TODO: use more interesting content for full_blocks
                .chain(std::iter::repeat_n(
                    b'f',
                    (full_block_count * VALUE_BLOB_MAX_LENGTH) as usize,
                ))
                .chain(suffix.clone().into_iter()),
        );
        for block_index in [0, 100] {
            let write_position = (block_index * VALUE_BLOB_MAX_LENGTH as u64) + position_in_block;
            let buffer = OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
            assert_eq!(prefix, buffer.prefix());
            assert_eq!(full_block_count, buffer.full_blocks().len());
            assert!(buffer.full_blocks().iter().all(|full_block| {
                full_block
                    .value()
                    .blob()
                    .as_slice()
                    .iter()
                    .all(|&byte| byte == b'f')
            }));
            assert_eq!(suffix, buffer.suffix());
        }
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
            1,
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
            write_buffer_in_blocks: 1,
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
            1,
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
            write_buffer_in_blocks: 1,
        });
        assert_eq!(expected_buffer, buffer);
        // cargo fmt silently refuses to format this for an unknown reason:
        let expected_digests =
        BTreeSet::from_iter (["23f3c29d5ead1d624ce6a64c730d6bb84acd6f9e6a51d411e189d396825ae4e393cdf18ddbe5a23b820c975f9efaa96d25cbfa14af369f5665fce583b44abc25",
       ].map(BlobDigest::parse_hex_string).map(Option::unwrap));
        assert_eq!(expected_digests, storage.digests());
    }

    #[test_case(0)]
    #[test_case(1)]
    #[test_case(2_000)]
    #[test_case(64_000)]
    #[test_case(200_000)]
    fn open_file_content_buffer_write_zero_bytes(write_position: u64) {
        Runtime::new().unwrap().block_on(async {
            let original_content = random_bytes(VALUE_BLOB_MAX_LENGTH);
            let last_known_digest = BlobDigest::hash(&original_content);
            let last_known_digest_file_size = original_content.len();
            let mut buffer = OpenFileContentBuffer::from_data(
                original_content.clone(),
                last_known_digest,
                last_known_digest_file_size as u64,
                1,
            )
            .unwrap();
            let write_data = bytes::Bytes::new();
            let write_buffer =
                OptimizedWriteBuffer::from_bytes(write_position, write_data.clone()).await;
            let storage = Arc::new(InMemoryValueStorage::empty());
            let _write_result: () = buffer
                .write(write_position, write_buffer, storage.clone())
                .await
                .unwrap();
            let expected_size = std::cmp::max(write_position, last_known_digest_file_size as u64);
            assert_eq!(expected_size, buffer.size());
            let zeroes = expected_size as usize - original_content.len();
            let expected_content = bytes::Bytes::from_iter(
                original_content
                    .into_iter()
                    .chain(std::iter::repeat_n(0u8, zeroes)),
            );
            check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
        });
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
            1,
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
            write_buffer_in_blocks:1,
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

    async fn check_open_file_content_buffer(
        buffer: &mut OpenFileContentBuffer,
        expected_content: bytes::Bytes,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        let mut checked = 0;
        while checked < expected_content.len() {
            let read_result = buffer
                .read(
                    checked as u64,
                    expected_content.len() - checked,
                    storage.clone(),
                )
                .await;
            let read_bytes = read_result.unwrap();
            let expected_piece = expected_content.slice(checked..(checked + read_bytes.len()));
            assert_eq!(expected_piece.len(), read_bytes.len());
            assert!(expected_piece == read_bytes);
            checked += read_bytes.len();
        }
        assert_eq!(expected_content.len(), checked);
    }

    #[test_case(0)]
    #[test_case(1)]
    #[test_case(20)]
    #[test_case(2_000)]
    #[test_case(200_000)]
    fn open_file_content_buffer_sizes(size: usize) {
        Runtime::new().unwrap().block_on(async {
            let initial_content = Vec::new();
            let last_known_digest = BlobDigest::hash(&initial_content);
            let last_known_digest_file_size = initial_content.len();
            let mut buffer = OpenFileContentBuffer::from_data(
                initial_content,
                last_known_digest,
                last_known_digest_file_size as u64,
                1,
            )
            .unwrap();
            let new_content = bytes::Bytes::from(random_bytes(size));
            let storage = Arc::new(InMemoryValueStorage::empty());
            buffer
                .write(
                    0,
                    OptimizedWriteBuffer::from_bytes(0, new_content.clone()).await,
                    storage.clone(),
                )
                .await
                .unwrap();
            check_open_file_content_buffer(&mut buffer, new_content, storage).await;
        });
    }

    #[test_case(1)]
    #[test_case(2_000)]
    #[test_case(63_999)]
    fn open_file_content_buffer_write_completes_a_block(write_position: u16) {
        Runtime::new().unwrap().block_on(async {
            let original_content = random_bytes(write_position as usize);
            let last_known_digest = BlobDigest::hash(&original_content);
            let last_known_digest_file_size = original_content.len();
            let mut buffer = OpenFileContentBuffer::from_data(
                original_content.clone(),
                last_known_digest,
                last_known_digest_file_size as u64,
                1,
            )
            .unwrap();
            let write_size = VALUE_BLOB_MAX_LENGTH - write_position as usize;
            let write_data = bytes::Bytes::from(random_bytes(write_size));
            let write_buffer =
                OptimizedWriteBuffer::from_bytes(write_position as u64, write_data.clone()).await;
            assert_eq!(write_size, write_buffer.prefix().len());
            let storage = Arc::new(InMemoryValueStorage::empty());
            let _write_result: () = buffer
                .write(write_position as u64, write_buffer, storage.clone())
                .await
                .unwrap();
            let expected_size = VALUE_BLOB_MAX_LENGTH as u64;
            assert_eq!(expected_size, buffer.size());
            let expected_content = bytes::Bytes::from_iter(
                original_content
                    .into_iter()
                    .chain(write_data.iter().copied()),
            );
            check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
        });
    }

    #[test_case(1)]
    #[test_case(2_000)]
    #[test_case(63_999)]
    fn open_file_content_buffer_write_creates_full_block_with_zero_fill(write_position: u16) {
        Runtime::new().unwrap().block_on(async {
            let original_content: Vec<u8> =
                std::iter::repeat_n(1u8, VALUE_BLOB_MAX_LENGTH).collect();
            let last_known_digest = BlobDigest::hash(&original_content);
            let last_known_digest_file_size = original_content.len();
            let mut buffer = OpenFileContentBuffer::from_data(
                original_content.clone(),
                last_known_digest,
                last_known_digest_file_size as u64,
                1,
            )
            .unwrap();
            let write_size = VALUE_BLOB_MAX_LENGTH - write_position as usize;
            let write_data = bytes::Bytes::from(random_bytes(write_size));
            let write_buffer =
                OptimizedWriteBuffer::from_bytes(write_position as u64, write_data.clone()).await;
            assert_eq!(write_size, write_buffer.prefix().len());
            let storage = Arc::new(InMemoryValueStorage::empty());
            let _write_result: () = buffer
                .write(
                    original_content.len() as u64 + write_position as u64,
                    write_buffer,
                    storage.clone(),
                )
                .await
                .unwrap();
            let expected_size = original_content.len() as u64 + VALUE_BLOB_MAX_LENGTH as u64;
            assert_eq!(expected_size, buffer.size());
            let expected_content = bytes::Bytes::from_iter(
                original_content
                    .iter()
                    .copied()
                    .chain(std::iter::repeat_n(0u8, write_position as usize))
                    .chain(write_data.iter().copied()),
            );
            check_open_file_content_buffer(&mut buffer, expected_content, storage).await;
        });
    }
}
