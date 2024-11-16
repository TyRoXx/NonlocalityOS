extern crate test;

#[cfg(test)]
mod tests {
    use super::test::Bencher;
    use crate::OpenFileContentBuffer;
    use crate::OptimizedWriteBuffer;
    use crate::StoreChanges;
    use astraea::storage::InMemoryValueStorage;
    use astraea::storage::LoadStoreValue;
    use astraea::storage::SQLiteStorage;
    use astraea::tree::{BlobDigest, VALUE_BLOB_MAX_LENGTH};
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    use std::sync::Arc;
    use tokio::runtime::Runtime;

    async fn check_open_file_content_buffer(
        buffer: &mut OpenFileContentBuffer,
        expected_content: &[u8],
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        let mut checked = 0;
        while checked < expected_content.len() {
            let max_read_size = VALUE_BLOB_MAX_LENGTH * 10;
            let read_result = buffer
                .read(
                    checked as u64,
                    std::cmp::min(max_read_size, expected_content.len() - checked),
                    storage.clone(),
                )
                .await;
            let read_bytes = read_result.unwrap();
            for byte in read_bytes.iter() {
                let expected_byte = expected_content[checked];
                assert_eq!(expected_byte, *byte);
                checked += 1;
            }
            assert_eq!(expected_content.len() as u64, buffer.size());
        }
        assert_eq!(expected_content.len(), checked);
    }

    fn make_in_memory_storage() -> Arc<(dyn LoadStoreValue + Send + Sync)> {
        Arc::new(InMemoryValueStorage::empty())
    }

    fn make_sqlite_in_memory_storage() -> Arc<(dyn LoadStoreValue + Send + Sync)> {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        Arc::new(SQLiteStorage::from(connection).unwrap())
    }

    fn read_large_file(
        b: &mut Bencher,
        is_buffer_hot: bool,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        let runtime = Runtime::new().unwrap();
        let original_content: Vec<u8> = Vec::new();
        let last_known_digest = BlobDigest::hash(&original_content);
        let last_known_digest_file_size = original_content.len();
        let file_size_in_blocks = 25;
        let write_buffer_in_blocks = file_size_in_blocks;
        let mut buffer = OpenFileContentBuffer::from_data(
            original_content.clone(),
            last_known_digest,
            last_known_digest_file_size as u64,
            write_buffer_in_blocks,
        )
        .unwrap();
        let mut small_rng = SmallRng::seed_from_u64(123);
        let file_size_in_bytes = file_size_in_blocks * VALUE_BLOB_MAX_LENGTH;
        let content = bytes::Bytes::from_iter((0..file_size_in_bytes).map(|_| small_rng.gen()));
        {
            let write_position = 0;
            let write_buffer = runtime.block_on(OptimizedWriteBuffer::from_bytes(
                write_position,
                content.clone(),
            ));
            let _write_result: () = runtime
                .block_on(buffer.write(write_position, write_buffer, storage.clone()))
                .unwrap();
        }
        assert_eq!(file_size_in_bytes as u64, buffer.size());
        {
            let store_result = runtime.block_on(buffer.store_all(storage.clone()));
            assert_eq!(Ok(StoreChanges::SomeChanges), store_result);
        }
        let (digest_status, size) = buffer.last_known_digest();
        assert!(digest_status.is_digest_up_to_date);
        assert_eq!(file_size_in_bytes as u64, size);

        b.iter(move || {
            if !is_buffer_hot {
                // reload from storage every time
                buffer = OpenFileContentBuffer::from_storage(
                    digest_status.last_known_digest,
                    size,
                    write_buffer_in_blocks,
                );
            }
            runtime.block_on(check_open_file_content_buffer(
                &mut buffer,
                &content,
                storage.clone(),
            ));
        });
    }

    #[bench]
    fn read_large_file_in_memory_storage_cold(b: &mut Bencher) {
        read_large_file(b, false, make_in_memory_storage());
    }

    #[bench]
    fn read_large_file_in_memory_storage_hot(b: &mut Bencher) {
        read_large_file(b, true, make_in_memory_storage());
    }

    #[bench]
    fn read_large_file_sqlite_in_memory_storage_cold(b: &mut Bencher) {
        read_large_file(b, false, make_sqlite_in_memory_storage());
    }

    #[bench]
    fn read_large_file_sqlite_in_memory_storage_hot(b: &mut Bencher) {
        read_large_file(b, true, make_sqlite_in_memory_storage());
    }

    #[test]
    fn read_large_file_test() {
        super::test::bench::run_once(|b| {
            Ok(read_large_file(b, false, make_sqlite_in_memory_storage()))
        })
        .unwrap();
    }
}
