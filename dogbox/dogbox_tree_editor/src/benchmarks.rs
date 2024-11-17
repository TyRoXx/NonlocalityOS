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
        max_read_size: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        assert_ne!(0, max_read_size);
        let mut checked = 0;
        while checked < expected_content.len() {
            let read_count = std::cmp::min(max_read_size, expected_content.len() - checked);
            let read_result = buffer
                .read(checked as u64, read_count, storage.clone())
                .await;
            let read_bytes = read_result.unwrap();
            assert_ne!(0, read_bytes.len());
            assert!(read_bytes.len() <= read_count);
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
        max_read_size: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) {
        let runtime = Runtime::new().unwrap();
        let original_content: Vec<u8> = Vec::new();
        let last_known_digest = BlobDigest::hash(&original_content);
        let last_known_digest_file_size = original_content.len();
        let file_size_in_blocks = 50;
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
                max_read_size,
                storage.clone(),
            ));
            buffer.last_known_digest()
        });
        b.bytes = size;
    }

    const UNREALISTICALLY_LARGE_READ_SIZE: usize = usize::MAX;
    const WINDOWS_WEBDAV_READ_SIZE: usize = 16384;

    #[bench]
    fn read_large_file_in_memory_storage_cold(b: &mut Bencher) {
        read_large_file(
            b,
            false,
            UNREALISTICALLY_LARGE_READ_SIZE,
            make_in_memory_storage(),
        );
    }

    #[bench]
    fn read_large_file_in_memory_storage_hot(b: &mut Bencher) {
        read_large_file(
            b,
            true,
            UNREALISTICALLY_LARGE_READ_SIZE,
            make_in_memory_storage(),
        );
    }

    #[bench]
    fn read_large_file_sqlite_in_memory_storage_cold(b: &mut Bencher) {
        read_large_file(
            b,
            false,
            UNREALISTICALLY_LARGE_READ_SIZE,
            make_sqlite_in_memory_storage(),
        );
    }

    #[bench]
    fn read_large_file_sqlite_in_memory_storage_cold_realistic_read_size(b: &mut Bencher) {
        read_large_file(
            b,
            false,
            WINDOWS_WEBDAV_READ_SIZE,
            make_sqlite_in_memory_storage(),
        );
    }

    #[bench]
    fn read_large_file_sqlite_in_memory_storage_hot(b: &mut Bencher) {
        read_large_file(
            b,
            true,
            UNREALISTICALLY_LARGE_READ_SIZE,
            make_sqlite_in_memory_storage(),
        );
    }
}
