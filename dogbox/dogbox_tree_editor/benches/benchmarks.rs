use astraea::in_memory_storage::InMemoryTreeStorage;
use astraea::load_cache_storage::LoadCache;
use astraea::sqlite_storage::SQLiteStorage;
use astraea::storage::LoadStoreTree;
use astraea::tree::HashedTree;
use astraea::tree::Tree;
use astraea::tree::TreeBlob;
use astraea::tree::TreeChildren;
use astraea::tree::TREE_BLOB_MAX_LENGTH;
use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion};
use dogbox_tree_editor::OpenFileContentBuffer;
use dogbox_tree_editor::OptimizedWriteBuffer;
use dogbox_tree_editor::StoreChanges;
use pretty_assertions::assert_eq;
use pretty_assertions::assert_ne;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

fn assert_equal_bytes(expected: &[u8], found: &[u8]) {
    assert!(expected == found);
}

async fn check_open_file_content_buffer(
    buffer: &mut OpenFileContentBuffer,
    expected_content: &[u8],
    max_read_size: usize,
    storage: Arc<dyn LoadStoreTree + Send + Sync>,
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
        assert_equal_bytes(
            &expected_content[checked..(checked + read_bytes.len())],
            &read_bytes,
        );
        checked += read_bytes.len();
        assert_eq!(expected_content.len() as u64, buffer.size());
    }
    assert_eq!(expected_content.len(), checked);
}

fn make_in_memory_storage() -> Arc<dyn LoadStoreTree + Send + Sync> {
    Arc::new(InMemoryTreeStorage::empty())
}

fn make_sqlite_in_memory_storage() -> Arc<dyn LoadStoreTree + Send + Sync> {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    Arc::new(SQLiteStorage::from(connection).unwrap())
}

fn read_large_file<S: Fn() -> Arc<dyn LoadStoreTree + Send + Sync>>(
    c: &mut Criterion,
    id: &str,
    is_buffer_hot: bool,
    max_read_size: usize,
    create_storage_for_iteration: S,
    runtime: Runtime,
) {
    // you may want to increase this number for actual benchmarking
    let file_size_in_blocks = 5;
    let file_size_in_bytes = file_size_in_blocks * TREE_BLOB_MAX_LENGTH;
    let mut group = c.benchmark_group("read_large_file");
    group.throughput(Throughput::Bytes(file_size_in_bytes as u64));
    group.bench_function(id, |b| {
        let original_content: Vec<u8> = Vec::new();
        let empty_file_reference = {
            let storage = create_storage_for_iteration();
            runtime
                .block_on(storage.store_tree(&HashedTree::from(Arc::new(Tree::new(
                    TreeBlob::try_from(bytes::Bytes::from(original_content.clone())).unwrap(),
                    TreeChildren::empty(),
                )))))
                .unwrap()
        };
        let last_known_digest_file_size = original_content.len();
        let write_buffer_in_blocks = file_size_in_blocks;
        let mut buffer = OpenFileContentBuffer::from_data(
            original_content.clone(),
            empty_file_reference,
            last_known_digest_file_size as u64,
            write_buffer_in_blocks,
        )
        .unwrap();
        let mut small_rng = SmallRng::seed_from_u64(123);
        let storage = create_storage_for_iteration();
        let content = bytes::Bytes::from_iter((0..file_size_in_bytes).map(|_| small_rng.random()));
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
        let store_result = runtime.block_on(buffer.store_all(storage.clone()));
        let (digest_status, size, reference) = buffer.last_known_digest();
        assert_eq!(size, file_size_in_bytes as u64);
        assert_eq!(
            Ok(StoreChanges::SomeChanges(reference.clone())),
            store_result
        );
        assert!(digest_status.is_digest_up_to_date);
        assert_eq!(file_size_in_bytes as u64, size);

        drop(storage);
        b.iter(|| {
            if !is_buffer_hot {
                // reload from storage every time
                buffer = OpenFileContentBuffer::from_storage(
                    reference.clone(),
                    size,
                    write_buffer_in_blocks,
                );
            }
            runtime.block_on(check_open_file_content_buffer(
                &mut buffer,
                &content,
                max_read_size,
                create_storage_for_iteration(),
            ));
            buffer.last_known_digest()
        });
    });
    group.finish();
}

const UNREALISTICALLY_LARGE_READ_SIZE: usize = usize::MAX;
const WINDOWS_WEBDAV_READ_SIZE: usize = 16384;

fn make_single_threaded_runtime() -> Runtime {
    Builder::new_current_thread().build().unwrap()
}

fn make_multi_threaded_runtime() -> Runtime {
    Builder::new_multi_thread().build().unwrap()
}

fn read_large_file_in_memory_storage_cold(c: &mut Criterion) {
    let storage = make_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_in_memory_storage_cold",
        false,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_in_memory_storage_hot(c: &mut Criterion) {
    let storage = make_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_in_memory_storage_hot",
        true,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_cold(c: &mut Criterion) {
    tracing_subscriber::fmt::init();
    let storage = make_sqlite_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_cold",
        false,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_cold_single_threaded(c: &mut Criterion) {
    let storage = make_sqlite_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_cold_single_threaded",
        false,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_single_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_cold_realistic_read_size(c: &mut Criterion) {
    let storage = make_sqlite_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_cold_realistic_read_size",
        false,
        WINDOWS_WEBDAV_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_cold_with_load_cache_hot(c: &mut Criterion) {
    let storage = Arc::new(LoadCache::new(make_sqlite_in_memory_storage(), 1000));
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_cold_with_load_cache_hot",
        false,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_cold_with_load_cache_cold(c: &mut Criterion) {
    let storage = make_sqlite_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_cold_with_load_cache_cold",
        false,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || Arc::new(LoadCache::new(storage.clone(), 1000)),
        make_multi_threaded_runtime(),
    );
}

fn read_large_file_sqlite_in_memory_storage_hot(c: &mut Criterion) {
    let storage = make_sqlite_in_memory_storage();
    read_large_file(
        c,
        "read_large_file_sqlite_in_memory_storage_hot",
        true,
        UNREALISTICALLY_LARGE_READ_SIZE,
        || storage.clone(),
        make_multi_threaded_runtime(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    read_large_file_in_memory_storage_cold(c);
    read_large_file_in_memory_storage_hot(c);
    read_large_file_sqlite_in_memory_storage_cold(c);
    read_large_file_sqlite_in_memory_storage_cold_single_threaded(c);
    read_large_file_sqlite_in_memory_storage_cold_realistic_read_size(c);
    read_large_file_sqlite_in_memory_storage_cold_with_load_cache_hot(c);
    read_large_file_sqlite_in_memory_storage_cold_with_load_cache_cold(c);
    read_large_file_sqlite_in_memory_storage_hot(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
