use astraea::{
    sqlite_storage::SQLiteStorage,
    storage::{
        CollectGarbage, GarbageCollectionStats, LoadTree, StoreTree, StrongReference, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use pretty_assertions::assert_eq;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn sqlite_in_memory_store_tree_redundantly(c: &mut Criterion, tree_blob_size: usize) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let stored_tree = HashedTree::from(Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from(random_bytes(tree_blob_size))).unwrap(),
        TreeChildren::empty(),
    )));
    let runtime = Runtime::new().unwrap();

    let mut group = c.benchmark_group(format!(
        "sqlite_in_memory_store_tree_redundantly {}",
        tree_blob_size
    ));
    group.throughput(Throughput::Bytes(tree_blob_size as u64));
    group.bench_function(
        format!("sqlite_in_memory_store_tree_redundantly {}", tree_blob_size),
        |b| {
            b.iter(|| {
                let reference = runtime.block_on(storage.store_tree(&stored_tree)).unwrap();
                assert_eq!(stored_tree.digest(), reference.digest());
                reference
            })
        },
    );
    group.finish();
}

fn sqlite_in_memory_store_tree_redundantly_small(c: &mut Criterion) {
    sqlite_in_memory_store_tree_redundantly(c, 100);
}

fn sqlite_in_memory_store_tree_redundantly_medium(c: &mut Criterion) {
    sqlite_in_memory_store_tree_redundantly(c, TREE_BLOB_MAX_LENGTH / 2);
}

fn sqlite_in_memory_store_tree_redundantly_large(c: &mut Criterion) {
    sqlite_in_memory_store_tree_redundantly(c, TREE_BLOB_MAX_LENGTH);
}

async fn generate_random_trees<T: StoreTree>(
    tree_count_in_database: u64,
    storage: &T,
) -> Option<StrongReference> {
    let mut previous_reference: Option<StrongReference> = None;
    for index in 0..tree_count_in_database {
        let stored_tree = HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
            match previous_reference.take() {
                Some(reference) => TreeChildren::try_from(vec![reference]).unwrap(),
                None => TreeChildren::empty(),
            },
        )));
        let reference = storage.store_tree(&stored_tree).await.unwrap();
        previous_reference = Some(reference);
    }
    previous_reference
}

fn generate_random_trees_1000(b: &mut Bencher) {
    let tree_count_in_database = 1000;
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    b.iter(|| {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        let _reference = runtime.block_on(generate_random_trees(tree_count_in_database, &storage));
        assert_eq!(
            Ok(tree_count_in_database),
            runtime.block_on(storage.approximate_tree_count())
        );
    });
}

fn random_bytes(len: usize) -> Vec<u8> {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    (0..len).map(|_| small_rng.gen()).collect()
}

fn sqlite_in_memory_load_and_hash_tree(c: &mut Criterion, tree_count_in_database: u64) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    runtime.block_on(generate_random_trees(tree_count_in_database, &storage));
    assert_eq!(
        Ok(tree_count_in_database),
        runtime.block_on(storage.approximate_tree_count())
    );
    let stored_tree = HashedTree::from(Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from(random_bytes(
            /*not too long because we don't just want to benchmark the digest function*/ 100,
        )))
        .unwrap(),
        TreeChildren::empty(),
    )));
    let reference = runtime.block_on(storage.store_tree(&stored_tree)).unwrap();
    assert_eq!(
        &BlobDigest::parse_hex_string(concat!(
            "f4f60b9678a11ac75b4c28944111e29657976c7cc46050eb8c2b422f77a3cc99",
            "043054027fb3c041ed5c2195002bd24ca0d93e08d20e5ce9b54a9a16d9fd5beb"
        ))
        .unwrap(),
        reference.digest()
    );
    let mut group = c.benchmark_group(format!(
        "sqlite_in_memory_load_and_hash_tree {}",
        tree_count_in_database
    ));
    group.throughput(Throughput::Bytes(stored_tree.tree().blob().len() as u64));
    group.bench_function(
        format!(
            "sqlite_in_memory_load_and_hash_tree {}",
            tree_count_in_database
        ),
        |b| {
            b.iter(|| {
                let loaded = runtime
                    .block_on(storage.load_tree(reference.digest()))
                    .unwrap()
                    .hash()
                    .unwrap();
                assert_eq!(&stored_tree, loaded.hashed_tree());
                loaded
            })
        },
    );
    group.finish();
    assert_eq!(
        Ok(tree_count_in_database + 1),
        runtime.block_on(storage.approximate_tree_count())
    );
}

fn sqlite_in_memory_load_and_hash_tree_small_database(c: &mut Criterion) {
    sqlite_in_memory_load_and_hash_tree(c, 0);
}

fn sqlite_in_memory_load_and_hash_tree_large_database(c: &mut Criterion) {
    sqlite_in_memory_load_and_hash_tree(c, 10_000);
}

fn collect_garbage_nothing_to_collect(b: &mut Bencher, tree_count_in_database: u32) {
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = Arc::new(SQLiteStorage::from(connection).unwrap());
    runtime.block_on(async {
        let mut previous_reference: Option<StrongReference> = None;
        assert_ne!(0, tree_count_in_database);
        for index in 0..tree_count_in_database {
            let stored_tree = HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
                TreeChildren::try_from(match previous_reference.take() {
                    Some(reference) => vec![reference],
                    None => vec![],
                })
                .unwrap(),
            )));
            let reference = storage.store_tree(&stored_tree).await.unwrap();
            previous_reference = Some(reference);
        }
        storage
            .update_root("bench", &previous_reference.unwrap())
            .await
            .unwrap();
    });
    b.iter(|| {
        let storage = storage.clone();
        runtime.block_on(async move {
            let stats = storage.collect_some_garbage().await.unwrap();
            assert_eq!(GarbageCollectionStats { trees_collected: 0 }, stats);
        });
    });
}

fn collect_garbage_nothing_to_collect_1(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 1);
}

fn collect_garbage_nothing_to_collect_1_000(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 1_000);
}

fn collect_garbage_nothing_to_collect_10_000(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 10_000);
}

fn criterion_benchmark(c: &mut Criterion) {
    sqlite_in_memory_store_tree_redundantly_small(c);
    sqlite_in_memory_store_tree_redundantly_medium(c);
    sqlite_in_memory_store_tree_redundantly_large(c);
    c.bench_function("generate_random_trees_1000", generate_random_trees_1000);
    sqlite_in_memory_load_and_hash_tree_small_database(c);
    sqlite_in_memory_load_and_hash_tree_large_database(c);
    c.bench_function(
        "collect_garbage_nothing_to_collect_1",
        collect_garbage_nothing_to_collect_1,
    );
    c.bench_function(
        "collect_garbage_nothing_to_collect_1_000",
        collect_garbage_nothing_to_collect_1_000,
    );
    c.bench_function(
        "collect_garbage_nothing_to_collect_10_000",
        collect_garbage_nothing_to_collect_10_000,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
