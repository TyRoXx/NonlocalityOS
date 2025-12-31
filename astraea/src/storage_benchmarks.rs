extern crate test;
use crate::{
    storage::{
        CollectGarbage, GarbageCollectionStats, LoadTree, SQLiteStorage, StoreTree, UpdateRoot,
    },
    tree::{BlobDigest, HashedTree, Tree, TreeBlob, TreeChildren, TREE_BLOB_MAX_LENGTH},
};
use pretty_assertions::assert_eq;
use std::sync::Arc;
use test::Bencher;
use tokio::runtime::Runtime;

fn sqlite_in_memory_store_tree_redundantly(b: &mut Bencher, tree_blob_size: usize) {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = SQLiteStorage::from(connection).unwrap();
    let stored_tree = HashedTree::from(Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from(random_bytes(tree_blob_size))).unwrap(),
        TreeChildren::empty(),
    )));
    let runtime = Runtime::new().unwrap();
    b.iter(|| {
        let reference = runtime.block_on(storage.store_tree(&stored_tree)).unwrap();
        assert_eq!(stored_tree.digest(), &reference);
        reference
    });
    b.bytes = tree_blob_size as u64;
}

#[bench]
fn sqlite_in_memory_store_tree_redundantly_small(b: &mut Bencher) {
    sqlite_in_memory_store_tree_redundantly(b, 100);
}

#[bench]
fn sqlite_in_memory_store_tree_redundantly_medium(b: &mut Bencher) {
    sqlite_in_memory_store_tree_redundantly(b, TREE_BLOB_MAX_LENGTH / 2);
}

#[bench]
fn sqlite_in_memory_store_tree_redundantly_large(b: &mut Bencher) {
    sqlite_in_memory_store_tree_redundantly(b, TREE_BLOB_MAX_LENGTH);
}

fn sqlite_in_memory_store_tree_newly(
    b: &mut Bencher,
    tree_blob_size: usize,
    reference_count: usize,
) {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    // count reduced to save time in the tests
    let store_count = 15;
    let stored_trees: Vec<_> = (0..store_count)
        .map(|_| {
            HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(bytes::Bytes::from_iter(
                    (0..tree_blob_size).map(|_| small_rng.gen()),
                ))
                .unwrap(),
                TreeChildren::try_from(
                    (0..reference_count)
                        .map(|_| BlobDigest::new(&small_rng.gen()))
                        .collect(),
                )
                .expect("We are not benchmarking with too many child references"),
            )))
        })
        .collect();
    let runtime = Runtime::new().unwrap();
    b.iter(|| {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        for stored_tree in &stored_trees {
            let reference = runtime.block_on(storage.store_tree(stored_tree)).unwrap();
            assert_eq!(stored_tree.digest(), &reference);
        }
        storage
    });
    b.bytes = store_count as u64 * (tree_blob_size as u64 + reference_count as u64 * 64);
}

#[bench]
fn sqlite_in_memory_store_tree_newly_small(b: &mut Bencher) {
    sqlite_in_memory_store_tree_newly(b, 100, 0);
}

#[bench]
fn sqlite_in_memory_store_tree_newly_large(b: &mut Bencher) {
    sqlite_in_memory_store_tree_newly(b, TREE_BLOB_MAX_LENGTH, 0);
}

#[bench]
fn sqlite_in_memory_store_tree_newly_only_refs(b: &mut Bencher) {
    sqlite_in_memory_store_tree_newly(b, 0, 100);
}

async fn generate_random_trees<T: StoreTree>(tree_count_in_database: u64, storage: &T) {
    for index in 0..tree_count_in_database {
        let stored_tree = HashedTree::from(Arc::new(Tree::new(
            TreeBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
            TreeChildren::empty(),
        )));
        let _reference = storage.store_tree(&stored_tree).await.unwrap();
    }
}

#[bench]
fn generate_random_trees_1000(b: &mut Bencher) {
    let tree_count_in_database = 1000;
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    b.iter(|| {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        SQLiteStorage::create_schema(&connection).unwrap();
        let storage = SQLiteStorage::from(connection).unwrap();
        runtime.block_on(generate_random_trees(tree_count_in_database, &storage));
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

fn sqlite_in_memory_load_and_hash_tree(b: &mut Bencher, tree_count_in_database: u64) {
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
        BlobDigest::parse_hex_string(concat!(
            "f4f60b9678a11ac75b4c28944111e29657976c7cc46050eb8c2b422f77a3cc99",
            "043054027fb3c041ed5c2195002bd24ca0d93e08d20e5ce9b54a9a16d9fd5beb"
        ))
        .unwrap(),
        reference
    );
    b.iter(|| {
        let loaded = runtime
            .block_on(storage.load_tree(&reference))
            .unwrap()
            .hash()
            .unwrap();
        assert_eq!(stored_tree.digest(), loaded.digest());
        loaded
    });
    b.bytes = stored_tree.tree().blob().len() as u64;
    assert_eq!(
        Ok(tree_count_in_database + 1),
        runtime.block_on(storage.approximate_tree_count())
    );
}

#[bench]
fn sqlite_in_memory_load_and_hash_tree_small_database(b: &mut Bencher) {
    sqlite_in_memory_load_and_hash_tree(b, 0);
}

#[bench]
fn sqlite_in_memory_load_and_hash_tree_large_database(b: &mut Bencher) {
    sqlite_in_memory_load_and_hash_tree(b, 10_000);
}

fn collect_garbage_nothing_to_collect(b: &mut Bencher, tree_count_in_database: u32) {
    let runtime = tokio::runtime::Builder::new_multi_thread().build().unwrap();
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    SQLiteStorage::create_schema(&connection).unwrap();
    let storage = Arc::new(SQLiteStorage::from(connection).unwrap());
    runtime.block_on(async {
        let mut previous_tree: Option<BlobDigest> = None;
        for index in 0..tree_count_in_database {
            let stored_tree = HashedTree::from(Arc::new(Tree::new(
                TreeBlob::try_from(bytes::Bytes::copy_from_slice(&index.to_be_bytes())).unwrap(),
                TreeChildren::try_from(match previous_tree.take() {
                    Some(digest) => vec![digest],
                    None => vec![],
                })
                .unwrap(),
            )));
            let digest = storage.store_tree(&stored_tree).await.unwrap();
            previous_tree = Some(digest);
        }
        storage
            .update_root("bench", &previous_tree.unwrap())
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

#[bench]
fn collect_garbage_nothing_to_collect_1(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 1);
}

#[bench]
fn collect_garbage_nothing_to_collect_1_000(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 1_000);
}

#[bench]
fn collect_garbage_nothing_to_collect_10_000(b: &mut Bencher) {
    collect_garbage_nothing_to_collect(b, 10_000);
}
