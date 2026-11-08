use criterion::Throughput;
use criterion::{criterion_group, criterion_main, Criterion};
use dropbox_importer::dropbox_content_hash::DropboxContentHasher;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use tokio::runtime::Builder;

fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::seed_from_u64(123);
    let mut data = vec![0u8; size];
    rng.fill(&mut data[..]);
    data
}

fn dropbox_content_hash_update(
    c: &mut Criterion,
    id: &str,
    data: &[u8],
    data_slicing: fn(&[u8]) -> Vec<&[u8]>,
) {
    let mut group = c.benchmark_group("dropbox_content_hash_update");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function(id, |b| {
        b.iter(|| {
            let mut hasher = DropboxContentHasher::new();
            let rt = Builder::new_multi_thread().build().unwrap();
            rt.block_on(hasher.update(&data_slicing(data)));
            hasher.finalize()
        });
    });
    group.finish();
}

fn criterion_benchmark(c: &mut Criterion) {
    let large_data =
        random_bytes(dropbox_importer::dropbox_content_hash::BLOCK_SIZE * 200 + 1234567);
    dropbox_content_hash_update(c, "large_data/one_piece", &large_data, |data| vec![data]);
    dropbox_content_hash_update(c, "large_data/tree_blob_pieces", &large_data, |data| {
        data.chunks(astraea::tree::TREE_BLOB_MAX_LENGTH).collect()
    });
    let one_block_data = random_bytes(dropbox_importer::dropbox_content_hash::BLOCK_SIZE);
    dropbox_content_hash_update(c, "small_data/one_block", &one_block_data, |data| {
        vec![data]
    });
    let empty_data = random_bytes(0);
    dropbox_content_hash_update(c, "small_data/empty", &empty_data, |data| vec![data]);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
