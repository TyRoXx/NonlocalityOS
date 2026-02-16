extern crate test;
use crate::storage::StrongReference;
use crate::tree::{BlobDigest, HashedTree, Tree, TreeBlob, TREE_BLOB_MAX_LENGTH};
use crate::tree::{TreeChildren, TREE_MAX_CHILDREN};
use pretty_assertions::assert_eq;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;
use std::sync::Arc;
use test::Bencher;

fn make_test_tree() -> Tree {
    let mut small_rng = SmallRng::seed_from_u64(123);
    Tree::new(
        TreeBlob::try_from(bytes::Bytes::from_iter(
            (0..TREE_BLOB_MAX_LENGTH).map(|_| small_rng.gen()),
        ))
        .unwrap(),
        TreeChildren::empty(),
    )
}

fn calculate_digest_fixed<D>(b: &mut Bencher)
where
    D: sha3::Digest,
{
    let referenced = make_test_tree();
    b.iter(|| crate::tree::calculate_digest_fixed::<D>(&referenced));

    assert!(referenced.children().references().is_empty());
    b.bytes = referenced.blob().len() as u64;
}

fn calculate_digest_extendable<D>(b: &mut Bencher)
where
    D: core::default::Default + sha3::digest::Update + sha3::digest::ExtendableOutput,
{
    let referenced = make_test_tree();
    b.iter(|| crate::tree::calculate_digest_extendable::<D>(&referenced));

    assert!(referenced.children().references().is_empty());
    b.bytes = referenced.blob().len() as u64;
}

#[bench]
fn calculate_digest_sha3_224(b: &mut Bencher) {
    calculate_digest_fixed::<sha3::Sha3_224>(b);
}

#[bench]
fn calculate_digest_sha3_256(b: &mut Bencher) {
    calculate_digest_fixed::<sha3::Sha3_256>(b);
}

#[bench]
fn calculate_digest_sha3_384(b: &mut Bencher) {
    calculate_digest_fixed::<sha3::Sha3_384>(b);
}

#[bench]
fn calculate_digest_sha3_512(b: &mut Bencher) {
    calculate_digest_fixed::<sha3::Sha3_512>(b);
}

#[bench]
fn calculate_digest_shake_128(b: &mut Bencher) {
    calculate_digest_extendable::<sha3::Shake128>(b);
}

#[bench]
fn calculate_digest_shake_256(b: &mut Bencher) {
    calculate_digest_extendable::<sha3::Shake256>(b);
}

/*
    #[bench]
    fn calculate_digest_turbo_shake_128(b: &mut Bencher) {
        calculate_digest_extendable::<sha3::TurboShake128>(b);
    }

    #[bench]
    fn calculate_digest_turbo_shake_256(b: &mut Bencher) {
        calculate_digest_extendable::<sha3::TurboShake256>(b);
    }
*/

fn hashed_tree_from(
    b: &mut Bencher,
    blob_size: usize,
    reference_count: usize,
    expected_digest: &BlobDigest,
) {
    let mut small_rng = SmallRng::seed_from_u64(123);
    let tree = Arc::new(Tree::new(
        TreeBlob::try_from(bytes::Bytes::from_iter(
            (0..blob_size).map(|_| small_rng.gen()),
        ))
        .unwrap(),
        TreeChildren::try_from(
            std::iter::repeat_n((), reference_count)
                .map(|()| {
                    StrongReference::from_weak(BlobDigest::new(&{
                        let mut array: [u8; 64] = [0; 64];
                        small_rng.fill(&mut array);
                        array
                    }))
                })
                .collect(),
        )
        .expect("We are not benchmarking with too many child references"),
    ));
    b.iter(|| {
        let hashed_tree = HashedTree::from(tree.clone());
        assert_eq!(expected_digest, hashed_tree.digest());
        hashed_tree
    });
    b.bytes = tree.blob().len() as u64 + tree.children().references().len() as u64 * 64;
}

#[bench]
fn hashed_tree_from_max_blob_max_references(b: &mut Bencher) {
    hashed_tree_from(b, TREE_BLOB_MAX_LENGTH, TREE_MAX_CHILDREN, &BlobDigest::parse_hex_string(
            "e33bdf70688ecf9ba89f83e43e4bb7d494b982fe4da53658caa6ca41f822280fb9b50ecf98b65276efe81bce8db3f474a01156410fc33b6ea1b49ee02d4c0f77").unwrap());
}

#[bench]
fn hashed_tree_from_max_blob_no_references(b: &mut Bencher) {
    hashed_tree_from(b, TREE_BLOB_MAX_LENGTH, 0, &BlobDigest::parse_hex_string(
            "d15454a6735a0bb995b758a221381c539eb16e7653fb6b1b4975377187cfd4f026495f5d6ad44b93d4738210700d88da92e876049aaffac298f9b3547479818a").unwrap());
}

#[bench]
fn hashed_tree_from_min_blob_max_references(b: &mut Bencher) {
    hashed_tree_from(b, 0, TREE_MAX_CHILDREN, &BlobDigest::parse_hex_string(
            "42f238ba350c07533609966f5ff913c3ed0e03f7a3fdfe5bb9c2d28933b24089277c3a69812d6c2ded04ea68f7f32d6e76fc3df2f6aca867bfb4273afe0b1097").unwrap());
}

#[bench]
fn hashed_tree_from_min_blob_no_references(b: &mut Bencher) {
    hashed_tree_from(b, 0, 0, &BlobDigest::parse_hex_string(
            "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909").unwrap());
}
