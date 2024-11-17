extern crate test;

#[cfg(test)]
mod tests {
    use super::test::Bencher;
    use crate::tree::{
        BlobDigest, HashedValue, Reference, TypeId, TypedReference, Value, ValueBlob,
        VALUE_BLOB_MAX_LENGTH,
    };
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    use std::sync::Arc;

    fn make_test_value() -> Value {
        let mut small_rng = SmallRng::seed_from_u64(123);
        Value::new(
            ValueBlob::try_from(bytes::Bytes::from_iter(
                (0..VALUE_BLOB_MAX_LENGTH).map(|_| small_rng.gen()),
            ))
            .unwrap(),
            vec![],
        )
    }

    fn calculate_digest_fixed<D>(b: &mut Bencher)
    where
        D: sha3::Digest,
    {
        let referenced = make_test_value();
        b.iter(|| crate::tree::calculate_digest_fixed::<D>(&referenced));

        assert!(referenced.references().is_empty());
        b.bytes = referenced.blob().len() as u64;
    }

    fn calculate_digest_extendable<D>(b: &mut Bencher)
    where
        D: core::default::Default + sha3::digest::Update + sha3::digest::ExtendableOutput,
    {
        let referenced = make_test_value();
        b.iter(|| crate::tree::calculate_digest_extendable::<D>(&referenced));

        assert!(referenced.references().is_empty());
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

    fn hashed_value_from(
        b: &mut Bencher,
        blob_size: usize,
        reference_count: usize,
        expected_digest: &BlobDigest,
    ) {
        let mut small_rng = SmallRng::seed_from_u64(123);
        let value = Arc::new(Value::new(
            ValueBlob::try_from(bytes::Bytes::from_iter(
                (0..blob_size).map(|_| small_rng.gen()),
            ))
            .unwrap(),
            std::iter::repeat_n((), reference_count)
                .map(|()| {
                    TypedReference::new(
                        TypeId(0),
                        Reference::new(BlobDigest::new(&{
                            let mut array: [u8; 64] = [0; 64];
                            small_rng.fill(&mut array);
                            array
                        })),
                    )
                })
                .collect(),
        ));
        b.iter(|| {
            let hashed_value = HashedValue::from(value.clone());
            assert_eq!(expected_digest, hashed_value.digest());
            hashed_value
        });
        b.bytes = value.blob().len() as u64 + value.references().len() as u64 * 64;
    }

    #[bench]
    fn hashed_value_from_max_blob_max_references(b: &mut Bencher) {
        hashed_value_from(b, VALUE_BLOB_MAX_LENGTH, 1000, &BlobDigest::parse_hex_string(
            "9af05cec4e85264b70e5f72494fd0a6e75f928b0dce7733c15433e5461c784cef636ad2d644ce594e1747c5c0ec7360d394d9be92f5eaef647d8bd6807609fdd").unwrap());
    }

    #[bench]
    fn hashed_value_from_max_blob_no_references(b: &mut Bencher) {
        hashed_value_from(b, VALUE_BLOB_MAX_LENGTH, 0, &BlobDigest::parse_hex_string(
            "23f3c29d5ead1d624ce6a64c730d6bb84acd6f9e6a51d411e189d396825ae4e393cdf18ddbe5a23b820c975f9efaa96d25cbfa14af369f5665fce583b44abc25").unwrap());
    }

    #[bench]
    fn hashed_value_from_min_blob_max_references(b: &mut Bencher) {
        hashed_value_from(b, 0, 1000, &BlobDigest::parse_hex_string(
            "e3bf9bdb9faad7419c7f99817269416d9bda1f2280df659ce1d978f9b894c4043f50eb21754babb577f5ab009531ac394d7b06cd43560ba5f1a0bbb3191004bc").unwrap());
    }

    #[bench]
    fn hashed_value_from_min_blob_no_references(b: &mut Bencher) {
        hashed_value_from(b, 0, 0, &BlobDigest::parse_hex_string(
            "a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26").unwrap());
    }
}
