#![no_main]
use astraea::{
    storage::{InMemoryValueStorage, LoadStoreValue},
    tree::{BlobDigest, VALUE_BLOB_MAX_LENGTH},
};
use dogbox_tree_editor::{OpenFileContentBuffer, OptimizedWriteBuffer};
use libfuzzer_sys::{fuzz_target, Corpus};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn compare_buffers(
    buffers: &mut [OpenFileContentBuffer],
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
) {
    assert_eq!(
        1,
        std::collections::BTreeSet::from_iter(buffers.iter().map(|buffer| buffer.size())).len()
    );
    let mut checked = 0;
    let expected_size = buffers[0].size();
    while checked < expected_size {
        let mut all_read_bytes = std::collections::BTreeSet::new();
        let position = checked;
        for read_result in buffers.iter_mut().map(|buffer| {
            buffer.read(
                position,
                (expected_size - position) as usize,
                storage.clone(),
            )
        }) {
            let read_bytes = read_result.await.unwrap();
            let is_expected_to_be_new = all_read_bytes.is_empty();
            if is_expected_to_be_new {
                checked += read_bytes.len() as u64;
            }
            let is_new = all_read_bytes.insert(read_bytes);
            assert_eq!(is_expected_to_be_new, is_new);
        }
    }
    assert_eq!(expected_size, checked);
}

#[derive(Serialize, Deserialize, Debug)]
enum FileOperation {
    Write {
        position: u32,
        data: Vec<u8>,
    },
    WriteRandomData {
        position: u32,
        size: u16, /*TODO: bigger writes*/
    },
    Nothing,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeneratedTest {
    operations: Vec<FileOperation>,
}

async fn write_to_all_buffers(
    buffers: &mut [OpenFileContentBuffer],
    position: u64,
    data: &bytes::Bytes,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
) {
    for buffer in buffers {
        buffer
            .write(
                position,
                OptimizedWriteBuffer::from_bytes(position, data.clone()).await,
                storage.clone(),
            )
            .await
            .unwrap();
    }
}

fn run_generated_test(test: GeneratedTest) -> Corpus {
    Runtime::new().unwrap().block_on(async move {
        let max_tested_file_size = VALUE_BLOB_MAX_LENGTH * 32;
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut small_rng = SmallRng::seed_from_u64(123);

        let initial_content: Vec<u8> = Vec::new();
        let last_known_digest = BlobDigest::hash(&initial_content);
        let last_known_digest_file_size = initial_content.len();
        let mut buffers: Vec<_> = std::iter::repeat_n((), 3)
            .map(|_| {
                OpenFileContentBuffer::from_data(
                    initial_content.clone(),
                    last_known_digest,
                    last_known_digest_file_size as u64,
                )
                .unwrap()
            })
            .collect();

        let storage = Arc::new(InMemoryValueStorage::empty());

        for operation in test.operations {
            // buffers[2] is recreated from storage before every operation.
            buffers[2] = OpenFileContentBuffer::from_storage(
                buffers[1].last_known_digest().0.last_known_digest,
                buffers[1].last_known_digest().1,
            );

            println!("{:?}", &operation);
            match &operation {
                FileOperation::Write { position, data } => {
                    if (*position as usize + data.len()) > max_tested_file_size {
                        return Corpus::Reject;
                    }
                    let data = bytes::Bytes::copy_from_slice(&data[..]);
                    let position = *position as u64;
                    write_to_all_buffers(&mut buffers, position, &data, storage.clone()).await;
                }
                FileOperation::WriteRandomData { position, size } => {
                    if (*position as usize + *size as usize) > max_tested_file_size {
                        return Corpus::Reject;
                    }
                    let data = bytes::Bytes::from_iter((0..*size).map(|_| small_rng.gen()));
                    let position = *position as u64;
                    write_to_all_buffers(&mut buffers, position, &data, storage.clone()).await;
                }
                FileOperation::Nothing => {}
            }

            // nothing special happens with buffers[0].

            // buffers[1] is forced into the storage after every operation.
            buffers[1].store_all(storage.clone()).await.unwrap();

            compare_buffers(&mut buffers, storage.clone()).await;
        }
        Corpus::Keep
    })
}

fuzz_target!(|data: &[u8]| -> libfuzzer_sys::Corpus {
    let generated_test = match postcard::from_bytes(data) {
        Ok(parsed) => parsed,
        Err(_) => return libfuzzer_sys::Corpus::Reject,
    };
    println!("{:?}", &generated_test);
    run_generated_test(generated_test)
});
