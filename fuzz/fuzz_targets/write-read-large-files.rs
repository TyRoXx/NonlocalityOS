#![no_main]
use astraea::{
    storage::{InMemoryValueStorage, StoreValue},
    tree::{HashedValue, Value, ValueBlob, VALUE_BLOB_MAX_LENGTH},
};
use dogbox_tree_editor::{OpenFileContentBuffer, OptimizedWriteBuffer};
use libfuzzer_sys::{fuzz_target, Corpus};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::Arc};
use tokio::runtime::Runtime;

struct BufferState {
    storage: Arc<InMemoryValueStorage>,
    buffer: OpenFileContentBuffer,
}

impl BufferState {
    fn new(storage: Arc<InMemoryValueStorage>, buffer: OpenFileContentBuffer) -> Self {
        Self { storage, buffer }
    }
}

async fn compare_buffers(buffers: &mut [BufferState]) {
    assert_eq!(
        1,
        std::collections::BTreeSet::from_iter(buffers.iter().map(|buffer| buffer.buffer.size()))
            .len()
    );
    let mut checked = 0;
    let expected_size = buffers[0].buffer.size();
    while checked < expected_size {
        let mut all_read_bytes = std::collections::BTreeSet::new();
        let position = checked;
        for read_result in buffers.iter_mut().map(|buffer| {
            buffer.buffer.read(
                position,
                (expected_size - position) as usize,
                buffer.storage.clone(),
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
    WriteWholeBlockOfRandomData {
        block_index: u16,
    },
    CopyBlock {
        from_block_index: u16,
        to_block_index: u16,
    },
    SaveToStorage,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeneratedTest {
    operations: Vec<FileOperation>,
}

async fn write_to_all_buffers(buffers: &mut [BufferState], position: u64, data: &bytes::Bytes) {
    for buffer in buffers {
        buffer
            .buffer
            .write(
                position,
                OptimizedWriteBuffer::from_bytes(position, data.clone()).await,
                buffer.storage.clone(),
            )
            .await
            .unwrap();
    }
}

async fn read_from_all_buffers(
    buffers: &mut [BufferState],
    position: u64,
    count: usize,
) -> Option<bytes::Bytes> {
    let mut all_data_read = BTreeSet::new();
    for buffer in buffers {
        let data_read = buffer
            .buffer
            .read(position, count, buffer.storage.clone())
            .await
            .unwrap();
        assert!(data_read.len() <= count);
        all_data_read.insert(data_read);
    }
    assert_eq!(1, all_data_read.len());
    let read = all_data_read.into_iter().next().unwrap();
    if read.len() == count {
        Some(read)
    } else {
        None
    }
}

async fn save_all_buffers(buffers: &mut [BufferState]) {
    let mut status = BTreeSet::new();
    for buffer in buffers {
        buffer
            .buffer
            .store_all(buffer.storage.clone())
            .await
            .unwrap();
        status.insert(buffer.buffer.last_known_digest());
    }
    assert_eq!(1, status.len());
}

fn run_generated_test(test: GeneratedTest) -> Corpus {
    Runtime::new().unwrap().block_on(async move {
        let max_tested_file_size = VALUE_BLOB_MAX_LENGTH * 32;
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut small_rng = SmallRng::seed_from_u64(123);

        let initial_content: Vec<u8> = Vec::new();
        let last_known_digest_file_size = initial_content.len();
        let mut buffers: Vec<_> = std::iter::repeat_n((), 3)
            .map(|_| {
                let storage = Arc::new(InMemoryValueStorage::empty());
                let last_known_digest = storage
                    .store_value(&HashedValue::from(Arc::new(Value::new(
                        ValueBlob::empty(),
                        Vec::new(),
                    ))))
                    .unwrap()
                    .digest;
                BufferState::new(
                    storage,
                    OpenFileContentBuffer::from_data(
                        initial_content.clone(),
                        last_known_digest,
                        last_known_digest_file_size as u64,
                    )
                    .unwrap(),
                )
            })
            .collect();

        for operation in test.operations {
            // buffers[2] is recreated from storage before every operation.
            {
                let storage = buffers[2].storage.clone();
                buffers[2].buffer.store_all(storage).await.unwrap();
                let (digest, size) = buffers[2].buffer.last_known_digest();
                buffers[2].buffer =
                    OpenFileContentBuffer::from_storage(digest.last_known_digest, size);
            }

            println!("{:?}", &operation);
            match &operation {
                FileOperation::Write { position, data } => {
                    if (*position as usize + data.len()) > max_tested_file_size {
                        return Corpus::Reject;
                    }
                    let data = bytes::Bytes::copy_from_slice(&data[..]);
                    let position = *position as u64;
                    write_to_all_buffers(&mut buffers, position, &data).await;
                }
                FileOperation::WriteRandomData { position, size } => {
                    if (*position as usize + *size as usize) > max_tested_file_size {
                        return Corpus::Reject;
                    }
                    let data = bytes::Bytes::from_iter((0..*size).map(|_| small_rng.gen()));
                    let position = *position as u64;
                    write_to_all_buffers(&mut buffers, position, &data).await;
                }
                FileOperation::Nothing => {}
                FileOperation::WriteWholeBlockOfRandomData { block_index } => {
                    if ((*block_index as u64 + 1) * VALUE_BLOB_MAX_LENGTH as u64)
                        > max_tested_file_size as u64
                    {
                        return Corpus::Reject;
                    }
                    let data = bytes::Bytes::from_iter(
                        (0..VALUE_BLOB_MAX_LENGTH).map(|_| small_rng.gen()),
                    );
                    let position = *block_index as u64 * VALUE_BLOB_MAX_LENGTH as u64;
                    write_to_all_buffers(&mut buffers, position, &data).await;
                }
                FileOperation::CopyBlock {
                    from_block_index,
                    to_block_index,
                } => {
                    if ((*from_block_index as u64 + 1) * VALUE_BLOB_MAX_LENGTH as u64)
                        > max_tested_file_size as u64
                    {
                        return Corpus::Reject;
                    }
                    if ((*to_block_index as u64 + 1) * VALUE_BLOB_MAX_LENGTH as u64)
                        > max_tested_file_size as u64
                    {
                        return Corpus::Reject;
                    }
                    let read_position = *from_block_index as u64 * VALUE_BLOB_MAX_LENGTH as u64;
                    let maybe_data =
                        read_from_all_buffers(&mut buffers, read_position, VALUE_BLOB_MAX_LENGTH)
                            .await;
                    match maybe_data {
                        Some(data) => {
                            let write_position =
                                *to_block_index as u64 * VALUE_BLOB_MAX_LENGTH as u64;
                            write_to_all_buffers(&mut buffers, write_position, &data).await;
                        }
                        None => {}
                    }
                }
                FileOperation::SaveToStorage => {
                    save_all_buffers(&mut buffers).await;
                }
            }

            // nothing special happens with buffers[0].

            // buffers[1] is forced into the storage after every operation.
            {
                let storage = buffers[1].storage.clone();
                buffers[1].buffer.store_all(storage).await.unwrap();
            }

            compare_buffers(&mut buffers).await;
        }

        save_all_buffers(&mut buffers).await;
        compare_buffers(&mut buffers).await;
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
