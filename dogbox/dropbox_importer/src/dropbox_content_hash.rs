use sha2::{Digest, Sha256};

// https://www.dropbox.com/developers/reference/content-hash?_tk=guides_lp&_ad=deepdive3&_camp=content_hash
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub struct DropboxContentHasher {
    overall_hasher: Sha256,
    current_block: Vec<u8>,
}

impl DropboxContentHasher {
    pub fn new() -> Self {
        Self {
            overall_hasher: Sha256::new(),
            current_block: Vec::with_capacity(BLOCK_SIZE),
        }
    }

    pub async fn update(&mut self, data: &[&[u8]]) {
        let mut block_hashes = Vec::new();

        for piece in data {
            let mut remaining_data = *piece;
            while !remaining_data.is_empty() {
                let space_in_block = BLOCK_SIZE - self.current_block.len();
                let (head, rest) =
                    remaining_data.split_at(::std::cmp::min(remaining_data.len(), space_in_block));
                self.current_block.extend_from_slice(head);
                remaining_data = rest;

                if self.current_block.len() == BLOCK_SIZE {
                    let block =
                        std::mem::replace(&mut self.current_block, Vec::with_capacity(BLOCK_SIZE));
                    block_hashes.push(tokio::task::spawn_blocking(move || Sha256::digest(block)));
                }
            }
        }

        for block_hash in block_hashes {
            self.overall_hasher
                .update(block_hash.await.expect("block hashing task failed"));
        }
    }

    pub fn finalize(mut self) -> sha2::digest::Output<Sha256> {
        if !self.current_block.is_empty() {
            self.overall_hasher
                .update(Sha256::digest(self.current_block));
        }
        self.overall_hasher.finalize()
    }
}

impl Default for DropboxContentHasher {
    fn default() -> Self {
        Self::new()
    }
}

pub fn format_dropbox_content_hash(hash: &sha2::digest::Output<Sha256>) -> String {
    hex::encode(hash)
}
