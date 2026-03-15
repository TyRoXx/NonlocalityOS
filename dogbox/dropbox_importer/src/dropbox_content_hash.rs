use sha2::{Digest, Sha256};

// https://www.dropbox.com/developers/reference/content-hash?_tk=guides_lp&_ad=deepdive3&_camp=content_hash
const BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub struct DropboxContentHasher {
    overall_hasher: Sha256,
    current_block: Sha256,
    current_block_size: usize,
}

impl DropboxContentHasher {
    pub fn new() -> Self {
        Self {
            overall_hasher: Sha256::new(),
            current_block: Sha256::new(),
            current_block_size: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut remaining_data = data;
        while !remaining_data.is_empty() {
            if self.current_block_size == BLOCK_SIZE {
                let current_block = self.current_block.clone();
                self.current_block = Sha256::new();
                self.overall_hasher.update(current_block.finalize());
                self.current_block_size = 0;
            }

            let space_in_block = BLOCK_SIZE - self.current_block_size;
            let (head, rest) =
                remaining_data.split_at(::std::cmp::min(remaining_data.len(), space_in_block));
            self.current_block.update(head);

            self.current_block_size += head.len();
            remaining_data = rest;
        }
    }

    pub fn finalize(mut self) -> sha2::digest::Output<Sha256> {
        if self.current_block_size > 0 {
            self.overall_hasher.update(self.current_block.finalize());
        }
        self.overall_hasher.finalize()
    }
}
