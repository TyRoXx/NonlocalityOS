#![deny(warnings)]
use dogbox_blob_layer::BlobDigest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum DirectoryEntry {
    Directory(Box<DirectoryTree>),
    File(BlobDigest),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DirectoryTree {
    pub children: std::collections::BTreeMap<String, DirectoryEntry>,
}
