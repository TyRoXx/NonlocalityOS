use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    tree::{BlobDigest, Tree, TreeBlob, TreeChildren},
};
use bytes::Bytes;
use std::sync::Arc;

#[test]
fn delayed_hashed_tree_inconsistent() {
    let tree_blob = TreeBlob::try_from(Bytes::from("test")).unwrap();
    let tree = Tree::new(tree_blob, TreeChildren::empty());
    let delayed_tree = DelayedHashedTree::delayed(
       Arc::new(tree),
        BlobDigest::parse_hex_string(
            "f0140e314ee38d4472393680e7a72a81abb36b134b467d90ea943b7aa1ea03bf2323bc1a2df91f7230a225952e162f6629cf435e53404e9cdd727a2d94e4f909",
        )
        .unwrap(),
    );
    assert!(delayed_tree.hash().is_none());
}
