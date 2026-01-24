use crate::tree::{BlobDigest, HashedTree, Tree};
use std::sync::Arc;

// TODO: This enum and the DelayedHashedTree wrapper implement a performance optimization pattern.
// When should "delayed" be used vs "immediate"? What are the trade-offs?
// Is this pattern primarily for avoiding redundant hash calculations when loading from storage?
// Should there be documentation explaining when each variant is appropriate to use?
#[derive(Debug, Clone, PartialEq)]
enum DelayedHashedTreeAlternatives {
    Delayed(Arc<Tree>, BlobDigest),
    Immediate(HashedTree),
}

// TODO: Document this pattern! This appears to be an optimization to defer hash verification.
// When loading from trusted storage, Delayed can skip immediate hashing.
// When creating new trees, Immediate ensures the hash is already computed.
// What are the security implications of trusting the expected_digest in Delayed variant?
#[derive(Debug, Clone, PartialEq)]
pub struct DelayedHashedTree {
    alternatives: DelayedHashedTreeAlternatives,
}

impl DelayedHashedTree {
    pub fn delayed(tree: Arc<Tree>, expected_digest: BlobDigest) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Delayed(tree, expected_digest),
        }
    }

    pub fn immediate(tree: HashedTree) -> Self {
        Self {
            alternatives: DelayedHashedTreeAlternatives::Immediate(tree),
        }
    }

    //#[instrument(skip_all)]
    // TODO: Why does this return Option instead of Result? What does None signify - hash mismatch?
    // Should hash verification failure be an error type instead of None for better error handling?
    // When hash() returns None for the Delayed variant, is this a security issue or data corruption?
    pub fn hash(self) -> Option<HashedTree> {
        match self.alternatives {
            DelayedHashedTreeAlternatives::Delayed(tree, expected_digest) => {
                let hashed_tree = HashedTree::from(tree);
                if hashed_tree.digest() == &expected_digest {
                    Some(hashed_tree)
                } else {
                    None
                }
            }
            DelayedHashedTreeAlternatives::Immediate(hashed_tree) => Some(hashed_tree),
        }
    }
}
