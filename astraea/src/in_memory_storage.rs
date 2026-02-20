use crate::{
    delayed_hashed_tree::DelayedHashedTree,
    storage::{
        CollectGarbage, CommitChanges, GarbageCollectionStats, LoadError, LoadStoreTree, LoadTree,
        StoreError, StoreTree, StrongDelayedHashedTree, StrongReference, StrongReferenceTrait,
    },
    tree::{BlobDigest, HashedTree},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Weak},
};
use tokio::sync::Mutex;

#[derive(Debug)]
struct InMemoryStrongReferenceImpl {}

impl StrongReferenceTrait for InMemoryStrongReferenceImpl {}

#[derive(Debug)]
struct InMemoryTreeEntry {
    tree: HashedTree,
    strong_reference_impl: Weak<InMemoryStrongReferenceImpl>,
    // just to keep them alive
    _children: Vec<StrongReference>,
}

#[derive(Debug)]
pub struct InMemoryTreeStorage {
    // TODO: automatic garbage collection when the number of trees exceeds a certain threshold
    reference_to_tree: Mutex<BTreeMap<BlobDigest, InMemoryTreeEntry>>,
}

impl InMemoryTreeStorage {
    pub fn empty() -> InMemoryTreeStorage {
        Self {
            reference_to_tree: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn clear(&self) {
        self.reference_to_tree.lock().await.clear();
    }

    pub async fn number_of_trees(&self) -> usize {
        self.reference_to_tree.lock().await.len()
    }

    pub async fn digests(&self) -> BTreeSet<BlobDigest> {
        self.reference_to_tree
            .lock()
            .await
            .keys()
            .copied()
            .collect()
    }
}

#[async_trait]
impl StoreTree for InMemoryTreeStorage {
    async fn store_tree(
        &self,
        tree: &HashedTree,
    ) -> std::result::Result<StrongReference, StoreError> {
        let mut children = Vec::new();
        for child_digest in tree.tree().children().references() {
            let child_tree = match self.load_tree(child_digest.digest()).await {
                Ok(success) => success,
                Err(error) => return Err(StoreError::TreeMissing(error)),
            };
            children.push(child_tree.reference().clone());
        }
        let mut lock = self.reference_to_tree.lock().await;
        let digest = *tree.digest();
        let impl_ = match lock.entry(digest) {
            std::collections::btree_map::Entry::Vacant(vacant_entry) => {
                let impl_ = Arc::new(InMemoryStrongReferenceImpl {});
                vacant_entry.insert(InMemoryTreeEntry {
                    tree: tree.clone(),
                    strong_reference_impl: Arc::<InMemoryStrongReferenceImpl>::downgrade(&impl_),
                    _children: children,
                });
                impl_
            }
            std::collections::btree_map::Entry::Occupied(mut occupied_entry) => occupied_entry
                .get()
                .strong_reference_impl
                .upgrade()
                .unwrap_or_else(|| {
                    let impl_ = Arc::new(InMemoryStrongReferenceImpl {});
                    occupied_entry.insert(InMemoryTreeEntry {
                        tree: occupied_entry.get().tree.clone(),
                        strong_reference_impl: Arc::<InMemoryStrongReferenceImpl>::downgrade(
                            &impl_,
                        ),
                        _children: children,
                    });
                    impl_
                }),
        };
        Ok(StrongReference::new(Some(impl_), digest))
    }
}

#[async_trait]
impl LoadTree for InMemoryTreeStorage {
    async fn load_tree(
        &self,
        reference: &BlobDigest,
    ) -> std::result::Result<StrongDelayedHashedTree, LoadError> {
        let mut lock = self.reference_to_tree.lock().await;
        match lock.get(reference) {
            Some(found) => match found.strong_reference_impl.upgrade() {
                Some(impl_) => Ok(StrongDelayedHashedTree::new(
                    StrongReference::new(Some(impl_), *reference),
                    DelayedHashedTree::immediate(found.tree.clone()),
                )),
                None => {
                    lock.remove(reference);
                    Err(LoadError::TreeNotFound(*reference))
                }
            },
            None => Err(LoadError::TreeNotFound(*reference)),
        }
    }

    async fn approximate_tree_count(&self) -> std::result::Result<u64, StoreError> {
        let lock = self.reference_to_tree.lock().await;
        Ok(lock.len() as u64)
    }
}

impl LoadStoreTree for InMemoryTreeStorage {}

#[async_trait]
impl CollectGarbage for InMemoryTreeStorage {
    async fn collect_some_garbage(
        &self,
    ) -> std::result::Result<GarbageCollectionStats, StoreError> {
        let mut lock = self.reference_to_tree.lock().await;
        let size_before = lock.len();
        lock.retain(|_digest, entry| entry.strong_reference_impl.upgrade().is_some());
        let size_after = lock.len();
        let trees_collected = size_before - size_after;
        Ok(GarbageCollectionStats {
            trees_collected: trees_collected as u64,
        })
    }
}

#[async_trait]
impl CommitChanges for InMemoryTreeStorage {
    async fn commit_changes(&self) -> Result<u64, StoreError> {
        Ok(0)
    }
}
