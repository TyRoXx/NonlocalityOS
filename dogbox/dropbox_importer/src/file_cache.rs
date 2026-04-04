use crate::Sha256Digest;
use astraea::storage::{LoadTree, StoreTree, StrongReference, UpdateRoot};
use async_trait::async_trait;
use tracing::info;

#[async_trait]
pub trait FileCache: Send + Sync {
    async fn require<'t>(
        &'t self,
        dropbox_content_hash: &Sha256Digest,
        download_file: Box<
            dyn FnOnce() -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = std::io::Result<(StrongReference, u64)>>
                            + Send
                            + 't,
                    >,
                > + Send
                + 't,
        >,
    ) -> std::io::Result<(StrongReference, u64)>;
}

pub type Sha256CacheKey = [u8; 32];

#[derive(Debug, Clone)]
pub struct PersistableFileCacheEntry {
    content_reference: StrongReference,
    content_size: u64,
}

impl sorted_tree::sorted_tree::NodeValue for PersistableFileCacheEntry {
    type Content = u64;

    fn has_child(_content: &Self::Content) -> bool {
        true
    }

    fn from_content(content: Self::Content, child: &Option<StrongReference>) -> Self {
        Self {
            content_reference: child.clone().unwrap(),
            content_size: content,
        }
    }

    fn to_content(&self) -> Self::Content {
        self.content_size
    }

    fn get_reference(&self) -> Option<StrongReference> {
        Some(self.content_reference.clone())
    }
}

pub struct PersistableFileCache<'a> {
    entries: tokio::sync::Mutex<
        sorted_tree::prolly_tree_editable_node::EditableNode<
            Sha256CacheKey,
            PersistableFileCacheEntry,
        >,
    >,
    load_tree: &'a (dyn LoadTree + Send + Sync),
}

impl<'a> PersistableFileCache<'a> {
    pub fn new(
        entries: sorted_tree::prolly_tree_editable_node::EditableNode<
            Sha256CacheKey,
            PersistableFileCacheEntry,
        >,
        load_tree: &'a (dyn LoadTree + Send + Sync),
    ) -> Self {
        Self {
            entries: tokio::sync::Mutex::new(entries),
            load_tree,
        }
    }

    pub async fn number_of_entries(&self) -> Result<u64, Box<dyn std::error::Error>> {
        self.entries.lock().await.count(self.load_tree).await
    }

    pub async fn load(
        reference: &StrongReference,
        load_tree: &'a (dyn LoadTree + Send + Sync),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let entries = sorted_tree::prolly_tree_editable_node::EditableNode::load(
            reference.digest(),
            load_tree,
        )
        .await?;
        Ok(Self {
            entries: tokio::sync::Mutex::new(entries),
            load_tree,
        })
    }

    pub async fn save(
        &self,
        store_tree: &(dyn StoreTree + Send + Sync),
    ) -> Result<StrongReference, Box<dyn std::error::Error>> {
        self.entries.lock().await.save(store_tree).await
    }
}

#[async_trait]
impl FileCache for PersistableFileCache<'_> {
    async fn require<'t>(
        &'t self,
        dropbox_content_hash: &Sha256Digest,
        download_file: Box<
            dyn FnOnce() -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = std::io::Result<(StrongReference, u64)>>
                            + Send
                            + 't,
                    >,
                > + Send
                + 't,
        >,
    ) -> std::io::Result<(StrongReference, u64)> {
        let cache_key: [u8; 32] = *dropbox_content_hash
            .as_array::<32>()
            .expect("Sha256Digest should always be 32 bytes");
        // TODO: don't hold the lock during the download
        let mut entries_locked = self.entries.lock().await;
        let maybe_found = entries_locked
            .find(&cache_key, self.load_tree)
            .await
            .map_err(|e| {
                std::io::Error::other(format!("Failed to find key in download cache: {e}"))
            })?;
        match maybe_found {
            Some(entry) => {
                info!("Cache hit for content hash {}", hex::encode(cache_key));
                Ok((entry.content_reference, entry.content_size))
            }
            None => {
                info!("Cache miss for content hash {}", hex::encode(cache_key));
                let (content_reference, content_size) = download_file().await?;
                let new_entry = PersistableFileCacheEntry {
                    content_reference: content_reference.clone(),
                    content_size,
                };
                entries_locked
                    .insert(cache_key, new_entry, self.load_tree)
                    .await
                    .map_err(|e| {
                        std::io::Error::other(format!(
                            "Failed to insert key into download cache: {e}"
                        ))
                    })?;
                Ok((content_reference, content_size))
            }
        }
    }
}

pub struct PersistentFileCache<'a> {
    original_cache: PersistableFileCache<'a>,
    store_tree: &'a (dyn StoreTree + Send + Sync),
    update_root: &'a (dyn UpdateRoot + Send + Sync),
    root_name: String,
}

impl<'a> PersistentFileCache<'a> {
    pub fn new(
        original_cache: PersistableFileCache<'a>,
        store_tree: &'a (dyn StoreTree + Send + Sync),
        update_root: &'a (dyn UpdateRoot + Send + Sync),
        root_name: String,
    ) -> Self {
        Self {
            original_cache,
            store_tree,
            update_root,
            root_name,
        }
    }

    pub async fn number_of_entries(&self) -> Result<u64, Box<dyn std::error::Error>> {
        self.original_cache.number_of_entries().await
    }
}

#[async_trait]
impl FileCache for PersistentFileCache<'_> {
    async fn require<'t>(
        &'t self,
        dropbox_content_hash: &Sha256Digest,
        download_file: Box<
            dyn FnOnce() -> std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = std::io::Result<(StrongReference, u64)>>
                            + Send
                            + 't,
                    >,
                > + Send
                + 't,
        >,
    ) -> std::io::Result<(StrongReference, u64)> {
        let success = self
            .original_cache
            .require(dropbox_content_hash, download_file)
            .await?;
        // TODO: only save and update root if the cache was modified (i.e. if it was a cache miss)
        let saved = self
            .original_cache
            .save(self.store_tree)
            .await
            .map_err(|e| {
                std::io::Error::other(format!("Failed to save file cache after download: {e}"))
            })?;
        self.update_root
            .update_root(&self.root_name, &saved)
            .await
            .map_err(|e| {
                std::io::Error::other(format!(
                    "Failed to update root after saving file cache: {e}"
                ))
            })?;
        Ok(success)
    }
}
