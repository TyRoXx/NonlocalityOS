use astraea::{
    storage::{LoadTree, StoreTree, StrongReference, UpdateRoot},
    tree::TREE_BLOB_MAX_LENGTH,
};
use async_trait::async_trait;
use dogbox_tree_editor::segmented_blob::DEFAULT_MAX_CHILDREN_PER_TREE;
use serde::{Deserialize, Serialize};
use tracing::info;

// Chosen for efficient representation as a segmented blob tree and concatenation into bigger segmented blob trees.
// Not too small so that we don't have to make too many Dropbox API requests for chunks.
// Not too big so that downloads of large files are still possible via slow and unreliable network connections.
pub const DEFAULT_CHUNK_SIZE: u64 = (DEFAULT_MAX_CHILDREN_PER_TREE as u64)
    * (DEFAULT_MAX_CHILDREN_PER_TREE as u64)
    * (TREE_BLOB_MAX_LENGTH as u64);

// The cache is chunked to limit the impact of an individual Dropbox API download requests failing.
// This also shall make it easier to resume downloads of large files that are interrupted by network failures or other issues.
// Downloading these chunks in parallel is also possible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Sha256ChunkCacheKey {
    digest: [u8; 32],
    chunk_index: u32,
}

impl Sha256ChunkCacheKey {
    pub fn new(digest: [u8; 32], chunk_index: u32) -> Self {
        Self {
            digest,
            chunk_index,
        }
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub fn chunk_index(&self) -> u32 {
        self.chunk_index
    }
}

impl std::fmt::Display for Sha256ChunkCacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}]", hex::encode(self.digest), self.chunk_index)
    }
}

#[async_trait]
pub trait FileCache: Send + Sync {
    async fn require<'t>(
        &'t self,
        chunk_cache_key: &Sha256ChunkCacheKey,
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

    fn chunk_size(&self) -> u64;
}

pub type DownloadFileCallback<'a> = Box<
    dyn FnOnce() -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = std::io::Result<(StrongReference, u64)>>
                    + Send
                    + 'a,
            >,
        > + Send
        + 'a,
>;

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

    fn from_content(content: Self::Content, child: &Option<&StrongReference>) -> Self {
        Self {
            content_reference: (*child.unwrap()).clone(),
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

pub struct FileCacheMap<'a> {
    entries: tokio::sync::Mutex<
        sorted_tree::prolly_tree_editable_node::EditableNode<
            Sha256ChunkCacheKey,
            PersistableFileCacheEntry,
        >,
    >,
    load_tree: &'a (dyn LoadTree + Send + Sync),
    chunk_size: u64,
}

impl<'a> FileCacheMap<'a> {
    pub fn new(
        entries: sorted_tree::prolly_tree_editable_node::EditableNode<
            Sha256ChunkCacheKey,
            PersistableFileCacheEntry,
        >,
        load_tree: &'a (dyn LoadTree + Send + Sync),
        chunk_size: u64,
    ) -> Self {
        // TODO: use the type system for this check
        assert_ne!(chunk_size, 0);
        assert_eq!(chunk_size % TREE_BLOB_MAX_LENGTH as u64, 0);
        Self {
            entries: tokio::sync::Mutex::new(entries),
            load_tree,
            chunk_size,
        }
    }

    pub async fn number_of_entries(&self) -> Result<u64, Box<dyn std::error::Error>> {
        self.entries.lock().await.count(self.load_tree).await
    }

    pub async fn load(
        reference: &StrongReference,
        load_tree: &'a (dyn LoadTree + Send + Sync),
        chunk_size: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let entries = sorted_tree::prolly_tree_editable_node::EditableNode::load(
            reference.digest(),
            load_tree,
        )
        .await?;
        Ok(Self {
            entries: tokio::sync::Mutex::new(entries),
            load_tree,
            chunk_size,
        })
    }

    pub async fn save(
        &self,
        store_tree: &(dyn StoreTree + Send + Sync),
    ) -> Result<StrongReference, Box<dyn std::error::Error>> {
        self.entries.lock().await.save(store_tree).await
    }

    async fn require_impl(
        &'a self,
        chunk_cache_key: &Sha256ChunkCacheKey,
        download_file: DownloadFileCallback<'a>,
    ) -> std::io::Result<(StrongReference, u64)> {
        // TODO: don't hold the lock during the download
        let mut entries_locked = self.entries.lock().await;
        let maybe_found = entries_locked
            .find(chunk_cache_key, self.load_tree)
            .await
            .map_err(|e| {
                std::io::Error::other(format!("Failed to find key in download cache: {e}"))
            })?;
        match maybe_found {
            Some(entry) => {
                info!("Cache hit for chunk cache key {}", chunk_cache_key);
                Ok((entry.content_reference, entry.content_size))
            }
            None => {
                info!("Cache miss for chunk cache key {}", chunk_cache_key);
                let (content_reference, content_size) = download_file().await?;
                let new_entry = PersistableFileCacheEntry {
                    content_reference: content_reference.clone(),
                    content_size,
                };
                entries_locked
                    .insert(*chunk_cache_key, new_entry, self.load_tree)
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

#[async_trait]
impl FileCache for FileCacheMap<'_> {
    async fn require<'t>(
        &'t self,
        chunk_cache_key: &Sha256ChunkCacheKey,
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
        // We call this function because code coverage doesn't work for async_traits.
        self.require_impl(chunk_cache_key, download_file).await
    }

    fn chunk_size(&self) -> u64 {
        self.chunk_size
    }
}

pub struct PersistentFileCacheMap<'a> {
    original_cache: FileCacheMap<'a>,
    store_tree: &'a (dyn StoreTree + Send + Sync),
    update_root: &'a (dyn UpdateRoot + Send + Sync),
    root_name: String,
}

impl<'a> PersistentFileCacheMap<'a> {
    pub fn new(
        original_cache: FileCacheMap<'a>,
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

    async fn require_impl(
        &'a self,
        chunk_cache_key: &Sha256ChunkCacheKey,
        download_file: DownloadFileCallback<'a>,
    ) -> std::io::Result<(StrongReference, u64)> {
        let success = self
            .original_cache
            .require(chunk_cache_key, download_file)
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

#[async_trait]
impl FileCache for PersistentFileCacheMap<'_> {
    async fn require<'t>(
        &'t self,
        chunk_cache_key: &Sha256ChunkCacheKey,
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
        // We call this function because code coverage doesn't work for async_traits.
        self.require_impl(chunk_cache_key, download_file).await
    }

    fn chunk_size(&self) -> u64 {
        self.original_cache.chunk_size()
    }
}
