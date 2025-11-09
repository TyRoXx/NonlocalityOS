use crate::operating_system::{Directory, OperatingSystem};
use std::{collections::BTreeMap, future::Future, path::PathBuf, pin::Pin, sync::Arc};
use tempfile::TempDir;

struct FakeDirectory {
    host_location: PathBuf,
    is_locked: tokio::sync::Mutex<bool>,
}

impl FakeDirectory {
    pub fn new(host_location: PathBuf) -> Self {
        Self {
            host_location,
            is_locked: tokio::sync::Mutex::new(false),
        }
    }

    pub async fn is_locked(&self) -> bool {
        let is_locked_locked = self.is_locked.lock().await;
        *is_locked_locked
    }
}

#[async_trait::async_trait]
impl Directory for FakeDirectory {
    async fn path(&self) -> Result<std::path::PathBuf, std::io::Error> {
        Ok(self.host_location.clone())
    }

    async fn create(&self) -> Result<(), std::io::Error> {
        {
            let is_locked_locked = self.is_locked.lock().await;
            assert!(!*is_locked_locked, "Directory is locked");
        }
        std::fs::create_dir_all(&self.host_location)
    }

    async fn file_exists(&self, file_name: &std::ffi::OsStr) -> Result<bool, std::io::Error> {
        {
            let is_locked_locked = self.is_locked.lock().await;
            assert!(!*is_locked_locked, "Directory is locked");
        }
        let full_path = self.host_location.join(file_name);
        Ok(full_path.exists())
    }

    async fn remove_file(&self, file_name: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        {
            let is_locked_locked = self.is_locked.lock().await;
            assert!(!*is_locked_locked, "Directory is locked");
        }
        let full_path = self.host_location.join(file_name);
        std::fs::remove_file(full_path)
    }

    async fn lock(&self) -> Result<std::path::PathBuf, std::io::Error> {
        let mut is_locked_locked = self.is_locked.lock().await;
        assert!(!*is_locked_locked, "Directory is already locked");
        *is_locked_locked = true;
        Ok(self.host_location.clone())
    }

    async fn unlock(&self) -> Result<(), std::io::Error> {
        let mut is_locked_locked = self.is_locked.lock().await;
        assert!(*is_locked_locked, "Directory is not locked");
        *is_locked_locked = false;
        Ok(())
    }
}

struct TemporaryFakeDirectory {
    _temp: TempDir,
    fake_dir: FakeDirectory,
}

impl TemporaryFakeDirectory {
    pub fn new(temp: TempDir) -> Self {
        let fake_dir = FakeDirectory::new(temp.path().to_path_buf());
        Self {
            _temp: temp,
            fake_dir,
        }
    }
}

#[async_trait::async_trait]
impl Directory for TemporaryFakeDirectory {
    async fn path(&self) -> Result<std::path::PathBuf, std::io::Error> {
        todo!()
    }

    async fn create(&self) -> Result<(), std::io::Error> {
        todo!()
    }

    async fn file_exists(&self, file_name: &std::ffi::OsStr) -> Result<bool, std::io::Error> {
        self.fake_dir.file_exists(file_name).await
    }

    async fn remove_file(&self, file_name: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        self.fake_dir.remove_file(file_name).await
    }

    async fn lock(&self) -> Result<std::path::PathBuf, std::io::Error> {
        self.fake_dir.lock().await
    }

    async fn unlock(&self) -> Result<(), std::io::Error> {
        self.fake_dir.unlock().await
    }
}

pub type RunProcessFunction = dyn (Fn(
        &std::path::Path,
        &std::path::Path,
        &[&str],
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + Sync>>)
    + Send
    + Sync;

#[derive(PartialEq, Eq, Debug)]
pub enum FakeDirectoryEntry {
    File(u64),
    Directory,
}

fn enumerate_filesystem_recursively(
    directory: &std::path::Path,
) -> Result<BTreeMap<std::path::PathBuf, FakeDirectoryEntry>, std::io::Error> {
    let mut map: BTreeMap<std::path::PathBuf, FakeDirectoryEntry> = BTreeMap::new();
    for maybe_entry in std::fs::read_dir(directory)? {
        let entry = maybe_entry?;
        let file_name = entry.file_name();
        let file_type = entry.file_type()?;
        let fake_directory_entry = if file_type.is_dir() {
            FakeDirectoryEntry::Directory
        } else if file_type.is_file() {
            FakeDirectoryEntry::File(entry.metadata()?.len())
        } else {
            panic!(
                "Unsupported file type {:?} in fake operating system: {}",
                &file_type,
                entry.path().display()
            );
        };
        match fake_directory_entry {
            FakeDirectoryEntry::File(size) => {
                map.insert(
                    std::path::PathBuf::from(file_name),
                    FakeDirectoryEntry::File(size),
                );
            }
            FakeDirectoryEntry::Directory => {
                let subdirectory = directory.join(&file_name);
                let submap = enumerate_filesystem_recursively(&subdirectory)?;
                if submap.is_empty() {
                    map.insert(
                        std::path::PathBuf::from(&file_name),
                        FakeDirectoryEntry::Directory,
                    );
                }
                for (subname, entry) in submap.into_iter() {
                    map.insert(std::path::PathBuf::from(&file_name).join(subname), entry);
                }
            }
        }
    }
    Ok(map)
}

pub struct FakeOperatingSystem {
    open_directories: tokio::sync::Mutex<
        std::collections::HashMap<std::path::PathBuf, std::sync::Arc<FakeDirectory>>,
    >,
    host_root: TempDir,
    run_process_function: Box<RunProcessFunction>,
}

impl FakeOperatingSystem {
    pub fn new(run_process_function: Box<RunProcessFunction>) -> Result<Self, std::io::Error> {
        Ok(Self {
            open_directories: tokio::sync::Mutex::new(std::collections::HashMap::new()),
            host_root: tempfile::tempdir()?,
            run_process_function,
        })
    }

    pub async fn enumerate_filesystem(
        &self,
    ) -> Result<BTreeMap<std::path::PathBuf, FakeDirectoryEntry>, std::io::Error> {
        let open_directories_locked = self.open_directories.lock().await;
        for (path, directory) in open_directories_locked.iter() {
            assert!(
                !directory.is_locked().await,
                "Directory at {} is locked",
                path.display()
            );
        }
        enumerate_filesystem_recursively(self.host_root.path())
    }
}

#[async_trait::async_trait]
impl OperatingSystem for FakeOperatingSystem {
    async fn open_directory(
        &self,
        path: &std::path::Path,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error> {
        let mut lock = self.open_directories.lock().await;
        if let Some(directory) = lock.get(path) {
            Ok(directory.clone())
        } else {
            let host_location = self.host_root.path().join(path.strip_prefix("/").unwrap());
            let new_directory = std::sync::Arc::new(FakeDirectory::new(host_location));
            lock.insert(path.to_path_buf(), new_directory.clone());
            Ok(new_directory)
        }
    }

    async fn create_temporary_directory(
        &self,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error> {
        let temp_root = self.host_root.path().join("tmp");
        std::fs::create_dir_all(&temp_root)?;
        let temp = TempDir::new_in(temp_root)?;
        Ok(Arc::new(TemporaryFakeDirectory::new(temp)))
    }

    async fn run_process(
        &self,
        working_directory: &std::path::Path,
        executable: &std::path::Path,
        arguments: &[&str],
    ) -> std::io::Result<()> {
        Box::pin((self.run_process_function)(
            working_directory,
            executable,
            arguments,
        ))
        .await
    }
}
