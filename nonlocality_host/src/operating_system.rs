use std::sync::Arc;
use tempfile::TempDir;
use tracing::{error, info};

#[async_trait::async_trait]
pub trait Directory {
    async fn create(&self) -> Result<(), std::io::Error>;
    async fn file_exists(&self, file_name: &std::ffi::OsStr) -> Result<bool, std::io::Error>;
    async fn remove_file(&self, file_name: &std::ffi::OsStr) -> Result<(), std::io::Error>;
    async fn lock(&self) -> Result<std::path::PathBuf, std::io::Error>;
    async fn unlock(&self) -> Result<(), std::io::Error>;
    async fn path(&self) -> Result<std::path::PathBuf, std::io::Error>;
}

#[async_trait::async_trait]
pub trait OperatingSystem {
    async fn open_directory(
        &self,
        path: &std::path::Path,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error>;

    async fn create_temporary_directory(
        &self,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error>;

    async fn run_process(
        &self,
        working_directory: &std::path::Path,
        executable: &std::path::Path,
        arguments: &[&str],
    ) -> std::io::Result<()>;
}

pub async fn file_exists(
    path: &std::path::Path,
    operating_system: &dyn OperatingSystem,
) -> Result<bool, std::io::Error> {
    let parent_directory = match path.parent() {
        Some(parent) => parent,
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No parent directory for path '{}'", path.display()),
            ))
        }
    };
    let directory = operating_system.open_directory(parent_directory).await?;
    directory
        .file_exists(path.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No file name for path '{}'", path.display()),
            )
        })?)
        .await
}

struct LinuxDirectory {
    location: std::path::PathBuf,
}

impl LinuxDirectory {
    pub fn new(location: std::path::PathBuf) -> Self {
        Self { location }
    }
}

#[async_trait::async_trait]
impl Directory for LinuxDirectory {
    async fn path(&self) -> Result<std::path::PathBuf, std::io::Error> {
        Ok(self.location.clone())
    }

    async fn create(&self) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(&self.location)
    }

    async fn file_exists(&self, file_name: &std::ffi::OsStr) -> Result<bool, std::io::Error> {
        let path = self.location.join(file_name);
        std::fs::exists(path)
    }

    async fn remove_file(&self, file_name: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        let path = self.location.join(file_name);
        std::fs::remove_file(path)
    }

    async fn lock(&self) -> Result<std::path::PathBuf, std::io::Error> {
        todo!()
    }

    async fn unlock(&self) -> Result<(), std::io::Error> {
        todo!()
    }
}

struct TemporaryLinuxDirectory {
    _temp: TempDir,
    linux_dir: LinuxDirectory,
}

impl TemporaryLinuxDirectory {
    pub fn new() -> Result<Self, std::io::Error> {
        let temp = TempDir::new()?;
        let linux_dir = LinuxDirectory::new(temp.path().to_path_buf());
        Ok(Self {
            _temp: temp,
            linux_dir,
        })
    }
}

#[async_trait::async_trait]
impl Directory for TemporaryLinuxDirectory {
    async fn path(&self) -> Result<std::path::PathBuf, std::io::Error> {
        todo!()
    }

    async fn create(&self) -> Result<(), std::io::Error> {
        todo!()
    }

    async fn file_exists(&self, file_name: &std::ffi::OsStr) -> Result<bool, std::io::Error> {
        self.linux_dir.file_exists(file_name).await
    }

    async fn remove_file(&self, file_name: &std::ffi::OsStr) -> Result<(), std::io::Error> {
        self.linux_dir.remove_file(file_name).await
    }

    async fn lock(&self) -> Result<std::path::PathBuf, std::io::Error> {
        self.linux_dir.lock().await
    }

    async fn unlock(&self) -> Result<(), std::io::Error> {
        self.linux_dir.unlock().await
    }
}

pub struct LinuxOperatingSystem {}

#[async_trait::async_trait]
impl OperatingSystem for LinuxOperatingSystem {
    async fn open_directory(
        &self,
        path: &std::path::Path,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error> {
        Ok(Arc::new(LinuxDirectory::new(path.to_path_buf())))
    }

    async fn create_temporary_directory(
        &self,
    ) -> Result<Arc<dyn Directory + Sync + Send>, std::io::Error> {
        Ok(Arc::new(TemporaryLinuxDirectory::new()?))
    }

    async fn run_process(
        &self,
        working_directory: &std::path::Path,
        executable: &std::path::Path,
        arguments: &[&str],
    ) -> std::io::Result<()> {
        info!("Run process: {} {:?}", executable.display(), arguments);
        let output = tokio::process::Command::new(executable)
            .args(arguments)
            .current_dir(working_directory)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .expect("start process");
        if output.status.success() {
            info!("Success");
            Ok(())
        } else {
            info!("Working directory: {}", working_directory.display());
            error!("Exit status: {}", output.status);
            let stdout = String::from_utf8_lossy(&output.stdout);
            if !stdout.is_empty() {
                error!("Standard output:\n{}", stdout.trim_end());
            }
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.is_empty() {
                error!("Standard error:\n{}", stderr.trim_end());
            }
            Err(std::io::Error::other(format!(
                "Process failed with exit code: {}",
                output.status
            )))
        }
    }
}
