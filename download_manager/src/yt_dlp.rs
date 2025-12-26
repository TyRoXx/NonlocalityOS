use crate::Download;
use astraea::tree::BlobDigest;
use std::process::{ExitStatus, Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use tracing::{error, info};

pub struct YtDlpDownload {
    pub executable_path: std::path::PathBuf,
    pub output_directory: std::path::PathBuf,
}

async fn update_yt_dlp(
    executable_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::new(executable_path);
    cmd.arg("--update");
    // nightly for the latest compatibility fixes
    cmd.arg("--update-to");
    cmd.arg("nightly");
    cmd.stdout(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child
        .stdout
        .take()
        .expect("child did not have a handle to stdout");
    let mut reader = BufReader::new(stdout).lines();
    let (status_result, read_result): (std::io::Result<ExitStatus>, std::io::Result<()>) =
        tokio::join!(child.wait(), async move {
            while let Some(line) = reader.next_line().await? {
                info!("yt-dlp: {}", line);
            }
            Ok(())
        });
    let status = status_result?;
    info!("Child status was: {}", status);
    if status.success() {
        info!("yt-dlp updated successfully");
    } else {
        let message = format!("yt-dlp exited with {status}");
        error!("{message}");
        return Err(Box::from(message));
    }
    read_result?;
    Ok(())
}

pub fn hash_file(file_path: &std::path::Path) -> std::io::Result<BlobDigest> {
    use sha3::{Digest, Sha3_512};
    let mut file = std::fs::File::open(file_path)?;
    let mut hasher = Sha3_512::new();
    std::io::copy(&mut file, &mut hasher)?;
    let result = hasher.finalize();
    let mut digest_array = [0u8; 64];
    digest_array.copy_from_slice(&result);
    Ok(BlobDigest::new(&digest_array))
}

async fn download_with_yt_dlp(
    executable_path: &std::path::Path,
    output_directory: &std::path::Path,
    video_url: &str,
) -> Result<Vec<BlobDigest>, Box<dyn std::error::Error>> {
    // Let's hope the temp is on the same file system for renaming to work.
    // We can't put temp under output_directory because yt-dlp.exe does not work in Dropbox on Windows (access denied errors).
    let temp_dir = tempfile::tempdir()?;
    let mut cmd = Command::new(executable_path);
    cmd.arg("--no-overwrites");
    cmd.arg("--no-mtime");
    cmd.arg("--windows-filenames");
    cmd.arg("--progress");
    cmd.arg("--progress-delta");
    cmd.arg("1" /*second*/);
    cmd.arg("--newline");
    cmd.arg("--cookies-from-browser");
    cmd.arg("firefox");
    cmd.arg(video_url);
    cmd.arg("-o");
    // We don't use %(uploader) here because %(title) already contains the uploader on some sites like Twitter.
    cmd.arg(format!(
        "{}/%(title).120B %(upload_date)s [%(webpage_url_domain)s %(id)s].%(ext)s",
        temp_dir.path().display()
    ));

    // Specify that we want the command's standard output piped back to us.
    // By default, standard input/output/error will be inherited from the
    // current process (for example, this means that standard input will
    // come from the keyboard and standard output/error will go directly to
    // the terminal if this process is invoked from the command line).
    cmd.stdout(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child
        .stdout
        .take()
        .expect("child did not have a handle to stdout");
    let mut reader = BufReader::new(stdout).lines();
    let (status_result, read_result): (std::io::Result<ExitStatus>, std::io::Result<()>) =
        tokio::join!(child.wait(), async move {
            while let Some(line) = reader.next_line().await? {
                info!("yt-dlp: {}", line);
            }
            Ok(())
        });
    let status = status_result?;
    info!("Child status was: {}", status);
    if status.success() {
        info!("yt-dlp completed successfully for URL: {}", video_url);
    } else {
        error!("yt-dlp exited with {status} for URL: {}", video_url);
        return Err(Box::from(format!("yt-dlp failed for URL: {}", video_url)));
    }
    read_result?;

    let mut created_files = Vec::new();
    for entry_result in std::fs::read_dir(temp_dir.path())? {
        let entry = entry_result?;
        info!("yt-dlp created file: {}", entry.path().display());
        created_files.push(entry.path());
    }
    if created_files.is_empty() {
        let message = format!("yt-dlp did not create any files for URL: {}", video_url);
        error!(message);
        return Err(Box::from(message));
    };
    info!(
        "yt-dlp created {} files for URL: {}",
        created_files.len(),
        video_url
    );
    let mut result = Vec::new();
    for created_file in created_files {
        let digest = hash_file(&created_file)?;
        info!("File digest for {}: {}", created_file.display(), digest);
        let file_name = created_file
            .file_name()
            .expect("There has to be a file name for the entry after enumerating the directory.");
        let output_directory_destination_file = output_directory.join(file_name);
        if output_directory_destination_file.exists() {
            let existing_file_digest = hash_file(&output_directory_destination_file)?;
            if digest == existing_file_digest {
                info!(
                    "File {} already exists with matching digest",
                    output_directory_destination_file.display()
                );
                result.push(digest);
            } else {
                let message = format!(
                "Output file {} already exists, but its digest is different from the downloaded file ({}). Will not overwrite it.",
                output_directory_destination_file.display(),
                existing_file_digest
            );
                error!(message);
                return Err(Box::from(message));
            }
        } else {
            info!(
                "Renaming {} to {}",
                created_file.display(),
                output_directory_destination_file.display()
            );
            std::fs::rename(&created_file, &output_directory_destination_file)?;
            result.push(digest);
        }
    }
    Ok(result)
}

#[async_trait::async_trait]
impl Download for YtDlpDownload {
    async fn download(&self, url: &str) -> Result<Vec<BlobDigest>, Box<dyn std::error::Error>> {
        download_with_yt_dlp(&self.executable_path, &self.output_directory, url).await
    }
}

pub async fn prepare_yt_dlp(
    yt_dlp_executable_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if !yt_dlp_executable_path.exists() {
        return Err(Box::from(format!(
            "yt-dlp executable not found at {}. Please download from https://github.com/yt-dlp/yt-dlp/releases",
            yt_dlp_executable_path.display()
        )));
    }
    info!(
        "yt-dlp executable found at {}",
        yt_dlp_executable_path.display()
    );
    #[cfg(unix)]
    {
        let file = match std::fs::File::open(yt_dlp_executable_path) {
            Ok(file) => file,
            Err(e) => {
                return Err(Box::from(format!(
                    "Failed to open yt-dlp executable at {}: {e}",
                    yt_dlp_executable_path.display()
                )));
            }
        };
        use std::os::unix::fs::PermissionsExt;
        let metadata = match file.metadata() {
            Ok(metadata) => metadata,
            Err(e) => {
                return Err(Box::from(format!(
                    "Failed to get metadata for yt-dlp executable at {}: {e}",
                    yt_dlp_executable_path.display()
                )));
            }
        };
        let mut permissions = metadata.permissions();
        permissions.set_mode(permissions.mode() | 0o111);
        match file.set_permissions(permissions) {
            Ok(_) => {}
            Err(e) => {
                return Err(Box::from(format!(
                    "Failed to set permissions for yt-dlp executable at {}: {e}",
                    yt_dlp_executable_path.display()
                )));
            }
        };
    }
    update_yt_dlp(yt_dlp_executable_path).await
}
