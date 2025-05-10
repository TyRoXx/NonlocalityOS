use tracing::{error, info, warn};

use crate::{
    downloads,
    host::{add_executable_ending, HostOperatingSystem},
    run::{run_cargo, ReportProgress},
};
use std::{collections::HashMap, path::PathBuf, sync::Arc};

pub const RASPBERRY_PI_TARGET_NAME: &str = "aarch64-unknown-linux-gnu";

#[derive(Clone, Debug)]
pub struct RaspberryPi64Target {
    pub compiler_installation: std::path::PathBuf,
    pub host: HostOperatingSystem,
}

pub async fn install_raspberry_pi_cpp_compiler(
    tools_directory: &std::path::Path,
    host: HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> Option<RaspberryPi64Target> {
    // found this compiler on https://developer.arm.com/downloads/-/gnu-a
    let (compiler_name, download_url) = match host {
        HostOperatingSystem::WindowsAmd64 => ("gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu", "https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu.tar.xz?rev=06b6c36e428c48fda4b6d907f17308be^&hash=B36CC5C9544DCFCB2DB06FB46C8B8262"),
        HostOperatingSystem::LinuxAmd64 => ("gcc-arm-10.3-2021.07-x86_64-aarch64-none-linux-gnu", "https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/gcc-arm-10.3-2021.07-x86_64-aarch64-none-linux-gnu.tar.xz?rev=1cb9c51b94f54940bdcccd791451cec3&hash=B380A59EA3DC5FDC0448CA6472BF6B512706F8EC"),
    };
    let archive_file_name = format!("{compiler_name}.tar.xz");
    let unpacked_directory = tools_directory.join("raspberry_pi_compiler");
    match downloads::install_from_downloaded_archive(
        download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Xz,
    ) {
        Ok(_) => Some(RaspberryPi64Target {
            compiler_installation: unpacked_directory.join(compiler_name),
            host,
        }),
        Err(error) => {
            progress_reporter.log(&format!(
                "Could not download and unpack {}: {}",
                &download_url, error
            ));
            None
        }
    }
}

pub async fn run_cargo_build_for_raspberry_pi(
    working_directory: &std::path::Path,
    binary: &str,
    compiler_installation: &std::path::Path,
    host: &HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<PathBuf> {
    let target_name = RASPBERRY_PI_TARGET_NAME;
    let bin = compiler_installation.join("bin");
    let ar = bin.join(add_executable_ending(host, "aarch64-none-linux-gnu-ar"));
    if !confirm_regular_file(&ar) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Could not find ar at {}", ar.display()),
        ));
    }
    let ar_str = ar.to_str().expect("Tried to convert path to string");
    let compiler = bin.join(add_executable_ending(host, "aarch64-none-linux-gnu-gcc"));
    if !confirm_regular_file(&compiler) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Could not find GCC at {}", compiler.display()),
        ));
    }
    let compiler_str = compiler.to_str().expect("Tried to convert path to string");
    let library_path = compiler_installation
        .join("aarch64-none-linux-gnu")
        .join("libc")
        .join("lib64");
    if !confirm_directory(&library_path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Could not find library path at {}", library_path.display()),
        ));
    }
    let library_path_str = library_path
        .to_str()
        .expect("Tried to convert path to string");
    let environment_variables = HashMap::from([
        (format!("CC_{target_name}"), compiler_str.to_string()),
        (format!("AR_{target_name}"), ar_str.to_string()),
        ("LD_LIBRARY_PATH".to_string(), library_path_str.to_string()),
    ]);
    let binary_path = working_directory
        .join("target")
        .join(target_name)
        .join("release")
        .join(binary);
    if std::fs::exists(&binary_path)? {
        info!("Deleting existing binary at {}", binary_path.display());
        std::fs::remove_file(&binary_path)?;
    }
    run_cargo(
        working_directory,
        &[
            "build",
            "--verbose",
            "--target",
            target_name,
            "--config",
            &format!("target.{target_name}.linker='{compiler_str}'"),
            "--release",
            "--bin",
            binary,
        ],
        &environment_variables,
        progress_reporter,
    )
    .await?;
    if confirm_regular_file(&binary_path) {
        return Ok(binary_path);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "Could not find compiled binary at {}",
            binary_path.display()
        ),
    ))
}

fn confirm_regular_file(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_file() {
                info!("Confirmed regular file at {}", path.display());
                true
            } else {
                warn!(
                    "Expected file at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(err) => {
            error!(
                "Expected file at {}, but got an error: {}",
                path.display(),
                err
            );
            false
        }
    }
}

pub fn confirm_directory(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_dir() {
                info!("Confirmed directory at {}", path.display());
                true
            } else {
                warn!(
                    "Expected directory at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(err) => {
            error!(
                "Expected directory at {}, but got an error: {}",
                path.display(),
                err
            );
            false
        }
    }
}
