use crate::{
    downloads,
    run::{run_cargo, NumberOfErrors, ReportProgress},
};
use std::{collections::HashMap, sync::Arc};

pub const RASPBERRY_PI_TARGET_NAME: &str = "aarch64-unknown-linux-gnu";

#[derive(Clone)]
pub struct RaspberryPi64Target {
    pub compiler_installation: std::path::PathBuf,
}

pub async fn install_raspberry_pi_cpp_compiler(
    tools_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<RaspberryPi64Target>) {
    // found this compiler on https://developer.arm.com/downloads/-/gnu-a
    let compiler_name = "gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu";
    let archive_file_name = format!("{}.tar.xz", compiler_name);
    let download_url = format!("https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/{}?rev=06b6c36e428c48fda4b6d907f17308be^&hash=B36CC5C9544DCFCB2DB06FB46C8B8262", &archive_file_name);
    let unpacked_directory = tools_directory.join("raspberry_pi_compiler");
    match downloads::install_from_downloaded_archive(
        &download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Xz,
    ) {
        Ok(_) => (
            NumberOfErrors(0),
            Some(RaspberryPi64Target {
                compiler_installation: unpacked_directory.join(compiler_name),
            }),
        ),
        Err(error) => {
            progress_reporter.log(&format!(
                "Could not download and unpack {}: {}",
                &download_url, error
            ));
            (NumberOfErrors(1), None)
        }
    }
}

pub async fn run_cargo_build_for_raspberry_pi(
    project: &std::path::Path,
    compiler_installation: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let target_name = RASPBERRY_PI_TARGET_NAME;
    let bin = compiler_installation.join("bin");
    let ar = bin.join("aarch64-none-linux-gnu-ar.exe");
    if !confirm_regular_file(&ar) {
        return NumberOfErrors(1);
    }
    let ar_str = ar.to_str().expect("Tried to convert path to string");
    let compiler = bin.join("aarch64-none-linux-gnu-gcc.exe");
    if !confirm_regular_file(&compiler) {
        return NumberOfErrors(1);
    }
    let compiler_str = compiler.to_str().expect("Tried to convert path to string");
    let library_path = compiler_installation
        .join("aarch64-none-linux-gnu")
        .join("libc")
        .join("lib64");
    if !confirm_directory(&library_path) {
        return NumberOfErrors(1);
    }
    let library_path_str = library_path
        .to_str()
        .expect("Tried to convert path to string");
    let environment_variables = HashMap::from([
        (format!("CC_{}", target_name), compiler_str.to_string()),
        (format!("AR_{}", target_name), ar_str.to_string()),
        ("LD_LIBRARY_PATH".to_string(), library_path_str.to_string()),
    ]);
    run_cargo(
        &project,
        &[
            "build",
            "--verbose",
            "--target",
            target_name,
            "--config",
            &format!("target.{}.linker='{}'", target_name, compiler_str),
            "--release",
        ],
        &environment_variables,
        progress_reporter,
    )
    .await
}

fn confirm_regular_file(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_file() {
                true
            } else {
                println!(
                    "Expected file at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(error) => {
            println!(
                "Expected file at {}, but got an error: {}",
                path.display(),
                error
            );
            false
        }
    }
}

pub fn confirm_directory(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_dir() {
                true
            } else {
                println!(
                    "Expected directory at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(error) => {
            println!(
                "Expected directory at {}, but got an error: {}",
                path.display(),
                error
            );
            false
        }
    }
}
