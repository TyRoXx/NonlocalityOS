use crate::host::{add_executable_ending, HostOperatingSystem};
use crate::run::run_process_with_error_only_output;
use crate::{
    downloads,
    raspberrypi::confirm_directory,
    run::{NumberOfErrors, ReportProgress},
};
use std::{collections::HashMap, sync::Arc};

pub const WASIP1_TARGET: &str = "wasm32-wasip1";
pub const WASIP1_THREADS_TARGET: &str = "wasm32-wasip1-threads";

#[derive(Clone)]
pub struct WasiSdk {
    pub wasi_sdk: std::path::PathBuf,
    pub host: HostOperatingSystem,
}

pub async fn install_wasi_cpp_compiler(
    tools_directory: &std::path::Path,
    host: HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<WasiSdk>) {
    let compiler_name = "wasi-sdk-22";
    let wasi_sdk_operating_system_name = match host {
        HostOperatingSystem::WindowsAmd64 => "m-mingw",
        HostOperatingSystem::LinuxAmd64 => "linux",
    };
    let archive_file_name = format!(
        "{}.0-{}.tar.gz",
        compiler_name, wasi_sdk_operating_system_name
    );
    let download_url = format!(
        "https://github.com/WebAssembly/wasi-sdk/releases/download/{}/{}",
        &compiler_name, &archive_file_name
    );
    let unpacked_directory = tools_directory.join(format!(
        "{}.0.{}",
        compiler_name, wasi_sdk_operating_system_name
    ));
    match downloads::install_from_downloaded_archive(
        &download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Gz,
    ) {
        Ok(_) => {
            let weird_suffix = match host {
                HostOperatingSystem::WindowsAmd64 => "+m",
                HostOperatingSystem::LinuxAmd64 => "",
            };
            let sub_dir = unpacked_directory.join(format!("{}.0{}", compiler_name, weird_suffix));
            if confirm_directory(&sub_dir) {
                (
                    NumberOfErrors(0),
                    Some(WasiSdk {
                        wasi_sdk: sub_dir,
                        host: host,
                    }),
                )
            } else {
                (NumberOfErrors(1), None)
            }
        }
        Err(error) => {
            progress_reporter.log(&format!(
                "Could not download and unpack {}: {}",
                &download_url, error
            ));
            (NumberOfErrors(1), None)
        }
    }
}

pub async fn run_cargo_build_wasi_threads(
    project: &std::path::Path,
    wasi_sdk: &std::path::Path,
    host: &HostOperatingSystem,
    target_name: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    // With default compiler options, wasmtime fails to run an application using SQLite:
    // "unknown import: `env::__extenddftf2` has not been defined"
    // This has something to do with long double (?).
    // Solution from: https://github.com/nmandery/h3ron/blob/9d80a2bf9fd5c4f311e64ffd40087dfb41fa55a5/h3ron/examples/compile_to_wasi/Makefile
    let lib_dir = wasi_sdk.join("lib/clang/18/lib/wasip1");
    if !confirm_directory(&lib_dir) {
        return NumberOfErrors(1);
    }
    let lib_dir_str = lib_dir
        .to_str()
        .expect("Tried to convert a path to a string");
    let clang_exe = wasi_sdk
        .join("bin")
        .join(&add_executable_ending(host, "clang"));
    let clang_exe_str = clang_exe
        .to_str()
        .expect("Tried to convert a path to a string");
    run_process_with_error_only_output(&project, std::path::Path::new(
         "cargo"), &["build", "--verbose", "--release", "--target", target_name], &HashMap::from([
        ("CFLAGS".to_string(), "-pthread".to_string()),
        ("RUSTFLAGS".to_string(), format!("-C target-feature=-crt-static -C link-arg=-L{} -C link-arg=-lclang_rt.builtins-wasm32",
          lib_dir_str)),
        (format!("CC_{}", target_name), clang_exe_str.to_string()),
        // not sure if WASI_SDK_PATH does anything
        ("WASI_SDK_PATH".to_string(), wasi_sdk.to_str().expect("convert WASI SDK path to a string").to_string())
    ]), progress_reporter).await
}
