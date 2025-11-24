use tracing::{error, info};

#[cfg(test)]
mod main_tests;

fn is_file_located_in_directory(
    file_path: &std::path::Path,
    directory_path: &std::path::Path,
) -> std::io::Result<bool> {
    match file_path.canonicalize() {
        Ok(canonical_file_path) => match directory_path.canonicalize() {
            Ok(canonical_directory_path) => {
                Ok(canonical_file_path.starts_with(&canonical_directory_path))
            }
            Err(e) => {
                error!(
                    "Failed to canonicalize directory path {}: {e}",
                    directory_path.display()
                );
                Err(e)
            }
        },
        Err(e) => {
            error!(
                "Failed to canonicalize file path {}: {e}",
                file_path.display()
            );
            Err(e)
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<std::process::ExitCode> {
    tracing_subscriber::fmt::init();
    let executable_path = std::env::current_exe().expect("Failed to get current executable path");
    let working_directory =
        std::env::current_dir().expect("Failed to get current working directory");
    info!(
        "Download Manager started. Executable path: {}, Working directory: {}",
        executable_path.display(),
        working_directory.display()
    );
    if !is_file_located_in_directory(&executable_path, &working_directory)? {
        info!("For simplicity sake, the executable is expected to be located in the current working directory.");
        error!(
            "Executable path {} is not located within the working directory {}",
            executable_path.display(),
            working_directory.display()
        );
        return Ok(std::process::ExitCode::from(1));
    }
    todo!()
}
