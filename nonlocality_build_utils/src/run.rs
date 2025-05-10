use std::fmt::Write;
use std::path::PathBuf;
use std::{collections::HashMap, sync::Arc};

use tracing::{error, info, span, Level};

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NumberOfErrors(pub u64);

impl std::ops::Add<NumberOfErrors> for NumberOfErrors {
    type Output = NumberOfErrors;

    fn add(self, rhs: NumberOfErrors) -> NumberOfErrors {
        let (sum, has_overflown) = u64::overflowing_add(self.0, rhs.0);
        assert!(!has_overflown);
        NumberOfErrors(sum)
    }
}

pub trait ReportProgress {
    fn log(&self, message: &str);
}

pub struct ConsoleErrorReporter {}

impl ReportProgress for ConsoleErrorReporter {
    fn log(&self, error_message: &str) {
        info!("{}", &error_message);
    }
}

impl std::ops::AddAssign for NumberOfErrors {
    fn add_assign(&mut self, rhs: NumberOfErrors) {
        *self = *self + rhs;
    }
}

pub async fn run_process_with_error_only_output(
    working_directory: &std::path::Path,
    executable: &std::path::Path,
    arguments: &[&str],
    environment_variables: &HashMap<String, String>,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let span = span!(Level::INFO, "run_process");
    let _enter = span.enter();
    info!(
        "Run process: {} {}",
        executable.display(),
        arguments.join(" "),
    );
    let maybe_output = tokio::process::Command::new(executable)
        .args(arguments)
        .current_dir(working_directory)
        .envs(environment_variables)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await;
    let output = match maybe_output {
        Ok(output) => output,
        Err(error) => {
            error!(
                "Failed to spawn {} (...) process: {}.",
                executable.display(),
                error
            );
            return Err(error);
        }
    };
    if output.status.success() {
        info!(
            "Process {} (...) finished successfully.",
            executable.display(),
        );
        return Ok(());
    }
    let mut message = String::from("");
    writeln!(message, "Executable: {}", executable.display()).unwrap();
    writeln!(message, "Arguments: {}", arguments.join(" ")).unwrap();
    writeln!(message, "Environment: {:?}", &environment_variables).unwrap();
    writeln!(
        message,
        "Working directory: {}",
        working_directory.display()
    )
    .unwrap();
    writeln!(message, "Exit status: {}", output.status).unwrap();
    writeln!(message, "Standard output:").unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    writeln!(message, "{}", &stdout).unwrap();
    writeln!(message, "Standard error:").unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    writeln!(message, "{}", &stderr).unwrap();
    progress_reporter.log(&message);
    error!("{}", &message);
    Err(std::io::Error::other(format!(
        "Process failed with exit code: {}",
        output.status
    )))
}

pub async fn run_cargo(
    working_directory: &std::path::Path,
    arguments: &[&str],
    environment_variables: &HashMap<String, String>,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    run_process_with_error_only_output(
        working_directory,
        std::path::Path::new("cargo"),
        arguments,
        environment_variables,
        progress_reporter,
    )
    .await
}

pub async fn run_cargo_test(
    project: &std::path::Path,
    coverage_info_directory: &std::path::Path,
    with_coverage: bool,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let coverage_info_directory_str = coverage_info_directory
        .to_str()
        .expect("Tried to convert path to string");
    let environment_variables = if with_coverage {
        HashMap::from([
            ("RUSTFLAGS".to_string(), "-Cinstrument-coverage".to_string()),
            (
                "LLVM_PROFILE_FILE".to_string(),
                format!("{coverage_info_directory_str}/cargo-test-%p-%m.profraw"),
            ),
        ])
    } else {
        HashMap::new()
    };
    run_cargo(
        project,
        &["nextest", "run"],
        &environment_variables,
        progress_reporter,
    )
    .await
}

pub async fn run_cargo_build_for_target(
    working_directory: &std::path::Path,
    target: &str,
    binary: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<PathBuf> {
    let binary_path = working_directory
        .join("target")
        .join(target)
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
            "--release",
            "--target",
            target,
            "--bin",
            binary,
        ],
        &HashMap::new(),
        progress_reporter,
    )
    .await?;
    if std::fs::metadata(&binary_path)?.is_file() {
        info!("Confirmed binary at {}", binary_path.display());
        Ok(binary_path)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "Could not find compiled binary at {}",
                binary_path.display()
            ),
        ))
    }
}

pub async fn run_cargo_fmt(
    project: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    run_cargo(project, &["fmt"], &HashMap::new(), progress_reporter).await
}
