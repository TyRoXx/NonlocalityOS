use std::fmt::Write;
use std::{collections::HashMap, sync::Arc};

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
        println!("{}", &error_message);
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
) -> NumberOfErrors {
    /*println!(
        "{}> {} {}",
        working_directory.display(),
        executable.display(),
        arguments.join(" ")
    );*/
    let maybe_output = tokio::process::Command::new(executable)
        .args(arguments)
        .current_dir(&working_directory)
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
            println!(
                "Failed to spawn {} process: {}.",
                executable.display(),
                error
            );
            return NumberOfErrors(1);
        }
    };
    if output.status.success() {
        return NumberOfErrors(0);
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
    NumberOfErrors(1)
}

pub async fn run_cargo(
    working_directory: &std::path::Path,
    arguments: &[&str],
    environment_variables: &HashMap<String, String>,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_process_with_error_only_output(
        &working_directory,
        std::path::Path::new("cargo"),
        &arguments,
        &environment_variables,
        progress_reporter,
    )
    .await
}

pub async fn run_cargo_test(
    project: &std::path::Path,
    coverage_info_directory: &std::path::Path,
    with_coverage: bool,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let coverage_info_directory_str = coverage_info_directory
        .to_str()
        .expect("Tried to convert path to string");
    let environment_variables = if with_coverage {
        HashMap::from([
            ("RUSTFLAGS".to_string(), "-Cinstrument-coverage".to_string()),
            (
                "LLVM_PROFILE_FILE".to_string(),
                format!("{}/cargo-test-%p-%m.profraw", coverage_info_directory_str),
            ),
        ])
    } else {
        HashMap::new()
    };
    run_cargo(
        &project,
        &["test", "--verbose"],
        &environment_variables,
        progress_reporter,
    )
    .await
}

pub async fn run_cargo_build_for_host(
    project: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(
        &project,
        &["build", "--verbose", "--release"],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

pub async fn run_cargo_fmt(
    project: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(&project, &["fmt"], &HashMap::new(), progress_reporter).await
}
