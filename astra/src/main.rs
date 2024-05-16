#![deny(warnings)]
use async_recursion::async_recursion;
use nonlocality_build_utils::coverage::delete_directory;
use nonlocality_build_utils::coverage::generate_coverage_report_with_grcov;
use nonlocality_build_utils::coverage::install_grcov;
use nonlocality_build_utils::raspberrypi::detect_host_operating_system;
use nonlocality_build_utils::raspberrypi::install_raspberry_pi_cpp_compiler;
use nonlocality_build_utils::raspberrypi::run_cargo_build_for_raspberry_pi;
use nonlocality_build_utils::raspberrypi::RaspberryPi64Target;
use nonlocality_build_utils::run::run_cargo_build_for_host;
use nonlocality_build_utils::run::run_cargo_fmt;
use nonlocality_build_utils::run::run_cargo_test;
use nonlocality_build_utils::run::ConsoleErrorReporter;
use nonlocality_build_utils::run::NumberOfErrors;
use nonlocality_build_utils::run::ReportProgress;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone)]
enum CargoBuildTarget {
    Host,
    RaspberryPi64(RaspberryPi64Target),
}

#[derive(Clone)]
struct Program {
    pub targets: Vec<CargoBuildTarget>,
}

impl Program {
    pub fn host() -> Program {
        Program {
            targets: vec![CargoBuildTarget::Host],
        }
    }

    pub fn host_and_pi(pi: RaspberryPi64Target) -> Program {
        Program {
            targets: vec![CargoBuildTarget::Host, CargoBuildTarget::RaspberryPi64(pi)],
        }
    }

    pub fn other() -> Program {
        Program { targets: vec![] }
    }
}

#[derive(Clone)]
struct Directory {
    entries: BTreeMap<String, Program>,
}

async fn run_cargo_build(
    project: &std::path::Path,
    target: &CargoBuildTarget,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    match target {
        CargoBuildTarget::Host => run_cargo_build_for_host(project, progress_reporter).await,
        CargoBuildTarget::RaspberryPi64(pi) => {
            run_cargo_build_for_raspberry_pi(
                &project,
                &pi.compiler_installation,
                &pi.host,
                progress_reporter,
            )
            .await
        }
    }
}

async fn build_program(
    program: &Program,
    where_in_filesystem: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
    mode: CargoBuildMode,
) -> NumberOfErrors {
    let mut tasks = Vec::new();
    match mode {
        CargoBuildMode::BuildRelease => {
            for target in &program.targets {
                let target_clone = target.clone();
                let where_in_filesystem_clone = where_in_filesystem.to_path_buf();
                let progress_reporter_clone = progress_reporter.clone();
                tasks.push(tokio::spawn(async move {
                    run_cargo_build(
                        &where_in_filesystem_clone,
                        &target_clone,
                        &progress_reporter_clone,
                    )
                    .await
                }));
            }
        }
        CargoBuildMode::Test => {}
        CargoBuildMode::Coverage => {}
    }
    join_all(tasks, progress_reporter).await
}

async fn join_all(
    tasks: Vec<tokio::task::JoinHandle<NumberOfErrors>>,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let mut error_count = NumberOfErrors(0);
    for entry in tasks {
        let result = entry.await;
        match result {
            Ok(errors) => {
                error_count += errors;
            }
            Err(error) => {
                progress_reporter.log(&format!("Failed to join a spawned task: {}", error))
            }
        }
    }
    error_count
}

#[async_recursion]
async fn build_recursively(
    description: &Directory,
    where_in_filesystem: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
    mode: CargoBuildMode,
) -> NumberOfErrors {
    let mut tasks = Vec::new();
    for entry in &description.entries {
        let subdirectory = where_in_filesystem.join(entry.0);
        let directory_entry = entry.1.clone();
        let progress_reporter_clone = progress_reporter.clone();
        let mode_clone = mode.clone();
        tasks.push(tokio::spawn(async move {
            build_program(
                &directory_entry,
                &subdirectory,
                &progress_reporter_clone,
                mode_clone,
            )
            .await
        }));
    }
    join_all(tasks, progress_reporter).await
}

async fn install_tools(
    repository: &std::path::Path,
    host: nonlocality_build_utils::raspberrypi::HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<RaspberryPi64Target>) {
    let tools_directory = repository.join("tools");
    let (error_count_1, raspberry_pi) =
        install_raspberry_pi_cpp_compiler(&tools_directory, host, progress_reporter).await;
    (error_count_1, raspberry_pi)
}

#[derive(Debug, Clone, Copy)]
enum CargoBuildMode {
    BuildRelease,
    Test,
    Coverage,
}

fn parse_command(input: &str) -> Option<CargoBuildMode> {
    match input {
        "build" => Some(CargoBuildMode::BuildRelease),
        "test" => Some(CargoBuildMode::Test),
        "coverage" => Some(CargoBuildMode::Coverage),
        _ => None,
    }
}

async fn build(
    mode: CargoBuildMode,
    repository: &std::path::Path,
    host: nonlocality_build_utils::raspberrypi::HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let (mut error_count, maybe_raspberry_pi) =
        install_tools(repository, host, &progress_reporter).await;
    error_count += run_cargo_fmt(&repository, &progress_reporter).await;

    let coverage_directory = repository.join("coverage");
    let coverage_info_directory = coverage_directory.join("info");
    error_count += delete_directory(&coverage_info_directory);

    let with_coverage = match mode {
        CargoBuildMode::BuildRelease => false,
        CargoBuildMode::Test => false,
        CargoBuildMode::Coverage => {
            error_count += install_grcov(&repository, progress_reporter).await;
            true
        }
    };
    let example_applications = repository.join("example_applications");
    match mode {
        CargoBuildMode::BuildRelease => {
            error_count += run_cargo_build_for_host(
                &example_applications.join("example_cluster"),
                progress_reporter,
            )
            .await;
        }
        CargoBuildMode::Test | CargoBuildMode::Coverage => {
            error_count += run_cargo_test(
                &repository,
                &coverage_info_directory,
                with_coverage,
                &progress_reporter,
            )
            .await;
            error_count += run_cargo_test(
                &example_applications,
                &coverage_info_directory,
                with_coverage,
                progress_reporter,
            )
            .await;
        }
    }

    let root = Directory {
        entries: BTreeMap::from([
            ("admin_tool".to_string(), Program::host()),
            ("astra".to_string(), Program::other()),
            ("management_interface".to_string(), Program::other()),
            (
                MANAGEMENT_SERVICE_NAME.to_string(),
                match maybe_raspberry_pi {
                    Some(raspberry_pi) => Program::host_and_pi(raspberry_pi),
                    None => Program::host(),
                },
            ),
            ("nonlocality_build_utils".to_string(), Program::other()),
            ("nonlocality_env".to_string(), Program::other()),
        ]),
    };

    error_count += build_recursively(&root, &repository, &progress_reporter, mode).await;

    match mode {
        CargoBuildMode::BuildRelease => {}
        CargoBuildMode::Test => {}
        CargoBuildMode::Coverage => {
            let coverage_report_directory = coverage_directory.join("report");
            error_count += delete_directory(&coverage_report_directory);
            error_count += generate_coverage_report_with_grcov(
                &repository,
                &coverage_info_directory,
                &coverage_report_directory,
                &progress_reporter,
            )
            .await;
        }
    }
    error_count
}

const MANAGEMENT_SERVICE_NAME: &str = "management_service";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let started_at = std::time::Instant::now();
    let command_line_arguments: Vec<String> = std::env::args().collect();
    if command_line_arguments.len() != 3 {
        println!(
            "Two command line arguments required: [Path to the root of the repository] test|build"
        );
        return std::process::ExitCode::FAILURE;
    }
    let repository = std::env::current_dir()
        .unwrap()
        .join(&command_line_arguments[1]);
    let command_input = &command_line_arguments[2];
    let command = match parse_command(command_input) {
        Some(success) => success,
        None => {
            println!("Unknown command: {}", command_input);
            return std::process::ExitCode::FAILURE;
        }
    };
    println!("Command: {:?}", &command);
    let progress_reporter: Arc<dyn ReportProgress + Send + Sync> =
        Arc::new(ConsoleErrorReporter {});

    let host_operating_system = detect_host_operating_system();
    let error_count = build(
        command,
        &repository,
        host_operating_system,
        &progress_reporter,
    )
    .await;

    let build_duration = started_at.elapsed();
    println!("Duration: {:?}", build_duration);

    match error_count.0 {
        0 => std::process::ExitCode::SUCCESS,
        _ => {
            println!("{} errors.", error_count.0);
            std::process::ExitCode::FAILURE
        }
    }
}
