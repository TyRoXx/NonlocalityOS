use async_recursion::async_recursion;
use nonlocality_build_utils::coverage::delete_directory;
use nonlocality_build_utils::coverage::generate_coverage_report_with_grcov;
use nonlocality_build_utils::coverage::install_grcov;
use nonlocality_build_utils::host::detect_host_operating_system;
use nonlocality_build_utils::host::HostOperatingSystem;
use nonlocality_build_utils::raspberrypi::install_raspberry_pi_cpp_compiler;
use nonlocality_build_utils::raspberrypi::run_cargo_build_for_raspberry_pi;
use nonlocality_build_utils::raspberrypi::RaspberryPi64Target;
use nonlocality_build_utils::run::run_cargo;
use nonlocality_build_utils::run::run_cargo_build_for_target;
use nonlocality_build_utils::run::run_cargo_fmt;
use nonlocality_build_utils::run::run_cargo_test;
use nonlocality_build_utils::run::ConsoleErrorReporter;
use nonlocality_build_utils::run::NumberOfErrors;
use nonlocality_build_utils::run::ReportProgress;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
enum CargoBuildTarget {
    LinuxAmd64,
    RaspberryPi64(RaspberryPi64Target),
}

#[derive(Clone)]
struct Program {}

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
        CargoBuildTarget::LinuxAmd64 => {
            run_cargo_build_for_target(project, "x86_64-unknown-linux-gnu", progress_reporter).await
        }
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
    where_in_filesystem: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
    mode: AstraCommand,
    target: &CargoBuildTarget,
) -> NumberOfErrors {
    let mut tasks = Vec::new();
    match mode {
        AstraCommand::BuildRelease | AstraCommand::Deploy => {
            let target_clone = target.clone();
            let where_in_filesystem_clone = where_in_filesystem.to_path_buf();
            let progress_reporter_clone = progress_reporter.clone();
            tasks.push(tokio::spawn(async move {
                println!(
                    "Building {} for {:?}",
                    where_in_filesystem_clone.display(),
                    &target_clone
                );
                run_cargo_build(
                    &where_in_filesystem_clone,
                    &target_clone,
                    &progress_reporter_clone,
                )
                .await
            }));
        }
        AstraCommand::Test => {}
        AstraCommand::Coverage => {}
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
    mode: AstraCommand,
    target: &CargoBuildTarget,
) -> NumberOfErrors {
    let mut tasks = Vec::new();
    for entry in &description.entries {
        let subdirectory = where_in_filesystem.join(entry.0);
        let progress_reporter_clone = progress_reporter.clone();
        let mode_clone = mode.clone();
        let target_clone = target.clone();
        tasks.push(tokio::spawn(async move {
            build_program(
                &subdirectory,
                &progress_reporter_clone,
                mode_clone,
                &target_clone,
            )
            .await
        }));
    }
    join_all(tasks, progress_reporter).await
}

async fn install_tools(
    repository: &std::path::Path,
    host: HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<RaspberryPi64Target>) {
    let tools_directory = repository.join("tools");
    let (error_count_1, raspberry_pi) =
        install_raspberry_pi_cpp_compiler(&tools_directory, host, progress_reporter).await;
    (error_count_1, raspberry_pi)
}

#[derive(Debug, Clone, Copy)]
enum AstraCommand {
    BuildRelease,
    Test,
    Coverage,
    Deploy,
}

fn parse_command(input: &str) -> Option<AstraCommand> {
    match input {
        "build" => Some(AstraCommand::BuildRelease),
        "test" => Some(AstraCommand::Test),
        "coverage" => Some(AstraCommand::Coverage),
        "deploy" => Some(AstraCommand::Deploy),
        _ => None,
    }
}

async fn install_nextest(
    working_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(
        &working_directory,
        &["install", "cargo-nextest", "--locked"],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

async fn build(
    mode: AstraCommand,
    target: &CargoBuildTarget,
    repository: &std::path::Path,
    host: HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let (mut error_count, _maybe_raspberry_pi) =
        install_tools(repository, host, &progress_reporter).await;
    error_count += run_cargo_fmt(&repository, &progress_reporter).await;

    let coverage_directory = repository.join("coverage");
    let coverage_info_directory = coverage_directory.join("info");
    error_count += delete_directory(&coverage_info_directory);

    let with_coverage = match mode {
        AstraCommand::BuildRelease => false,
        AstraCommand::Test => false,
        AstraCommand::Coverage => {
            error_count += install_grcov(&repository, progress_reporter).await;
            true
        }
        AstraCommand::Deploy => false,
    };
    match mode {
        AstraCommand::BuildRelease => {}
        AstraCommand::Test | AstraCommand::Coverage => {
            error_count += install_nextest(&repository, &progress_reporter).await;
            error_count += run_cargo_test(
                &repository,
                &coverage_info_directory,
                with_coverage,
                &progress_reporter,
            )
            .await;
        }
        AstraCommand::Deploy => {}
    }

    let root = Directory {
        entries: BTreeMap::from([("nonlocality_host".to_string(), Program {})]),
    };

    error_count += build_recursively(&root, &repository, &progress_reporter, mode, target).await;

    match mode {
        AstraCommand::BuildRelease => {}
        AstraCommand::Test => {}
        AstraCommand::Coverage => {
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
        AstraCommand::Deploy => {}
    }
    error_count
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let started_at = std::time::Instant::now();
    let command_line_arguments: Vec<String> = std::env::args().collect();
    if command_line_arguments.len() != 3 {
        println!(
            "Two command line arguments required: [Path to the root of the repository] test|build|coverage|deploy"
        );
        return std::process::ExitCode::FAILURE;
    }
    let repository = std::env::current_dir()
        .unwrap()
        .join(&command_line_arguments[1]);
    println!("Repository: {}", repository.display());
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
        &CargoBuildTarget::LinuxAmd64,
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
