#![deny(warnings)]
use async_recursion::async_recursion;
use nonlocality_build_utils::coverage::delete_directory;
use nonlocality_build_utils::coverage::generate_coverage_report_with_grcov;
use nonlocality_build_utils::coverage::install_grcov;
use nonlocality_build_utils::install::deploy;
use nonlocality_build_utils::install::MANAGEMENT_SERVICE_NAME;
use nonlocality_build_utils::raspberrypi::RASPBERRY_PI_TARGET_NAME;
use nonlocality_build_utils::run::run_cargo;
use nonlocality_build_utils::run::run_cargo_build_for_host;
use nonlocality_build_utils::run::run_cargo_fmt;
use nonlocality_build_utils::run::run_cargo_test;
use nonlocality_build_utils::run::ConsoleErrorReporter;
use nonlocality_build_utils::run::NumberOfErrors;
use nonlocality_build_utils::run::ReportProgress;
use nonlocality_build_utils::wasi::install_wasi_cpp_compiler;
use nonlocality_build_utils::wasi::run_cargo_build_wasi_threads;
use nonlocality_build_utils::wasi::WasiThreadsTarget;
use postcard::to_allocvec;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
pub mod cluster_configuration;

#[derive(Clone)]
enum CargoBuildTarget {
    Host,
    Wasi,
    WasiThreads(WasiThreadsTarget),
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

    pub fn wasi() -> Program {
        Program {
            targets: vec![CargoBuildTarget::Wasi],
        }
    }

    pub fn wasi_threads(threads: WasiThreadsTarget) -> Program {
        Program {
            targets: vec![CargoBuildTarget::WasiThreads(threads)],
        }
    }

    pub fn other() -> Program {
        Program { targets: vec![] }
    }
}

#[derive(Clone)]
enum DirectoryEntry {
    Program(Program),
    Directory(Directory),
}

#[derive(Clone)]
struct Directory {
    entries: BTreeMap<String, DirectoryEntry>,
}

async fn run_cargo_build_target_name(
    project: &std::path::Path,
    target_name: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(
        &project,
        &["build", "--verbose", "--release", "--target", &target_name],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

async fn run_cargo_build(
    project: &std::path::Path,
    target: &CargoBuildTarget,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    match target {
        CargoBuildTarget::Host => run_cargo_build_for_host(project, progress_reporter).await,
        CargoBuildTarget::Wasi => {
            run_cargo_build_target_name(project, "wasm32-wasi", progress_reporter).await
        }
        CargoBuildTarget::WasiThreads(threads) => {
            run_cargo_build_wasi_threads(
                project,
                &threads.wasi_sdk,
                "wasm32-wasip1-threads",
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

#[async_recursion]
async fn build_directory_entry(
    directory_entry: &DirectoryEntry,
    where_in_filesystem: &std::path::Path,
    progress_reporter: Arc<dyn ReportProgress + Sync + Send>,
    mode: CargoBuildMode,
) -> NumberOfErrors {
    let mut error_count = NumberOfErrors(0);
    match directory_entry {
        DirectoryEntry::Program(program) => {
            error_count +=
                build_program(&program, &where_in_filesystem, &progress_reporter, mode).await;
        }
        DirectoryEntry::Directory(directory) => {
            error_count +=
                build_recursively(&directory, &where_in_filesystem, &progress_reporter, mode).await;
        }
    }
    error_count
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
            build_directory_entry(
                &directory_entry,
                &subdirectory,
                progress_reporter_clone,
                mode_clone,
            )
            .await
        }));
    }
    join_all(tasks, progress_reporter).await
}

async fn install_tools(
    repository: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<WasiThreadsTarget>) {
    let tools_directory = repository.join("tools");
    std::fs::create_dir_all(&tools_directory).expect("create tools directory");

    let (error_count_2, wasi_threads) =
        install_wasi_cpp_compiler(&tools_directory, progress_reporter).await;
    (error_count_2, wasi_threads)
}

#[derive(Debug, Clone, Copy)]
enum CargoBuildMode {
    BuildRelease,
    Test,
    Coverage,
}

#[derive(Debug, Clone, Copy)]
enum AstraCommand {
    Build(CargoBuildMode),
    Deploy,
}

fn parse_command(input: &str) -> Option<AstraCommand> {
    match input {
        "build" => Some(AstraCommand::Build(CargoBuildMode::BuildRelease)),
        "test" => Some(AstraCommand::Build(CargoBuildMode::Test)),
        "coverage" => Some(AstraCommand::Build(CargoBuildMode::Coverage)),
        "deploy" => Some(AstraCommand::Deploy),
        _ => None,
    }
}

fn where_cluster_configuration(repository: &std::path::Path) -> PathBuf {
    let target = repository.join("target");
    let output_path = target.join("example_applications_cluster.config");
    output_path
}

async fn build(
    mode: CargoBuildMode,
    repository: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let (mut error_count, maybe_wasi_threads) = install_tools(repository, &progress_reporter).await;
    error_count += run_cargo_fmt(&repository, &progress_reporter).await;

    let coverage_directory = repository.join("coverage");
    let coverage_info_directory = coverage_directory.join("info");
    error_count += delete_directory(&coverage_info_directory);

    match mode {
        CargoBuildMode::BuildRelease => {}
        CargoBuildMode::Test => {
            error_count += run_cargo_test(
                &repository,
                &coverage_info_directory,
                false,
                &progress_reporter,
            )
            .await;
        }
        CargoBuildMode::Coverage => {
            error_count += install_grcov(repository, &progress_reporter).await;
            error_count += run_cargo_test(
                &repository,
                &coverage_info_directory,
                true,
                &progress_reporter,
            )
            .await;
        }
    }

    let root = Directory {
        entries: BTreeMap::from([
            (
                "example_cluster".to_string(),
                DirectoryEntry::Program(Program::host()),
            ),
            (
                "rust".to_string(),
                DirectoryEntry::Directory(Directory {
                    entries: BTreeMap::from([
                        (
                            "call_api".to_string(),
                            DirectoryEntry::Program(Program::wasi()),
                        ),
                        (
                            "database".to_string(),
                            DirectoryEntry::Directory(Directory {
                                entries: BTreeMap::from([
                                    (
                                        "database_client".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                    (
                                        "database_server".to_string(),
                                        DirectoryEntry::Program(match maybe_wasi_threads {
                                            Some(ref wasi_threads) => {
                                                Program::wasi_threads(wasi_threads.clone())
                                            }
                                            None => Program::other(),
                                        }),
                                    ),
                                    (
                                        "database_trait".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                ]),
                            }),
                        ),
                        (
                            "essrpc_example".to_string(),
                            DirectoryEntry::Directory(Directory {
                                entries: BTreeMap::from([
                                    (
                                        "essrpc_client".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                    (
                                        "essrpc_server".to_string(),
                                        DirectoryEntry::Program(match maybe_wasi_threads {
                                            Some(ref wasi_threads) => {
                                                Program::wasi_threads(wasi_threads.clone())
                                            }
                                            None => Program::other(),
                                        }),
                                    ),
                                    (
                                        "essrpc_trait".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                ]),
                            }),
                        ),
                        (
                            "logging".to_string(),
                            DirectoryEntry::Directory(Directory {
                                entries: BTreeMap::from([
                                    (
                                        "log_client".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                    (
                                        "log_server".to_string(),
                                        DirectoryEntry::Program(match maybe_wasi_threads {
                                            Some(ref wasi_threads) => {
                                                Program::wasi_threads(wasi_threads.clone())
                                            }
                                            None => Program::other(),
                                        }),
                                    ),
                                    (
                                        "log_trait".to_string(),
                                        DirectoryEntry::Program(Program::wasi()),
                                    ),
                                ]),
                            }),
                        ),
                        (
                            "hello_rust".to_string(),
                            DirectoryEntry::Program(Program::wasi()),
                        ),
                        (
                            "idle_service".to_string(),
                            DirectoryEntry::Program(Program::wasi()),
                        ),
                        (
                            "provide_api".to_string(),
                            DirectoryEntry::Program(Program::wasi()),
                        ),
                    ]),
                }),
            ),
        ]),
    };

    error_count += build_recursively(&root, &repository, &progress_reporter, mode).await;

    match mode {
        CargoBuildMode::BuildRelease => {
            let configuration =
                cluster_configuration::compile_cluster_configuration(&repository.join("target"))
                    .await;
            let configuration_serialized = to_allocvec(&configuration).unwrap();
            let output_path = where_cluster_configuration(&repository);
            tokio::fs::write(&output_path, &configuration_serialized[..])
                .await
                .unwrap();
        }
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
    let repository = std::path::Path::new(&command_line_arguments[1]);
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

    let error_count = match command {
        AstraCommand::Build(mode) => build(mode, &repository, &progress_reporter).await,
        AstraCommand::Deploy => {
            let management_service_binary = repository
                .parent()
                .expect("looking for the NonlocalityOS parent directory")
                .join("target")
                .join(RASPBERRY_PI_TARGET_NAME)
                .join("release")
                .join(MANAGEMENT_SERVICE_NAME);
            deploy(
                &where_cluster_configuration(&repository),
                &management_service_binary,
                &progress_reporter,
            )
            .await
        }
    };

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
