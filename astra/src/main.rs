#![deny(warnings)]
use async_recursion::async_recursion;
use postcard::to_allocvec;
use ssh2::OpenFlags;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::os::windows::fs::MetadataExt;
use std::sync::Arc;
pub mod downloads;
pub mod cluster_configuration;

#[derive(Clone)]
struct RaspberryPi64Target {
    compiler_installation: std::path::PathBuf,
}

#[derive(Clone)]
struct WasiThreadsTarget {
    wasi_sdk: std::path::PathBuf,
}

#[derive(Clone)]
enum CargoBuildTarget {
    Host,
    RaspberryPi64(RaspberryPi64Target),
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

    pub fn host_and_pi(pi: RaspberryPi64Target) -> Program {
        Program {
            targets: vec![CargoBuildTarget::Host, CargoBuildTarget::RaspberryPi64(pi)],
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

#[derive(Debug, PartialEq, Clone, Copy)]
struct NumberOfErrors(u64);

impl std::ops::Add<NumberOfErrors> for NumberOfErrors {
    type Output = NumberOfErrors;

    fn add(self, rhs: NumberOfErrors) -> NumberOfErrors {
        let (sum, has_overflown) = u64::overflowing_add(self.0, rhs.0);
        assert!(!has_overflown);
        NumberOfErrors(sum)
    }
}

trait ReportProgress {
    fn log(&self, message: &str);
}

impl std::ops::AddAssign for NumberOfErrors {
    fn add_assign(&mut self, rhs: NumberOfErrors) {
        *self = *self + rhs;
    }
}

async fn run_process_with_error_only_output(
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

async fn run_cargo(
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

async fn run_cargo_fmt(
    project: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(&project, &["fmt"], &HashMap::new(), progress_reporter).await
}

async fn run_cargo_test(
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

const RASPBERRY_PI_TARGET_NAME: &str = "aarch64-unknown-linux-gnu";

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

fn confirm_directory(path: &std::path::Path) -> bool {
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

async fn run_cargo_build_for_raspberry_pi(
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

async fn run_cargo_build_wasi_threads(
    project: &std::path::Path,
    wasi_sdk: &std::path::Path,
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
    let clang_exe = wasi_sdk.join("bin/clang.exe");
    let clang_exe_str = clang_exe
        .to_str()
        .expect("Tried to convert a path to a string");
    run_process_with_error_only_output(&project, std::path::Path::new(
         "cargo"), &["build", "--verbose", "--release", "--target", target_name], &HashMap::from([
        ("CFLAGS".to_string(), "-pthread".to_string()),
        ("RUSTFLAGS".to_string(), format!("-C target-feature=-crt-static -C link-arg=-L{} -C link-arg=-lclang_rt.builtins-wasm32", lib_dir_str)),
        (format!("CC_{}", target_name), clang_exe_str.to_string()),
    ]), progress_reporter).await
}

async fn run_cargo_build_for_host(
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

async fn run_cargo_build(
    project: &std::path::Path,
    target: &CargoBuildTarget,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    match target {
        CargoBuildTarget::Host => run_cargo_build_for_host(project, progress_reporter).await,
        CargoBuildTarget::RaspberryPi64(pi) => {
            run_cargo_build_for_raspberry_pi(&project, &pi.compiler_installation, progress_reporter)
                .await
        }
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

async fn install_raspberry_pi_cpp_compiler(
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

async fn install_wasi_cpp_compiler(
    tools_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (NumberOfErrors, Option<WasiThreadsTarget>) {
    let compiler_name = "wasi-sdk-22";
    let archive_file_name = format!("{}.0.m-mingw.tar.gz", compiler_name);
    let download_url = format!(
        "https://github.com/WebAssembly/wasi-sdk/releases/download/{}/{}",
        &compiler_name, &archive_file_name
    );
    let unpacked_directory = tools_directory.join(format!("{}.0.m-mingw", compiler_name));
    match downloads::install_from_downloaded_archive(
        &download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Gz,
    ) {
        Ok(_) => {
            let sub_dir = unpacked_directory.join(format!("{}.0+m", compiler_name));
            if confirm_directory(&sub_dir) {
                (
                    NumberOfErrors(0),
                    Some(WasiThreadsTarget { wasi_sdk: sub_dir }),
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

async fn install_tools(
    repository: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> (
    NumberOfErrors,
    Option<RaspberryPi64Target>,
    Option<WasiThreadsTarget>,
) {
    let tools_directory = repository.join("tools");
    let (error_count_1, raspberry_pi) =
        install_raspberry_pi_cpp_compiler(&tools_directory, progress_reporter).await;
    let (error_count_2, wasi_threads) =
        install_wasi_cpp_compiler(&tools_directory, progress_reporter).await;
    (error_count_1 + error_count_2, raspberry_pi, wasi_threads)
}

async fn install_grcov(
    working_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_cargo(
        &working_directory,
        &["install", "grcov"],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

async fn generate_coverage_report_with_grcov(
    repository: &std::path::Path,
    coverage_info_directory: &std::path::Path,
    coverage_report_directory: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    run_process_with_error_only_output(
        &repository,
        std::path::Path::new("grcov"),
        &[
            coverage_info_directory
                .to_str()
                .expect("Tried to convert path to string"),
            "--log-level",
            "DEBUG",
            "--binary-path",
            repository
                .join("target/debug/deps")
                .to_str()
                .expect("Tried to convert path to string"),
            "--source-dir",
            repository
                .to_str()
                .expect("Tried to convert path to string"),
            // exclude some generated files in the target dir:
            "--ignore",
            "target/debug/build/*",
            "-t",
            "html",
            "--branch",
            "--output-path",
            coverage_report_directory
                .to_str()
                .expect("Tried to convert path to string"),
        ],
        &HashMap::new(),
        progress_reporter,
    )
    .await
}

fn delete_directory(root: &std::path::Path) -> NumberOfErrors {
    match std::fs::metadata(&root) {
        Ok(_) => match std::fs::remove_dir_all(&root) {
            Ok(_) => NumberOfErrors(0),
            Err(error) => {
                println!("Could not delete {}: {}", root.display(), error);
                NumberOfErrors(1)
            }
        },
        Err(_) => NumberOfErrors(0),
    }
}

struct ConsoleErrorReporter {}

impl ReportProgress for ConsoleErrorReporter {
    fn log(&self, error_message: &str) {
        println!("{}", &error_message);
    }
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

async fn build(
    mode: CargoBuildMode,
    repository: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    let (mut error_count, maybe_raspberry_pi, maybe_wasi_threads) =
        install_tools(repository, &progress_reporter).await;
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
                "admin_tool".to_string(),
                DirectoryEntry::Program(Program::host()),
            ),
            (
                "astra".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
            (
                "example_applications".to_string(),
                DirectoryEntry::Directory(Directory {
                    entries: BTreeMap::from([(
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
                    )]),
                }),
            ),
            (
                "management_interface".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
            (
                MANAGEMENT_SERVICE_NAME.to_string(),
                DirectoryEntry::Program(match maybe_raspberry_pi {
                    Some(raspberry_pi) => Program::host_and_pi(raspberry_pi),
                    None => Program::host(),
                }),
            ),
            (
                "nonlocality_env".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
        ]),
    };

    error_count += build_recursively(&root, &repository, &progress_reporter, mode).await;

    match mode {
        CargoBuildMode::BuildRelease => {
            let configuration = cluster_configuration::compile_cluster_configuration(&repository.join("target")).await;
            let configuration_serialized = to_allocvec(&configuration).unwrap();
            let target = repository.join("target");
            let output_path = target.join("example_applications_cluster.config");
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

const MANAGEMENT_SERVICE_NAME: &str = "management_service";

fn to_std_path(linux_path: &relative_path::RelativePath) -> std::path::PathBuf {
    linux_path.to_path(std::path::Path::new("/"))
}

async fn deploy(
    repository: &std::path::Path,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> NumberOfErrors {
    dotenv::dotenv().ok();
    let ssh_endpoint = std::env::var("ASTRA_DEPLOY_SSH_ENDPOINT")
        .expect("Tried to read env variable ASTRA_DEPLOY_SSH_ENDPOINT");
    let ssh_user = std::env::var("ASTRA_DEPLOY_SSH_USER")
        .expect("Tried to read env variable ASTRA_DEPLOY_SSH_USER");
    let ssh_password = std::env::var("ASTRA_DEPLOY_SSH_PASSWORD")
        .expect("Tried to read env variable ASTRA_DEPLOY_SSH_PASSWORD");

    let tcp = std::net::TcpStream::connect(&ssh_endpoint).unwrap();
    let mut session = ssh2::Session::new().unwrap();
    session.set_tcp_stream(tcp);
    match session.handshake() {
        Ok(_) => {}
        Err(error) => progress_reporter.log(&format!("Could not SSH handshake: {}", error)),
    }
    session.userauth_password(&ssh_user, &ssh_password).unwrap();
    assert!(session.authenticated());

    let binary = repository
        .join("target")
        .join(RASPBERRY_PI_TARGET_NAME)
        .join("release")
        .join(MANAGEMENT_SERVICE_NAME);
    let mut file_to_upload =
        std::fs::File::open(&binary).expect("Tried to open the binary to upload");
    let file_size = file_to_upload
        .metadata()
        .expect("Tried to determine the file size")
        .file_size();
    println!("Uploading file with {} bytes", file_size);

    let sftp = session.sftp().expect("Tried to open SFTP");
    let home = relative_path::RelativePath::new("/home").join(ssh_user);
    let home_found = sftp
        .stat(&to_std_path(&home))
        .expect("Tried to stat home on the remote");
    if !home_found.is_dir() {
        progress_reporter.log(&format!("Expected a directory at remote location {}", home));
        return NumberOfErrors(1);
    }

    let nonlocality_dir = home.join(".nonlocality");
    match sftp.stat(&to_std_path(&nonlocality_dir)) {
        Ok(exists) => {
            if exists.is_dir() {
                println!("Our directory appears to exist.");
            } else {
                progress_reporter.log(&format!("Our directory is a file!"));
                return NumberOfErrors(1);
            }
        }
        Err(error) => {
            println!("Could not stat our directory: {}", error);
            println!("Creating directory {}", nonlocality_dir);
            sftp.mkdir(&to_std_path(&nonlocality_dir), 0o755)
                .expect("Tried to create our directory on the remote");
        }
    }

    let remote_management_service_binary = nonlocality_dir.join(MANAGEMENT_SERVICE_NAME);
    let mut file_uploader = sftp
        .open_mode(
            &to_std_path(&remote_management_service_binary),
            OpenFlags::WRITE | OpenFlags::TRUNCATE,
            0o755,
            ssh2::OpenType::File,
        )
        .expect("Tried to create binary on the remote");
    std::io::copy(&mut file_to_upload, &mut file_uploader)
        .expect("Tried to upload the file contents");
    std::io::Write::flush(&mut file_uploader).expect("Tried to flush file uploader");
    drop(file_uploader);

    let mut channel = session.channel_session().unwrap();
    channel
        .exec(&format!("file {}", remote_management_service_binary))
        .unwrap();
    let mut s = String::new();
    std::io::Read::read_to_string(&mut channel, &mut s).unwrap();
    println!("{}", s);
    channel.wait_close().expect("Waited for close");
    assert_eq!(0, channel.exit_status().unwrap());

    println!("Starting {}", &remote_management_service_binary);
    let mut channel = session.channel_session().unwrap();
    channel
        .exec(&format!("{}", remote_management_service_binary))
        .unwrap();
    let mut s = String::new();
    std::io::Read::read_to_string(&mut channel, &mut s).unwrap();
    println!("{}", s);
    channel.wait_close().expect("Waited for close");
    assert_eq!(101, channel.exit_status().unwrap());

    NumberOfErrors(0)
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
        AstraCommand::Deploy => deploy(&repository, &progress_reporter).await,
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
