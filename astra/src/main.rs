use astraea::storage::SQLiteStorage;
use nonlocality_build_utils::host::detect_host_operating_system;
use nonlocality_build_utils::host::HostOperatingSystem;
use nonlocality_build_utils::install::deploy;
use nonlocality_build_utils::install::BuildHostBinary;
use nonlocality_build_utils::install::BuildTarget;
use nonlocality_build_utils::raspberrypi::install_raspberry_pi_cpp_compiler;
use nonlocality_build_utils::raspberrypi::run_cargo_build_for_raspberry_pi;
use nonlocality_build_utils::raspberrypi::RaspberryPi64Target;
use nonlocality_build_utils::run::run_cargo_build_for_target;
use nonlocality_build_utils::run::ConsoleErrorReporter;
use nonlocality_build_utils::run::ReportProgress;
use nonlocality_host::INITIAL_DATABASE_FILE_NAME;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

const NONLOCALITY_HOST_BINARY_NAME: &str = "nonlocality_host";

async fn run_cargo_build(
    working_directory: &std::path::Path,
    target: &BuildTarget,
    binary: &str,
    pi: &RaspberryPi64Target,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<PathBuf> {
    match target {
        BuildTarget::LinuxAmd64 => {
            run_cargo_build_for_target(
                working_directory,
                "x86_64-unknown-linux-gnu",
                binary,
                progress_reporter,
            )
            .await
        }
        BuildTarget::RaspberryPi64 => {
            run_cargo_build_for_raspberry_pi(
                &working_directory,
                binary,
                &pi.compiler_installation,
                &pi.host,
                progress_reporter,
            )
            .await
        }
    }
}

async fn install_tools(
    repository: &std::path::Path,
    host: HostOperatingSystem,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> Option<RaspberryPi64Target> {
    let tools_directory = repository.join("tools");
    let raspberry_pi =
        install_raspberry_pi_cpp_compiler(&tools_directory, host, progress_reporter).await;
    raspberry_pi
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let started_at = std::time::Instant::now();
    let command_line_arguments: Vec<String> = std::env::args().collect();
    info!("Command line arguments: {:?}", &command_line_arguments[1..]);
    if command_line_arguments.len() != 1 {
        error!("No command line arguments supported at the moment");
        return std::process::ExitCode::FAILURE;
    }
    let repository = std::env::current_dir().unwrap();
    info!("Repository: {}", repository.display());

    dotenv::dotenv().ok();
    let ssh_endpoint = SocketAddr::from_str(
        &std::env::var("ASTRA_DEPLOY_SSH_ENDPOINT")
            .expect("Tried to read env variable ASTRA_DEPLOY_SSH_ENDPOINT"),
    )
    .unwrap();
    let ssh_user = std::env::var("ASTRA_DEPLOY_SSH_USER")
        .expect("Tried to read env variable ASTRA_DEPLOY_SSH_USER");
    let ssh_password = std::env::var("ASTRA_DEPLOY_SSH_PASSWORD")
        .expect("Tried to read env variable ASTRA_DEPLOY_SSH_PASSWORD");
    info!("Deploying to {} as {}", &ssh_endpoint, &ssh_user);

    let progress_reporter: Arc<dyn ReportProgress + Send + Sync> =
        Arc::new(ConsoleErrorReporter {});

    let temporary_directory = tempfile::tempdir().unwrap();
    let database_path = temporary_directory.path().join(INITIAL_DATABASE_FILE_NAME);
    {
        let connection1 = rusqlite::Connection::open(&database_path).unwrap();
        SQLiteStorage::create_schema(&connection1).unwrap();
        // TODO: put something in the database
        let _storage = SQLiteStorage::from(connection1).unwrap();
    }
    let host_operating_system = detect_host_operating_system();
    let raspberry_pi = install_tools(&repository, host_operating_system, &progress_reporter)
        .await
        .expect("Could not install tools for Raspberry Pi");
    let build: Box<BuildHostBinary> =
        Box::new(
            move |output_binary,
                  target,
                  progress|
                  -> Pin<
                Box<dyn std::future::Future<Output = std::io::Result<()>> + Sync + Send>,
            > {
                let output_binary: PathBuf = output_binary.into();
                let repository = repository.clone();
                let target = target.clone();
                let raspberry_pi = raspberry_pi.clone();
                let progress = progress.clone();
                Box::pin(async move {
                    let executable = run_cargo_build(
                        &repository,
                        &target,
                        NONLOCALITY_HOST_BINARY_NAME,
                        &raspberry_pi,
                        &progress,
                    )
                    .await?;
                    info!(
                        "Copying {} to {}",
                        &executable.display(),
                        &output_binary.display()
                    );
                    std::fs::copy(&executable, &output_binary)?;
                    Ok(())
                })
            },
        );
    let result = deploy(
        &database_path,
        build,
        NONLOCALITY_HOST_BINARY_NAME,
        INITIAL_DATABASE_FILE_NAME,
        &ssh_endpoint,
        &ssh_user,
        &ssh_password,
        &progress_reporter,
    )
    .await;

    let build_duration = started_at.elapsed();
    info!("Duration: {:?}", build_duration);

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            info!("Error: {}", &error);
            std::process::ExitCode::FAILURE
        }
    }
}
