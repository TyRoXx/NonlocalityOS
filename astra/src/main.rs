use astraea::storage::SQLiteStorage;
use clap::{Parser, Subcommand};
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
use tracing::info;
use tracing_subscriber::fmt::format::FmtSpan;

const NONLOCALITY_HOST_BINARY_NAME: &str = "nonlocality_host";

#[derive(Parser)]
#[command(name = "astra")]
#[command(about = "Astra deployment tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Install the application on the target system
    Install,
    /// Uninstall the application from the target system
    Uninstall,
}

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
                working_directory,
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

async fn build_host_binary(
    repository: &std::path::Path,
    output_binary: &std::path::Path,
    target: &BuildTarget,
    progress: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let host_operating_system = detect_host_operating_system();
    let raspberry_pi = install_tools(repository, host_operating_system, progress)
        .await
        .expect("Could not install tools for Raspberry Pi");
    let executable = run_cargo_build(
        repository,
        target,
        NONLOCALITY_HOST_BINARY_NAME,
        &raspberry_pi,
        progress,
    )
    .await?;
    info!(
        "Copying {} to {}",
        &executable.display(),
        output_binary.display()
    );
    std::fs::copy(&executable, output_binary)?;
    Ok(())
}

fn make_build_host_binary_function(repository: &std::path::Path) -> Box<BuildHostBinary> {
    let repository = repository.to_path_buf();
    Box::new(
        move |output_binary,
            target,
            progress|
            -> Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Sync + Send>,
        > {
            let output_binary: PathBuf = output_binary.into();
            let repository = repository.clone();
            let target = target.clone();
            let progress = progress.clone();
            Box::pin(async move { build_host_binary(&repository, &output_binary, &target, &progress).await })
        }
    )
}

async fn install(
    repository: &std::path::Path,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let temporary_directory = tempfile::tempdir().unwrap();
    let database_path = temporary_directory.path().join(INITIAL_DATABASE_FILE_NAME);
    {
        let connection1 = rusqlite::Connection::open(&database_path).unwrap();
        SQLiteStorage::create_schema(&connection1).unwrap();
        // TODO: put something in the database
        let _storage = SQLiteStorage::from(connection1).unwrap();
    }
    let build = make_build_host_binary_function(repository);
    deploy(
        &database_path,
        build,
        NONLOCALITY_HOST_BINARY_NAME,
        INITIAL_DATABASE_FILE_NAME,
        ssh_endpoint,
        ssh_user,
        ssh_password,
        progress_reporter,
    )
    .await
}

async fn uninstall(
    repository: &std::path::Path,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let build = make_build_host_binary_function(repository);
    nonlocality_build_utils::install::uninstall(
        build,
        NONLOCALITY_HOST_BINARY_NAME,
        ssh_endpoint,
        ssh_user,
        ssh_password,
        progress_reporter,
    )
    .await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let started_at = std::time::Instant::now();

    let cli = Cli::parse();
    info!("Command: {:?}", cli.command);

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

    let result = match cli.command {
        Commands::Install => {
            install(
                &repository,
                &ssh_endpoint,
                &ssh_user,
                &ssh_password,
                &progress_reporter,
            )
            .await
        }
        Commands::Uninstall => {
            uninstall(
                &repository,
                &ssh_endpoint,
                &ssh_user,
                &ssh_password,
                &progress_reporter,
            )
            .await
        }
    };

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
