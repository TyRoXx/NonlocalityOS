#![feature(duration_constructors)]
use crate::dav_server::dav_server_main;
use clap::{Parser, Subcommand};
use std::{ffi::OsStr, path::Path};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;
mod dav_server;
use astraea::storage::SQLiteStorage;
use nonlocality_host::INSTALLED_DATABASE_FILE_NAME;

#[derive(Parser)]
#[command(name = "nonlocality_host", about = "NonlocalityOS Host Service")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Install the NonlocalityOS host service
    Install {
        /// Directory containing the NonlocalityOS installation
        #[arg(value_name = "NONLOCALITY_DIRECTORY", value_parser = clap::value_parser!(std::path::PathBuf))]
        nonlocality_directory: std::path::PathBuf,
    },
    /// Uninstall the NonlocalityOS host service
    Uninstall,
    /// Run the NonlocalityOS host service
    Run {
        /// Directory containing the NonlocalityOS installation
        #[arg(value_name = "NONLOCALITY_DIRECTORY", value_parser = clap::value_parser!(std::path::PathBuf))]
        nonlocality_directory: std::path::PathBuf,
    },
}

async fn run_process(
    working_directory: &std::path::Path,
    executable: &std::path::Path,
    arguments: &[&str],
) -> std::io::Result<()> {
    info!("Run process: {} {:?}", executable.display(), arguments);
    let output = tokio::process::Command::new(executable)
        .args(arguments)
        .current_dir(working_directory)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .expect("start process");
    if output.status.success() {
        info!("Success");
        Ok(())
    } else {
        info!("Working directory: {}", working_directory.display());
        error!("Exit status: {}", output.status);
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.is_empty() {
            error!("Standard output:\n{}", stdout.trim_end());
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.is_empty() {
            error!("Standard error:\n{}", stderr.trim_end());
        }
        Err(std::io::Error::other(format!(
            "Process failed with exit code: {}",
            output.status
        )))
    }
}

const SERVICE_FILE_NAME: &str = "nonlocalityos_host.service";

fn make_service_file_path() -> std::path::PathBuf {
    let systemd_services_directory = std::path::Path::new("/etc/systemd/system");
    systemd_services_directory.join(SERVICE_FILE_NAME)
}

async fn run_systemctl(arguments: &[&str]) -> std::io::Result<()> {
    let systemctl = std::path::Path::new("/usr/bin/systemctl");
    let root_directory = std::path::Path::new("/");
    run_process(root_directory, systemctl, arguments).await
}

fn make_installed_database_path(nonlocality_directory: &Path) -> std::path::PathBuf {
    nonlocality_directory.join(INSTALLED_DATABASE_FILE_NAME)
}

async fn install(nonlocality_directory: &Path, host_binary_name: &OsStr) -> std::io::Result<()> {
    info!("Installing host from {}", nonlocality_directory.display());
    let executable = &nonlocality_directory.join(host_binary_name);

    let installed_database = &make_installed_database_path(nonlocality_directory);
    if std::fs::exists(installed_database)? {
        warn!(
            "Installed database already exists, not going to overwrite it: {}",
            installed_database.display()
        );
    } else {
        info!("Generating database: {}", installed_database.display());

        SQLiteStorage::create_schema(&rusqlite::Connection::open(installed_database).unwrap())
            .unwrap();
    }

    let executable_argument = executable.display().to_string();
    let nonlocality_directory_argument = nonlocality_directory.display().to_string();
    let service_file_content = format!(
        r#"[Unit]
Description=NonlocalityOS Host
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=15
ExecStart='{}' run '{}'
WorkingDirectory=/
User=root

[Install]
WantedBy=multi-user.target
"#,
        &executable_argument, &nonlocality_directory_argument
    );
    let temporary_directory = tempfile::tempdir().expect("create a temporary directory");
    let temporary_file_path = temporary_directory.path().join(SERVICE_FILE_NAME);
    let mut temporary_file =
        std::fs::File::create_new(&temporary_file_path).expect("create temporary file");
    use std::io::Write;
    write!(&mut temporary_file, "{}", &service_file_content)
        .expect("write content of the service file");
    let systemd_service_path = make_service_file_path();
    info!(
        "Installing systemd service file to {}",
        systemd_service_path.display()
    );

    std::fs::rename(&temporary_file_path, &systemd_service_path)
        .expect("move temporary service file into systemd directory");

    run_systemctl(&["daemon-reload"]).await?;
    run_systemctl(&["enable", SERVICE_FILE_NAME]).await?;
    run_systemctl(&["restart", SERVICE_FILE_NAME]).await?;
    run_systemctl(&["status", SERVICE_FILE_NAME]).await
}

async fn uninstall() -> std::io::Result<()> {
    info!("Uninstalling systemd service. Not deleting any other files.");
    let systemd_service_path = make_service_file_path();
    if std::fs::exists(&systemd_service_path)? {
        run_systemctl(&["disable", SERVICE_FILE_NAME]).await?;
        info!(
            "Deleting systemd service file {}",
            systemd_service_path.display()
        );
        std::fs::remove_file(&systemd_service_path)?;
    } else {
        info!(
            "Systemd service file {} does not exist; nothing to delete",
            systemd_service_path.display()
        );
    }
    run_systemctl(&["daemon-reload"]).await?;
    Ok(())
}

async fn run(nonlocality_directory: &Path) -> std::io::Result<()> {
    info!("Running host in {}", nonlocality_directory.display());
    match std::fs::create_dir_all(nonlocality_directory) {
        Ok(_) => {}
        Err(e) => {
            error!(
                "Failed to create Nonlocality directory {}: {e}",
                nonlocality_directory.display()
            );
            return Err(e);
        }
    }
    let database_file_name = make_installed_database_path(nonlocality_directory);
    info!(
        "Using database file for DAV server: {}",
        database_file_name.display()
    );
    match dav_server_main(&database_file_name).await {
        Ok(_) => {
            warn!("DAV server exited without an error");
            Ok(())
        }
        Err(e) => {
            error!("DAV server failed: {e}");
            Err(std::io::Error::other(format!("DAV server failed: {e}")))
        }
    }
}

async fn handle_command_line(host_binary_name: &OsStr) -> std::io::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Install {
            nonlocality_directory,
        } => {
            info!(
                "Nonlocality directory for installation: {}",
                nonlocality_directory.display()
            );
            install(&nonlocality_directory, host_binary_name).await
        }
        Commands::Uninstall => uninstall().await,
        Commands::Run {
            nonlocality_directory,
        } => {
            info!(
                "Nonlocality directory for running: {}",
                nonlocality_directory.display()
            );
            run(&nonlocality_directory).await
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let current_binary = std::env::current_exe().unwrap();
    info!("Current binary: {}", current_binary.display());

    let host_binary_name = current_binary.file_name().unwrap();
    handle_command_line(host_binary_name).await
}
