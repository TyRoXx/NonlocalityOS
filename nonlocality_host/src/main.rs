use crate::{
    dav_server::dav_server_main,
    operating_system::{LinuxOperatingSystem, OperatingSystem},
    service::{install, make_installed_database_path, uninstall},
};
use clap::{Parser, Subcommand};
use std::{ffi::OsStr, path::Path};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;

#[cfg(test)]
mod fake_operating_system;

#[cfg(test)]
mod main_tests;

mod dav_server;
mod operating_system;
mod service;

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

async fn handle_command_line(
    host_binary_name: &OsStr,
    operating_system: &dyn OperatingSystem,
) -> std::io::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Install {
            nonlocality_directory,
        } => {
            info!(
                "Nonlocality directory for installation: {}",
                nonlocality_directory.display()
            );
            install(&nonlocality_directory, host_binary_name, operating_system).await
        }
        Commands::Uninstall => uninstall(operating_system).await,
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
    let operating_system = LinuxOperatingSystem {};
    handle_command_line(host_binary_name, &operating_system).await
}
