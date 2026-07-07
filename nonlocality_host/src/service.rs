use astraea::sqlite_storage::SQLiteStorage;
use std::path::Path;
use std::{ffi::OsStr, sync::Arc};
use tracing::{error, info, warn};

use crate::operating_system::{file_exists, Directory, OperatingSystem};

pub const SERVICE_FILE_NAME: &str = "nonlocalityos_host.service";
pub const SYSTEMD_SERVICES_DIRECTORY: &str = "/etc/systemd/system";
pub const INSTALLED_DATABASE_FILE_NAME: &str = "database.sqlite3";

async fn open_systemd_service_directory(
    operating_system: &dyn OperatingSystem,
) -> std::io::Result<Arc<dyn Directory + Sync + Send>> {
    operating_system
        .open_directory(std::path::Path::new(SYSTEMD_SERVICES_DIRECTORY))
        .await
}

async fn run_systemctl(
    arguments: &[&str],
    operating_system: &dyn OperatingSystem,
) -> std::io::Result<()> {
    let systemctl = std::path::Path::new("/usr/bin/systemctl");
    let root_directory = std::path::Path::new("/");
    operating_system
        .run_process(root_directory, systemctl, arguments)
        .await
}

pub fn make_installed_database_path(nonlocality_directory: &Path) -> std::path::PathBuf {
    nonlocality_directory.join(INSTALLED_DATABASE_FILE_NAME)
}

pub async fn install(
    nonlocality_directory: &Path,
    host_binary_name: &OsStr,
    operating_system: &dyn OperatingSystem,
) -> std::io::Result<()> {
    info!("Installing host from {}", nonlocality_directory.display());
    let executable = &nonlocality_directory.join(host_binary_name);

    let installed_database = &make_installed_database_path(nonlocality_directory);
    if file_exists(installed_database, operating_system).await? {
        warn!(
            "Installed database already exists, not going to overwrite it: {}",
            installed_database.display()
        );
    } else {
        info!("Generating database: {}", installed_database.display());
        let database_directory = operating_system
            .open_directory(nonlocality_directory)
            .await?;
        match database_directory.create().await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "Failed to create directory for database at {}: {}",
                    nonlocality_directory.display(),
                    e
                );
                return Err(e);
            }
        }
        let locked_directory = database_directory.lock().await?;
        let locked_installed_database = make_installed_database_path(&locked_directory);
        SQLiteStorage::create_schema(
            &rusqlite::Connection::open(locked_installed_database).unwrap(),
        )
        .unwrap();
        database_directory.unlock().await?;
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
        executable_argument, nonlocality_directory_argument
    );
    let temporary_directory = operating_system
        .create_temporary_directory()
        .await
        .expect("create a temporary directory");
    let temporary_directory_path = temporary_directory.lock().await?;
    let temporary_file_path = temporary_directory_path.join(SERVICE_FILE_NAME);
    let mut temporary_file =
        std::fs::File::create_new(&temporary_file_path).expect("create temporary file");
    use std::io::Write;
    write!(&mut temporary_file, "{}", service_file_content)
        .expect("write content of the service file");
    let systemd_service_directory = open_systemd_service_directory(operating_system).await?;
    let systemd_service_directory_path = systemd_service_directory.lock().await?;
    let systemd_service_path = systemd_service_directory_path.join(SERVICE_FILE_NAME);
    info!(
        "Installing systemd service file to {}",
        systemd_service_path.display()
    );

    std::fs::rename(&temporary_file_path, &systemd_service_path)
        .expect("move temporary service file into systemd directory");
    systemd_service_directory.unlock().await?;
    drop(systemd_service_directory);
    temporary_directory.unlock().await?;
    drop(temporary_directory);

    run_systemctl(&["daemon-reload"], operating_system).await?;
    run_systemctl(&["enable", SERVICE_FILE_NAME], operating_system).await?;
    run_systemctl(&["restart", SERVICE_FILE_NAME], operating_system).await?;
    run_systemctl(&["status", SERVICE_FILE_NAME], operating_system).await
}

pub async fn uninstall(operating_system: &dyn OperatingSystem) -> std::io::Result<()> {
    info!("Uninstalling systemd service. Not deleting any other files.");
    let systemd_service_directory = open_systemd_service_directory(operating_system).await?;
    let systemd_service_path = systemd_service_directory
        .path()
        .await?
        .join(SERVICE_FILE_NAME);
    if systemd_service_directory
        .file_exists(std::ffi::OsStr::new(SERVICE_FILE_NAME))
        .await?
    {
        run_systemctl(&["disable", SERVICE_FILE_NAME], operating_system).await?;
        info!(
            "Deleting systemd service file {}",
            systemd_service_path.display()
        );
        systemd_service_directory
            .remove_file(std::ffi::OsStr::new(SERVICE_FILE_NAME))
            .await?;
    } else {
        info!(
            "Systemd service file {} does not exist; nothing to delete",
            systemd_service_path.display()
        );
    }
    run_systemctl(&["daemon-reload"], operating_system).await?;
    Ok(())
}
