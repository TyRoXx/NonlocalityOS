#![feature(duration_constructors)]
use nonlocality_host::{INITIAL_DATABASE_FILE_NAME, INSTALLED_DATABASE_FILE_NAME};
use std::{ffi::OsStr, path::Path};
use tracing::{error, info, warn};
use tracing_subscriber::fmt::format::FmtSpan;

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
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
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

async fn install(nonlocality_directory: &Path, host_binary_name: &OsStr) -> std::io::Result<()> {
    info!("Installing host from {}", nonlocality_directory.display());
    let executable = &nonlocality_directory.join(host_binary_name);

    let initial_database = &nonlocality_directory.join(INITIAL_DATABASE_FILE_NAME);
    let installed_database = &nonlocality_directory.join(INSTALLED_DATABASE_FILE_NAME);
    if std::fs::exists(installed_database)? {
        warn!(
            "Installed database already exists, not going to overwrite it: {}",
            installed_database.display()
        );
    } else {
        info!(
            "Moving initial database {} to installed database: {}",
            initial_database.display(),
            installed_database.display()
        );
        std::fs::rename(initial_database, installed_database)?;
    }

    let service_file_content = format!(
        r#"[Unit]
Description=NonlocalityOS Host
After=network.target

[Service]
Type=simple
Restart=always
RestartSec=15
ExecStart={} run
WorkingDirectory=/
User=root

[Install]
WantedBy=multi-user.target
"#,
        executable.display(),
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

async fn uninstall(nonlocality_directory: &Path) -> std::io::Result<()> {
    info!(
        "Uninstalling systemd service for host {}. Not deleting any other files.",
        nonlocality_directory.display()
    );
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
    let duration = std::time::Duration::from_hours(1);
    warn!(
        "Nothing to do (not implemented), exiting {:?} from now.",
        &duration
    );
    tokio::time::sleep(duration).await;
    warn!("Exiting");
    Ok(())
}

async fn handle_command_line(
    nonlocality_directory: &Path,
    host_binary_name: &OsStr,
    command_line_arguments: &[String],
) -> std::io::Result<()> {
    info!("Command line arguments: {:?}", command_line_arguments);
    if command_line_arguments.len() != 1 {
        error!("Command line argument required: install|run");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid number of command line arguments",
        ));
    }
    let command = command_line_arguments[0].as_str();
    match command {
        "install" => install(nonlocality_directory, host_binary_name).await,
        "uninstall" => uninstall(nonlocality_directory).await,
        "run" => run(nonlocality_directory).await,
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Unknown command {command}"),
        )),
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let command_line_arguments: Vec<String> = std::env::args().collect();
    let current_binary = Path::new(&command_line_arguments[0]);
    info!("Current binary: {}", current_binary.display());
    let nonlocality_directory = current_binary.parent().unwrap();
    info!(
        "Nonlocality directory detected: {}",
        nonlocality_directory.display()
    );
    let host_binary_name = current_binary.file_name().unwrap();
    handle_command_line(
        nonlocality_directory,
        host_binary_name,
        &command_line_arguments[1..],
    )
    .await
}
