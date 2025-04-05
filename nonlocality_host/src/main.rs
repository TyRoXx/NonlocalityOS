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
        .current_dir(&working_directory)
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
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Process failed with exit code: {}", output.status),
        ))
    }
}

async fn install(nonlocality_directory: &Path, host_binary_name: &OsStr) -> std::io::Result<()> {
    info!("Installing host from {}", nonlocality_directory.display());
    let executable = &nonlocality_directory.join(host_binary_name);

    let initial_database = &nonlocality_directory.join(INITIAL_DATABASE_FILE_NAME);
    let installed_database = &nonlocality_directory.join(INSTALLED_DATABASE_FILE_NAME);
    if std::fs::exists(&installed_database)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "Installed database already exists: {}",
                installed_database.display()
            ),
        ));
    }
    info!(
        "Moving initial database {} to installed database: {}",
        initial_database.display(),
        installed_database.display()
    );
    std::fs::rename(initial_database, installed_database)?;

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
    let service_file_name = "nonlocalityos_host.service";
    let temporary_file_path = temporary_directory.path().join(service_file_name);
    let mut temporary_file =
        std::fs::File::create_new(&temporary_file_path).expect("create temporary file");
    use std::io::Write;
    write!(&mut temporary_file, "{}", &service_file_content)
        .expect("write content of the service file");
    let systemd_services_directory = std::path::Path::new("/etc/systemd/system");
    let systemd_service_path = systemd_services_directory.join(service_file_name);
    info!(
        "Installing systemd service file to {}",
        systemd_service_path.display()
    );
    std::fs::rename(&temporary_file_path, &systemd_service_path)
        .expect("move temporary service file into systemd directory");
    let systemctl = std::path::Path::new("/usr/bin/systemctl");
    let root_directory = std::path::Path::new("/");
    run_process(root_directory, systemctl, &["daemon-reload"]).await?;
    run_process(root_directory, systemctl, &["enable", service_file_name]).await?;
    run_process(root_directory, systemctl, &["restart", service_file_name]).await?;
    run_process(root_directory, systemctl, &["status", service_file_name]).await
}

async fn run(nonlocality_directory: &Path) -> std::io::Result<()> {
    info!("Running host in {}", nonlocality_directory.display());
    warn!("Nothing to do (not implemented), exiting soon.");
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;
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
        "run" => run(nonlocality_directory).await,
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Unknown command {}", command),
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
