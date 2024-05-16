use relative_path::RelativePath;
use ssh2::OpenFlags;
use std::sync::Arc;

use crate::run::{NumberOfErrors, ReportProgress};

pub const MANAGEMENT_SERVICE_NAME: &str = "management_service";

fn to_std_path(linux_path: &relative_path::RelativePath) -> std::path::PathBuf {
    linux_path.to_path(std::path::Path::new("/"))
}

fn upload_file(
    session: &ssh2::Session,
    sftp: &ssh2::Sftp,
    from: &std::path::Path,
    to: &RelativePath,
    is_executable: bool,
) {
    println!("Uploading {} to {}", from.display(), to);
    let mut file_to_upload =
        std::fs::File::open(&from).expect("Tried to open the binary to upload");
    let file_size = file_to_upload
        .metadata()
        .expect("Tried to determine the file size")
        .len();
    println!("Uploading file with {} bytes", file_size);

    let mode = match is_executable {
        true => 0o755,
        false => 0o644,
    };
    let mut file_uploader = sftp
        .open_mode(
            &to_std_path(&to),
            OpenFlags::WRITE | OpenFlags::TRUNCATE,
            mode,
            ssh2::OpenType::File,
        )
        .expect("Tried to create binary on the remote");
    std::io::copy(&mut file_to_upload, &mut file_uploader)
        .expect("Tried to upload the file contents");
    std::io::Write::flush(&mut file_uploader).expect("Tried to flush file uploader");
    drop(file_uploader);

    let mut channel = session.channel_session().unwrap();
    channel.exec(&format!("file {}", to)).unwrap();
    let mut standard_output = String::new();
    std::io::Read::read_to_string(&mut channel, &mut standard_output)
        .expect("Tried to read standard output");
    println!("{}", standard_output);
    channel.wait_close().expect("Waited for close");
    assert_eq!(0, channel.exit_status().unwrap());
}

async fn run_simple_ssh_command(session: &ssh2::Session, command: &str) {
    println!("Running {}", command);
    let mut channel: ssh2::Channel = session.channel_session().unwrap();
    channel.exec(command).expect("Tried exec");

    let mut standard_output = String::new();
    let standard_output_stream_id = 0;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_output_stream_id),
        &mut standard_output,
    )
    .expect("Tried to read standard output");
    println!("Standard output: {}", standard_output);

    let mut standard_error = String::new();
    let standard_error_stream_id = ssh2::EXTENDED_DATA_STDERR;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_error_stream_id),
        &mut standard_error,
    )
    .expect("Tried to read standard error");
    println!("Standard error: {}", standard_error);

    channel.wait_close().expect("Waited for close");
    let exit_code = channel.exit_status().unwrap();
    println!("Exit code: {}", exit_code);
    assert_eq!(0, exit_code);
}

pub async fn deploy(
    local_configuration: &std::path::Path,
    management_service_binary: &std::path::Path,
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

    let remote_management_service_binary_next =
        nonlocality_dir.join(&format!("{}.next", MANAGEMENT_SERVICE_NAME));
    upload_file(
        &session,
        &sftp,
        &management_service_binary,
        &remote_management_service_binary_next,
        true,
    );
    let remote_management_service_binary = nonlocality_dir.join(MANAGEMENT_SERVICE_NAME);
    // Sftp.rename doesn't work (error "4", and it's impossible to find documentation on what "4" means).
    run_simple_ssh_command(
        &session,
        // TODO: encode command line arguments correctly
        &format!(
            "/usr/bin/mv {} {}",
            &remote_management_service_binary_next, &remote_management_service_binary
        ),
    )
    .await;

    let remote_configuration = nonlocality_dir.join("cluster_configuration");
    upload_file(
        &session,
        &sftp,
        &local_configuration,
        &remote_configuration,
        false,
    );

    let filesystem_access_root = nonlocality_dir.join("filesystem_access");
    let sudo = RelativePath::new("/usr/bin/sudo");
    run_simple_ssh_command(
        &session,
        &format!(
            "{} {} {} --filesystem_access_root {} --install",
            sudo, remote_management_service_binary, remote_configuration, filesystem_access_root
        ),
    )
    .await;

    NumberOfErrors(0)
}
