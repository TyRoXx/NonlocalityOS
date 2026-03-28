use tracing::{error, info};
mod dropbox_importer_tests;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    let working_dir = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(e) => {
            error!("Failed to determine working directory: {e}");
            std::process::exit(1);
        }
    };
    info!("Working directory: {}", working_dir.display());
    match dotenv::dotenv() {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to load .env file: {e}. Copy system_tests/.env.template to .env in the working directory and fill in the required values!");
            std::process::exit(1);
        }
    }
    let dropbox_api_app_key = match std::env::var("SYSTEM_TESTS_DROPBOX_API_APP_KEY") {
        Ok(key) => key,
        Err(e) => {
            error!("Failed to read SYSTEM_TESTS_DROPBOX_API_APP_KEY from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_oauth = match std::env::var("SYSTEM_TESTS_DROPBOX_OAUTH") {
        Ok(oauth) => oauth,
        Err(e) => {
            error!("Failed to read SYSTEM_TESTS_DROPBOX_OAUTH from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_test_directory = match std::env::var("SYSTEM_TESTS_DROPBOX_TEST_DIRECTORY") {
        Ok(directory) => directory,
        Err(e) => {
            error!("Failed to read SYSTEM_TESTS_DROPBOX_TEST_DIRECTORY from env: {e}");
            std::process::exit(1);
        }
    };
    dropbox_importer_tests::test_dropbox_importer(
        &dropbox_api_app_key,
        &dropbox_oauth,
        &dropbox_test_directory,
    )
    .await;
}
