use astraea::{
    sqlite_storage::SQLiteStorage,
    storage::{CollectGarbage, CommitChanges},
};
use dropbox_importer::{
    dropbox_api::RealDropboxApi, file_cache::PersistentFileCacheMap, importer::import_directory,
};
use dropbox_sdk::{default_async_client::UserAuthDefaultClient, oauth2::Authorization};
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let database_path_from_args = std::env::args()
        .nth(1)
        .expect("Database path argument missing");
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
            error!("Failed to load .env file: {e}. Copy .env.template to .env in the root of the repository and fill in the required values!");
            std::process::exit(1);
        }
    }
    let dropbox_api_app_key = match std::env::var("IMPORT_DROPBOX_CLI_API_APP_KEY") {
        Ok(key) => key,
        Err(e) => {
            error!("Failed to read IMPORT_DROPBOX_CLI_API_APP_KEY from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_oauth = match std::env::var("IMPORT_DROPBOX_CLI_OAUTH") {
        Ok(oauth) => oauth,
        Err(e) => {
            error!("Failed to read IMPORT_DROPBOX_CLI_OAUTH from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_test_directory = match std::env::var("IMPORT_DROPBOX_CLI_TEST_DIRECTORY") {
        Ok(directory) => directory,
        Err(e) => {
            error!("Failed to read IMPORT_DROPBOX_CLI_TEST_DIRECTORY from env: {e}");
            std::process::exit(1);
        }
    };
    let auth = Authorization::load(dropbox_api_app_key.to_string(), &dropbox_oauth)
        .expect("Failed to load Dropbox authorization");
    let dropbox_client = Arc::new(UserAuthDefaultClient::new(auth));
    let database_existed = std::path::Path::new(&database_path_from_args).exists();
    let connection = rusqlite::Connection::open(&database_path_from_args)
        .expect("Failed to open SQLite database");
    if !database_existed {
        SQLiteStorage::create_schema(&connection).unwrap();
    }
    // not sure what's the best value for auto commit
    let auto_commit_after_number_of_writes = Some(20);
    let storage = Arc::new(
        SQLiteStorage::from(connection, auto_commit_after_number_of_writes)
            .expect("Failed to create SQLiteStorage"),
    );
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let dropbox_api = RealDropboxApi {
        dropbox_client: dropbox_client.clone(),
    };
    let chunk_size = dropbox_importer::file_cache::DEFAULT_CHUNK_SIZE;
    let download_cache = PersistentFileCacheMap::load_or_create(
        &*storage,
        &*storage,
        &*storage,
        &*storage,
        "import_dropbox_cli".to_string(),
        chunk_size,
    )
    .await
    .expect("Failed to load or create persistent file cache");
    info!(
        "Loaded or created cache with {} entries",
        download_cache.number_of_entries().await.unwrap()
    );
    let open_directory = import_directory(
        &dropbox_test_directory,
        storage.clone(),
        clock,
        &dropbox_api,
        &download_cache,
    )
    .await
    .expect("Failed to import Dropbox directory");
    let status = open_directory
        .request_save()
        .await
        .expect("Failed to save open directory");
    info!("Successfully saved imported directory: {:?}", status.digest);
    // TODO: commit regularly
    let committed_changes = storage
        .commit_changes()
        .await
        .expect("Database COMMIT failed");
    info!("Committed database changes: {:?}", committed_changes);
    let stats = storage
        .collect_some_garbage()
        .await
        .expect("Failed to collect garbage");
    info!("Collected garbage: {:?}", stats);
    Ok(())
}
