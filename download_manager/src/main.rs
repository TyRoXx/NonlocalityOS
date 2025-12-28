use crate::{
    dropbox::{join_dropbox_paths, RealDropbox},
    telegram_bot::{HandleTelegramBotRequests, TelegramBot, TeloxideTelegramBot},
};
use astraea::{storage::SQLiteStorage, tree::BlobDigest};
use notify::{RecommendedWatcher, Watcher};
use pretty_assertions::assert_eq;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
};
use tracing::{debug, error, info, warn};

#[cfg(test)]
mod main_tests;

mod yt_dlp;

#[cfg(test)]
mod yt_dlp_tests;

mod telegram_bot;

#[cfg(test)]
mod telegram_bot_tests;

mod dropbox;

#[cfg(test)]
mod dropbox_tests;

fn upgrade_schema(
    connection: &rusqlite::Connection,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let user_version =
        connection.query_row("PRAGMA user_version;", [], |row| row.get::<_, i32>(0))?;
    match user_version {
        0 => {
            assert_eq!(
                0,
                connection.execute(
                    "CREATE TABLE download_job (
                        id INTEGER PRIMARY KEY NOT NULL,
                        url TEXT UNIQUE NOT NULL
                    ) STRICT",
                    ()
                )?
            );
            assert_eq!(
                0,
                connection.execute(
                    "CREATE TABLE result_file (
                    id INTEGER PRIMARY KEY NOT NULL,
                    download_job_id INTEGER NOT NULL,
                    sha3_512_digest BLOB NOT NULL,
                    FOREIGN KEY(download_job_id) REFERENCES download_job(id),
                    CONSTRAINT sha3_512_digest_length_check CHECK (LENGTH(sha3_512_digest) == 64)
                ) STRICT",
                    ()
                )?
            );
            assert_eq!(0, connection.execute("PRAGMA user_version = 1;", ())?);
            Ok(())
        }
        1 => {
            // Future migrations go here
            Ok(())
        }
        _ => {
            error!("Unsupported database schema version: {}", user_version);
            Err(Box::from(format!(
                "Unsupported database schema version: {}",
                user_version
            )))
        }
    }
}

fn store_urls_in_database(
    urls: Vec<String>,
    connection: &mut rusqlite::Connection,
) -> rusqlite::Result<usize> {
    let mut inserted_rows = 0;
    let transaction = connection.transaction()?;
    {
        let mut statement =
            transaction.prepare("INSERT OR IGNORE INTO download_job (url) VALUES (?1);")?;
        for url in urls {
            inserted_rows += statement.execute(rusqlite::params![url])?;
        }
    }
    transaction.commit()?;
    Ok(inserted_rows)
}

fn load_undownloaded_urls_from_database(
    connection: &mut rusqlite::Connection,
) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection
        .prepare("SELECT url FROM download_job WHERE NOT EXISTS (SELECT 1 FROM result_file WHERE download_job_id = download_job.id) ORDER BY url ASC")?;
    let url_iter = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut urls = Vec::new();
    for url_result in url_iter {
        urls.push(url_result?);
    }
    Ok(urls)
}

fn load_downloaded_urls_from_database(
    connection: &mut rusqlite::Connection,
) -> rusqlite::Result<Vec<(String, BlobDigest)>> {
    let mut statement = connection.prepare(
        "SELECT download_job.url AS url, result_file.sha3_512_digest AS digest FROM download_job, result_file WHERE download_job.id = result_file.download_job_id ORDER BY url, digest ASC",
    )?;
    let url_iter = statement.query_map([], |row| {
        let url = row.get::<_, String>(0)?;
        let digest = row.get::<_, [u8; 64]>(1)?;
        Ok((url, digest))
    })?;
    let mut urls = Vec::new();
    for url_result in url_iter {
        let (url, digest) = url_result?;
        urls.push((url, BlobDigest::new(&digest)));
    }
    Ok(urls)
}

#[derive(Debug, PartialEq, Eq)]
enum SetDownloadJobDigestOutcome {
    Success,
    UrlNotFound,
}

fn find_download_job_id(
    transaction: &mut rusqlite::Transaction,
    url: &str,
) -> rusqlite::Result<Option<i64>> {
    let mut statement = transaction.prepare("SELECT id FROM download_job WHERE url = ?1")?;
    let mut iter = statement.query_map(rusqlite::params![url], |row| row.get::<_, i64>(0))?;
    if let Some(id_result) = iter.next() {
        Ok(Some(id_result?))
    } else {
        Ok(None)
    }
}

fn set_download_job_digests(
    connection: &mut rusqlite::Connection,
    url: &str,
    digests: &[BlobDigest],
) -> rusqlite::Result<SetDownloadJobDigestOutcome> {
    let mut transaction = connection.transaction()?;
    let download_job_id = match find_download_job_id(&mut transaction, url)? {
        Some(id) => id,
        None => return Ok(SetDownloadJobDigestOutcome::UrlNotFound),
    };
    let rows_deleted = transaction.execute(
        "DELETE FROM result_file WHERE download_job_id = ?1",
        rusqlite::params![download_job_id],
    )?;
    if rows_deleted > 0 {
        info!(
            "Deleted {} existing result_file entries for download_job_id {}",
            rows_deleted, download_job_id
        );
    }
    for digest in digests {
        let digest_bytes = digest.to_array();
        let rows_updated = transaction.execute(
            "INSERT OR IGNORE INTO result_file (download_job_id, sha3_512_digest) VALUES(?1, ?2)",
            rusqlite::params![download_job_id, digest_bytes],
        )?;
        match rows_updated {
            0 => {
                unreachable!("We just deleted any matching rows")
            }
            1 => {
                // Successfully inserted
            }
            _ => {
                unreachable!("One INSERT won't affect multiple rows")
            }
        }
    }
    transaction.commit()?;
    Ok(SetDownloadJobDigestOutcome::Success)
}

fn make_database_file_name(config_directory: &std::path::Path) -> std::path::PathBuf {
    config_directory.join("download_manager.sqlite")
}

fn prepare_database(
    config_directory: &std::path::Path,
) -> std::result::Result<rusqlite::Connection, Box<dyn std::error::Error>> {
    let database_path = make_database_file_name(config_directory);
    let connection = match rusqlite::Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    ) {
        Ok(conn) => conn,
        Err(e) => {
            error!(
                "Failed to open or create database file {}: {e}",
                database_path.display()
            );
            return Err(Box::from("Failed to open or create database file"));
        }
    };
    match SQLiteStorage::configure_connection(&connection) {
        Ok(_) => {}
        Err(e) => {
            error!(
                "Failed to configure database connection for file {}: {e}",
                database_path.display()
            );
            return Err(Box::from("Failed to configure database connection"));
        }
    }
    match upgrade_schema(&connection) {
        Ok(_) => {}
        Err(e) => {
            error!(
                "Failed to upgrade database schema for file {}: {e}",
                database_path.display()
            );
            return Err(Box::from("Failed to upgrade database schema"));
        }
    }
    Ok(connection)
}

fn is_relevant_change_to_url_input_file(
    event: &notify::Event,
    url_input_file_path: &std::path::Path,
) -> bool {
    if event.paths.contains(&url_input_file_path.to_path_buf()) {
        match &event.kind {
            notify::EventKind::Modify(modify_kind) => match modify_kind {
                // this generally happens on Windows
                notify::event::ModifyKind::Any => true,
                // this generally happens on Linux
                notify::event::ModifyKind::Data(_) => true,
                notify::event::ModifyKind::Name(rename) => match rename {
                    notify::event::RenameMode::Any => false,
                    notify::event::RenameMode::To => true,
                    notify::event::RenameMode::From => false,
                    // We handle the "To" event which should be sufficient.
                    notify::event::RenameMode::Both => false,
                    notify::event::RenameMode::Other => false,
                },
                _ => false,
            },
            _ => false,
        }
    } else {
        false
    }
}

fn start_watching_url_input_file(
    url_input_file_path: std::path::PathBuf,
) -> notify::Result<(
    RecommendedWatcher,
    JoinHandle<()>,
    tokio::sync::watch::Receiver<()>,
)> {
    let (tx_async, rx_async) = tokio::sync::watch::channel(());
    let (tx_sync, rx_sync) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
    // unfortunately, notify crate does not support async
    let mut watcher = notify::recommended_watcher(tx_sync)?;
    let directory: PathBuf = match url_input_file_path.parent() {
        Some(parent) => parent,
        None => {
            error!("Failed to get parent directory");
            return Err(notify::Error::generic("Failed to get parent directory"));
        }
    }
    .into();
    watcher.watch(&directory, notify::RecursiveMode::Recursive)?;
    info!("Watching directory {} for changes", directory.display());

    if url_input_file_path.exists() {
        // Treat the file as changed initially so that it is read on the first iteration.
        tx_async.send(()).unwrap();
    }

    let watcher_thread = thread::spawn(move || {
        info!("File watcher thread started");
        for res in rx_sync {
            match &res {
                Ok(event) => {
                    let is_relevant =
                        is_relevant_change_to_url_input_file(event, &url_input_file_path);
                    //debug!("Watch event: {:?} (relevant: {})", event, is_relevant);
                    if is_relevant {
                        info!("Relevant watch event: {:?}", event);
                        match tx_async.send(()) {
                            Ok(_) => {}
                            Err(e) => {
                                error!("Failed to send event or error via async channel: {:?}", e)
                            }
                        }
                    }
                }
                Err(e) => error!("Watch error: {:?}", e),
            }
        }
        info!("File watcher thread ending");
    });
    Ok((watcher, watcher_thread, rx_async))
}

async fn read_file_tolerantly(
    url_input_file_path: &Path,
    retry_delay: &std::time::Duration,
) -> (String, u64) {
    let mut attempts = 0;
    loop {
        attempts += 1;
        match tokio::fs::read_to_string(url_input_file_path).await {
            Ok(content) => {
                return (content, attempts);
            }
            Err(e) => {
                warn!(
                    "Failed to read URL input file {}: {e}",
                    url_input_file_path.display()
                );
                info!("Retrying to read URL input file in {retry_delay:?}");
                tokio::time::sleep(*retry_delay).await;
            }
        }
    }
}

async fn read_file_after_next_change(
    url_input_file_path: &Path,
    change_event_receiver: &mut tokio::sync::watch::Receiver<()>,
    retry_delay: &std::time::Duration,
) -> Option<String> {
    match change_event_receiver.changed().await {
        Ok(_) => {
            info!(
                "Detected change to URL input file: {}",
                url_input_file_path.display()
            );
            let (content, attempts) = read_file_tolerantly(url_input_file_path, retry_delay).await;
            info!(
                "Read URL input file with {} Bytes after {} attempt(s)",
                content.len(),
                attempts
            );
            Some(content)
        }
        Err(e) => {
            info!("Change event receiver channel closed: {e}; stopping URL input file monitoring");
            None
        }
    }
}

fn parse_url_input_file(content: &str) -> Vec<String> {
    content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect()
}

async fn keep_reading_url_input_file(
    url_input_file_path: &Path,
    mut input_file_change_event_receiver: tokio::sync::watch::Receiver<()>,
    database_change_event_sender: tokio::sync::watch::Sender<()>,
    connection: &mut rusqlite::Connection,
    retry_delay: &std::time::Duration,
) {
    while let Some(content) = read_file_after_next_change(
        url_input_file_path,
        &mut input_file_change_event_receiver,
        retry_delay,
    )
    .await
    {
        let parsed = parse_url_input_file(&content);
        info!("Parsed {} URLs", parsed.len());
        match store_urls_in_database(parsed, connection) {
            Ok(rows_inserted) => {
                if rows_inserted > 0 {
                    info!("Stored {} URLs in database successfully", rows_inserted);
                    match database_change_event_sender.send(()) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Failed to send database change event: {e}");
                            // A broken channel is not recoverable.
                            break;
                        }
                    }
                } else {
                    info!("No new URLs to store in database");
                }
            }
            Err(e) => {
                error!("Failed to store URLs in database: {e}");
                // A database write error is potentially recoverable, so we don't break here.
            }
        }
    }
}

pub async fn keep_adding_download_job_urls_from_telegram_bot(
    mut download_job_url_receiver: tokio::sync::mpsc::Receiver<String>,
    database_change_event_sender: tokio::sync::watch::Sender<()>,
    connection: &mut rusqlite::Connection,
) {
    while let Some(url) = download_job_url_receiver.recv().await {
        info!("Received URL from Telegram bot: {}", url);
        match store_urls_in_database(vec![url], connection) {
            Ok(_) => {
                info!("Stored URL from Telegram bot in database");
                match database_change_event_sender.send(()) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Failed to send database change event: {e}");
                        // A broken channel is not recoverable.
                        break;
                    }
                }
            }
            Err(e) => {
                error!("Failed to store URL from Telegram bot in database: {e}");
                // A database write error is potentially recoverable, so we don't break here.
            }
        }
    }
}

#[async_trait::async_trait]
trait Download {
    async fn download(&self, url: &str) -> Result<Vec<BlobDigest>, Box<dyn std::error::Error>>;
}

async fn run_download_job(
    connection: &mut rusqlite::Connection,
    download: &dyn Download,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting download job for URL: {}", url);
    match download.download(url).await {
        Ok(digests) => {
            info!("Download job completed successfully for URL: {}", url);
            match set_download_job_digests(connection, url, &digests)? {
                SetDownloadJobDigestOutcome::Success => {
                    info!("Set download job digest successfully for URL: {}", url);
                    Ok(())
                }
                SetDownloadJobDigestOutcome::UrlNotFound => {
                    error!(
                        "URL not found in database when setting digest for URL: {}",
                        url
                    );
                    Err(Box::from("URL not found in database"))
                }
            }
        }
        Err(e) => {
            error!("Download failed for URL: {}: {}", url, e);
            Err(e)
        }
    }
}

async fn keep_downloading_urls_from_database(
    mut database_change_event_receiver: tokio::sync::watch::Receiver<()>,
    connection: &mut rusqlite::Connection,
    download: &dyn Download,
) {
    loop {
        match load_undownloaded_urls_from_database(connection) {
            Ok(urls) => {
                info!("Loaded {} undownloaded URLs from database", urls.len());
                for url in &urls {
                    info!("Loaded URL from the database: {}", url);
                }
                for url in &urls {
                    match run_download_job(connection, download, url).await {
                        Ok(_) => {
                            info!("Download job completed successfully for URL: {}", url);
                        }
                        Err(e) => {
                            error!("Download job failed for URL: {}: {}", url, e);
                        }
                    }
                }
                match load_downloaded_urls_from_database(connection) {
                    Ok(urls) => {
                        info!("Total downloaded URLs: {}", urls.len());
                        for (url, digest) in &urls {
                            debug!("Downloaded URL: {} (digest: {})", url, digest);
                        }
                    }
                    Err(e) => {
                        error!("Failed to load downloaded URLs from database: {}", e);
                    }
                };
            }
            Err(e) => {
                error!("Failed to load undownloaded URLs from database: {}", e);
            }
        }
        match database_change_event_receiver.changed().await {
            Ok(_) => {
                info!("Detected change in database");
            }
            Err(e) => {
                error!(
                    "Database change event receiver closed; stopping URL download: {}",
                    e
                );
                break;
            }
        }
    }
}

async fn run_main_loop(
    config_directory: &std::path::Path,
    url_input_file_path: &std::path::Path,
    url_input_file_event_receiver: tokio::sync::watch::Receiver<()>,
    telegram_bot_download_job_url_receiver: tokio::sync::mpsc::Receiver<String>,
    download: &dyn Download,
    retry_delay: &std::time::Duration,
) {
    let mut database_connection1 = match prepare_database(config_directory) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to prepare database: {e}");
            std::process::exit(1);
        }
    };
    let mut database_connection2 = match prepare_database(config_directory) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to prepare database: {e}");
            std::process::exit(1);
        }
    };
    let mut database_connection3 = match prepare_database(config_directory) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to prepare database: {e}");
            std::process::exit(1);
        }
    };
    let (database_change_sender, database_change_receiver) = tokio::sync::watch::channel::<()>(());
    tokio::join!(
        keep_reading_url_input_file(
            url_input_file_path,
            url_input_file_event_receiver,
            database_change_sender.clone(),
            &mut database_connection1,
            retry_delay
        ),
        keep_adding_download_job_urls_from_telegram_bot(
            telegram_bot_download_job_url_receiver,
            database_change_sender,
            &mut database_connection2,
        ),
        keep_downloading_urls_from_database(
            database_change_receiver,
            &mut database_connection3,
            download
        )
    );
}

fn make_url_input_file_path(config_directory: &std::path::Path) -> std::path::PathBuf {
    config_directory.join("urls.txt")
}

struct TelegramBotRequestHandler {
    download_job_url_sender: tokio::sync::mpsc::Sender<String>,
}

#[async_trait::async_trait]
impl HandleTelegramBotRequests for TelegramBotRequestHandler {
    async fn add_download_job(&self, url: &str) -> Option<String> {
        if let Err(e) = self.download_job_url_sender.send(url.to_string()).await {
            let message = format!("Failed to send download job URL to mpsc queue: {e}");
            error!("{}", message);
            Some(message)
        } else {
            None
        }
    }
}

async fn run_application(
    config_directory: &std::path::Path,
    download: &dyn Download,
    retry_delay: &std::time::Duration,
    telegram_bot: &dyn TelegramBot,
    dropbox: &dyn dropbox::Dropbox,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Download Manager started. Config directory: {}",
        config_directory.display()
    );
    let url_input_file_path = make_url_input_file_path(config_directory);
    let (url_input_file_watcher, url_input_file_watcher_thread, url_input_file_event_receiver) =
        match start_watching_url_input_file(url_input_file_path.clone()) {
            Ok((
                url_input_file_watcher,
                url_input_file_watcher_thread,
                url_input_file_event_receiver,
            )) => {
                info!(
                    "Started watching URL input file: {}",
                    url_input_file_path.display()
                );
                (
                    url_input_file_watcher,
                    url_input_file_watcher_thread,
                    url_input_file_event_receiver,
                )
            }
            Err(e) => {
                error!(
                    "Failed to start watching URL input file {}: {e}",
                    url_input_file_path.display()
                );
                return Err(e.into());
            }
        };
    let (telegram_bot_download_job_url_sender, telegram_bot_download_job_url_receiver) =
        tokio::sync::mpsc::channel(
            /*too much queueing would obscure valuable performance feedback to the user*/ 1,
        );
    let telegram_bot_request_handler = Arc::new(TelegramBotRequestHandler {
        download_job_url_sender: telegram_bot_download_job_url_sender,
    });
    tokio::select! {
        _ =  run_main_loop(
                config_directory,
                &url_input_file_path,
                url_input_file_event_receiver,
                telegram_bot_download_job_url_receiver,
                download,
                retry_delay,
            ) => {
            error!("Main loop has exited unexpectedly");
        }
        _ = telegram_bot.run(telegram_bot_request_handler) => {
            error!("Telegram bot has exited unexpectedly");
        }
        _ = dropbox.keep_moving_files() => {
            error!("Dropbox file mover has exited unexpectedly");
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down...");
        }
    };
    drop(url_input_file_watcher);
    url_input_file_watcher_thread
        .join()
        .expect("Joining the file watcher thread shouldn't fail");
    Ok(())
}

// If working_directory is under dropbox_root, returns the relative path. Otherwise, None.
pub fn find_config_directory_in_dropbox(
    working_directory: &std::path::Path,
    dropbox_root: &std::path::Path,
) -> Option<String> {
    if working_directory.starts_with(dropbox_root) {
        working_directory
            .strip_prefix(dropbox_root)
            .ok()
            .map(|p| format!("/{}", p.to_string_lossy()))
    } else {
        None
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    match dotenv::dotenv() {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to load .env file: {e}. Copy download_manager/.env.template to .env in the working directory and fill in the required values!");
            std::process::exit(1);
        }
    }
    let telegram_api_token = match std::env::var("TELOXIDE_TOKEN") {
        Ok(token) => token,
        Err(e) => {
            error!("Failed to read TELOXIDE_TOKEN from env: {e}");
            std::process::exit(1);
        }
    };
    let download_manager_allowed_telegram_user_var =
        match std::env::var("DOWNLOAD_MANAGER_ALLOWED_TELEGRAM_USER_ID") {
            Ok(variable) => variable,
            Err(e) => {
                error!("Failed to read DOWNLOAD_MANAGER_ALLOWED_TELEGRAM_USER_ID from env: {e}");
                std::process::exit(1);
            }
        };
    let download_manager_allowed_telegram_user = teloxide::types::UserId(
        match download_manager_allowed_telegram_user_var.parse::<u64>() {
            Ok(id) => id,
            Err(e) => {
                error!("Failed to parse DOWNLOAD_MANAGER_ALLOWED_TELEGRAM_USER_ID as u64: {e}");
                std::process::exit(1);
            }
        },
    );
    let dropbox_api_app_key = match std::env::var("DOWNLOAD_MANAGER_DROPBOX_API_APP_KEY") {
        Ok(key) => key,
        Err(e) => {
            error!("Failed to read DOWNLOAD_MANAGER_DROPBOX_API_APP_KEY from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_oauth = match std::env::var("DOWNLOAD_MANAGER_DROPBOX_OAUTH") {
        Ok(oauth) => oauth,
        Err(e) => {
            error!("Failed to read DOWNLOAD_MANAGER_DROPBOX_OAUTH from env: {e}");
            std::process::exit(1);
        }
    };
    let dropbox_root =
        std::path::PathBuf::from(match std::env::var("DOWNLOAD_MANAGER_DROPBOX_ROOT") {
            Ok(root) => root,
            Err(e) => {
                error!("Failed to read DOWNLOAD_MANAGER_DROPBOX_ROOT from env: {e}");
                std::process::exit(1);
            }
        });
    let dropbox_destination = match std::env::var("DOWNLOAD_MANAGER_DROPBOX_DESTINATION") {
        Ok(destination) => destination,
        Err(e) => {
            error!("Failed to read DOWNLOAD_MANAGER_DROPBOX_DESTINATION from env: {e}");
            std::process::exit(1);
        }
    };
    let config_directory =
        std::env::current_dir().expect("Failed to get current working directory");
    info!("Config directory: {}", config_directory.display());
    let config_directory_in_dropbox =
        match find_config_directory_in_dropbox(&config_directory, &dropbox_root) {
            Some(path) => path,
            None => {
                error!("Failed to find config directory in Dropbox");
                std::process::exit(1);
            }
        };
    info!(
        "Config directory in Dropbox: {}",
        config_directory_in_dropbox
    );
    let output_directory = config_directory.join("output");
    info!("Output directory: {}", output_directory.display());
    let output_directory_in_dropbox = join_dropbox_paths(&config_directory_in_dropbox, "output");
    info!(
        "Output directory in Dropbox: {}",
        output_directory_in_dropbox
    );
    match std::fs::create_dir_all(&output_directory) {
        Ok(_) => {}
        Err(e) => {
            error!(
                "Failed to create output directory {}: {e}",
                output_directory.display()
            );
            std::process::exit(1);
        }
    }
    #[cfg(target_os = "linux")]
    let exe_name = "yt-dlp_linux";
    #[cfg(windows)]
    let exe_name = "yt-dlp.exe";
    let yt_dlp_executable_path = config_directory.join(exe_name);
    match yt_dlp::prepare_yt_dlp(&yt_dlp_executable_path).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to prepare yt-dlp: {e}");
            std::process::exit(1);
        }
    }

    let download = yt_dlp::YtDlpDownload {
        executable_path: yt_dlp_executable_path,
        output_directory,
    };
    let retry_delay = std::time::Duration::from_millis(100);
    let telegram_bot = TeloxideTelegramBot {
        telegram_api_token,
        allowed_user: download_manager_allowed_telegram_user,
    };
    let dropbox = RealDropbox {
        dropbox_api_app_key,
        dropbox_oauth: if dropbox_oauth.is_empty() {
            None
        } else {
            Some(dropbox_oauth)
        },
        from_directory: output_directory_in_dropbox,
        into_directory: dropbox_destination,
    };
    match run_application(
        &config_directory,
        &download,
        &retry_delay,
        &telegram_bot,
        &dropbox,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to run application: {e}");
            std::process::exit(1);
        }
    }
}
