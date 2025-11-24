use std::{
    path::PathBuf,
    thread::{self, JoinHandle},
};

use astraea::storage::SQLiteStorage;
use clap::Parser;
use notify::{ReadDirectoryChangesWatcher, Watcher};
use tracing::{error, info};

#[cfg(test)]
mod main_tests;

fn is_file_located_in_directory(
    file_path: &std::path::Path,
    directory_path: &std::path::Path,
) -> std::io::Result<bool> {
    match file_path.canonicalize() {
        Ok(canonical_file_path) => match directory_path.canonicalize() {
            Ok(canonical_directory_path) => {
                Ok(canonical_file_path.starts_with(&canonical_directory_path))
            }
            Err(e) => {
                error!(
                    "Failed to canonicalize directory path {}: {e}",
                    directory_path.display()
                );
                Err(e)
            }
        },
        Err(e) => {
            error!(
                "Failed to canonicalize file path {}: {e}",
                file_path.display()
            );
            Err(e)
        }
    }
}

fn upgrade_schema(
    connection: &rusqlite::Connection,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let user_version =
        connection.query_row("PRAGMA user_version;", [], |row| row.get::<_, i32>(0))?;
    match user_version {
        0 => {
            let query = "CREATE TABLE download_job (
                id INTEGER PRIMARY KEY NOT NULL,
                url TEXT UNIQUE NOT NULL,
                sha3_512_digest BLOB,
                CONSTRAINT sha3_512_digest_length_check CHECK ((sha3_512_digest IS NULL) OR (LENGTH(sha3_512_digest) == 64))
            ) STRICT";
            connection
                .execute(&query, ())
                .map(|size| assert_eq!(0, size))?;
            connection.execute("PRAGMA user_version = 1;", ())?;
            Ok(())
        }
        1 => {
            // Future migrations go here
            Ok(())
        }
        _ => {
            error!("Unsupported database schema version: {}", user_version);
            return Err(Box::from("Unsupported database schema version"));
        }
    }
}

fn prepare_database(
    working_directory: &std::path::Path,
) -> std::result::Result<rusqlite::Connection, Box<dyn std::error::Error>> {
    let database_path = working_directory.join("download_manager.sqlite");
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

fn start_watching_url_input_file(
    url_input_file_path: &std::path::Path,
) -> notify::Result<(
    ReadDirectoryChangesWatcher,
    JoinHandle<()>,
    tokio::sync::mpsc::Receiver<notify::Result<notify::Event>>,
)> {
    let (tx_async, rx_async) = tokio::sync::mpsc::channel::<notify::Result<notify::Event>>(1);
    let (tx_sync, rx_sync) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
    // unfortunately, notify crate does not support async
    let mut watcher = notify::recommended_watcher(tx_sync)?;
    let directory: PathBuf = url_input_file_path
        .parent()
        .expect("Failed to get parent directory")
        .into();
    watcher.watch(&directory, notify::RecursiveMode::Recursive)?;
    info!("Watching directory {} for changes", directory.display());
    let watcher_thread = thread::spawn(move || {
        info!("File watcher thread started");
        for res in rx_sync {
            match &res {
                Ok(event) => info!("Watch event: {:?}", event),
                Err(e) => error!("Watch error: {:?}", e),
            }
            match tx_async.blocking_send(res) {
                Ok(_) => {}
                Err(e) => error!("Failed to send event or error via async channel: {:?}", e),
            }
        }
        info!("File watcher thread ending");
    });
    Ok((watcher, watcher_thread, rx_async))
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    output: std::path::PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    let command_line_arguments = Args::parse();
    let output_directory = command_line_arguments.output;
    match std::fs::create_dir_all(&output_directory) {
        Ok(_) => {}
        Err(e) => {
            error!(
                "Failed to create output directory {}: {e}",
                output_directory.display()
            );
            return;
        }
    }
    let executable_path = std::env::current_exe().expect("Failed to get current executable path");
    let working_directory =
        std::env::current_dir().expect("Failed to get current working directory");
    info!(
        "Download Manager started. Executable path: {}, Working directory: {}",
        executable_path.display(),
        working_directory.display()
    );
    match is_file_located_in_directory(&executable_path, &working_directory) {
        Ok(result) => {
            if !result {
                info!("For simplicity sake, the executable is expected to be located in the current working directory.");
                error!(
                    "Executable path {} is not located within the working directory {}",
                    executable_path.display(),
                    working_directory.display()
                );
                todo!()
            }
        }
        Err(_e) => {
            todo!()
        }
    }
    let database_connection = match prepare_database(&working_directory) {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to prepare database: {e}");
            todo!()
        }
    };
    let url_input_file_path = working_directory.join("urls.txt");
    let (url_input_file_watcher, url_input_file_watcher_thread, url_input_file_event_receiver) =
        match start_watching_url_input_file(&url_input_file_path) {
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
                return ();
            }
        };
    url_input_file_watcher_thread
        .join()
        .expect("Joining the file watcher thread shouldn't fail");
    todo!()
}
