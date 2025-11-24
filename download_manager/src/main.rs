use astraea::storage::SQLiteStorage;
use clap::Parser;
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

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    output: std::path::PathBuf,
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
    todo!()
}
