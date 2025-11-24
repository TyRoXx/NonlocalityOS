use astraea::storage::SQLiteStorage;
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::result::Result<std::process::ExitCode, Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let executable_path = std::env::current_exe().expect("Failed to get current executable path");
    let working_directory =
        std::env::current_dir().expect("Failed to get current working directory");
    info!(
        "Download Manager started. Executable path: {}, Working directory: {}",
        executable_path.display(),
        working_directory.display()
    );
    if !is_file_located_in_directory(&executable_path, &working_directory)? {
        info!("For simplicity sake, the executable is expected to be located in the current working directory.");
        error!(
            "Executable path {} is not located within the working directory {}",
            executable_path.display(),
            working_directory.display()
        );
        return Ok(std::process::ExitCode::from(1));
    }
    let database_path = working_directory.join("download_manager.sqlite");
    let connection = rusqlite::Connection::open_with_flags(
        &database_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
    )
    .map_err(|e| {
        error!(
            "Failed to open or create database file {}: {e}",
            database_path.display()
        );
        e
    })?;
    SQLiteStorage::configure_connection(&connection)?;
    upgrade_schema(&connection)?;
    todo!()
}
