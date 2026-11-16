use astraea::{sqlite_storage::SQLiteStorage, storage::CollectGarbage};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    name = "astraea_cli",
    about = "Tool for managing NonlocalityOS tree databases"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Verify integrity of database contents.
    Check {
        /// Path to the SQLite3 database file
        #[arg(value_name = "DATABASE_PATH", value_parser = clap::value_parser!(std::path::PathBuf))]
        database_path: std::path::PathBuf,

        /// Maximum number of trees to check
        #[arg(value_name = "LIMIT_TREE_COUNT", value_parser = clap::value_parser!(i64))]
        limit_tree_count: Option<i64>,
    },
    /// Perform garbage collection on the database.
    Gc {
        /// Path to the SQLite3 database file
        #[arg(value_name = "DATABASE_PATH", value_parser = clap::value_parser!(std::path::PathBuf))]
        database_path: std::path::PathBuf,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match &cli.command {
        Commands::Check {
            database_path,
            limit_tree_count,
        } => {
            info!("Checking database at {:?}", database_path);
            let database = rusqlite::Connection::open_with_flags(
                database_path,
                // to do: Make this work read-only
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .expect("Failed to open existing database");
            // disable automatic GC for performance reasons and because this tool should change the database as little as possible
            let is_automatic_collection_enabled = false;
            let storage = SQLiteStorage::new_with_configuration(
                database,
                is_automatic_collection_enabled,
                None,
            )
            .expect("Failed to configure database connection");
            let inconsistencies = storage
                .find_inconsistencies(limit_tree_count)
                .await
                .unwrap();
            for inconsistency in inconsistencies.iter() {
                info!("Found inconsistency: {:?}", inconsistency);
            }
            if inconsistencies.is_empty() {
                info!("No inconsistencies found");
            } else {
                warn!("Found {} inconsistencies", inconsistencies.len());
                std::process::exit(1);
            }
        }
        Commands::Gc { database_path } => {
            info!(
                "Performing garbage collection on database at {:?}",
                database_path
            );
            let database = rusqlite::Connection::open_with_flags(
                database_path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .expect("Failed to open existing database");
            let storage =
                SQLiteStorage::from(database).expect("Failed to configure database connection");
            let stats = storage.collect_some_garbage().await.unwrap();
            info!("Garbage collection stats: {:?}", stats);
        }
    }
}
