use astraea::{
    storage::{CommitChanges, LoadRoot, SQLiteStorage, UpdateRoot},
    tree::TREE_BLOB_MAX_LENGTH,
};
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{CacheDropStats, OpenDirectory, OpenDirectoryStatus, WallClock};
use file_system::DogBoxFileSystem;
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, net::SocketAddr, path::Path, pin::Pin, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Handle,
};
use tracing::{debug, error, info};
use tracing_subscriber::fmt::format::FmtSpan;
mod file_system;

#[cfg(test)]
mod file_system_test;

#[cfg(test)]
mod main_test;

async fn serve_connection(stream: TcpStream, dav_server: Arc<DavHandler>) {
    let make_service = move |request: Request<body::Incoming>| {
        debug!("Request: {:?}", &request);
        let dav_server = dav_server.clone();
        async move {
            let response = dav_server.handle(request).await;
            Ok::<_, Infallible>(response)
        }
    };
    let io = TokioIo::new(stream);
    if let Err(err) = http1::Builder::new()
        .max_buf_size(TREE_BLOB_MAX_LENGTH * 500)
        .serve_connection(io, hyper::service::service_fn(make_service))
        .await
    {
        info!("Error serving connection: {:?}", err);
    }
}

async fn handle_tcp_connections(
    listener: TcpListener,
    dav_server: Arc<DavHandler>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
        tokio::task::spawn(async move { serve_connection(stream, dav_server).await });
    }
}

#[derive(Debug, PartialEq)]
enum SaveStatus {
    Saved { files_open_for_writing_count: usize },
    Saving,
}

async fn save_root_regularly(root: Arc<OpenDirectory>, auto_save_interval: std::time::Duration) {
    loop {
        debug!("Time to check if root needs to be saved.");
        let save_result = root.request_save().await;
        match save_result {
            Ok(_status) => {}
            Err(error) => {
                error!("request_save failed with {:?}", &error);
            }
        }
        tokio::time::sleep(auto_save_interval).await;
    }
}

async fn drop_all_read_caches_regularly(
    root: Arc<OpenDirectory>,
    drop_interval: std::time::Duration,
) {
    loop {
        let drop_stats = root.drop_all_read_caches().await;
        if drop_stats != CacheDropStats::new(0, 0, 0) {
            info!("Dropped some read caches: {:?}", &drop_stats);
        }
        tokio::time::sleep(drop_interval).await;
    }
}

fn log_differences(old: &OpenDirectoryStatus, new: &OpenDirectoryStatus) {
    if old.digest != new.digest {
        info!(
            "Root digest changed from {:?} to {:?}",
            &old.digest, &new.digest
        );
    }
    if old.bytes_unflushed_count != new.bytes_unflushed_count {
        info!(
            "Root bytes_unflushed_count changed from {:?} to {:?}",
            &old.bytes_unflushed_count, &new.bytes_unflushed_count
        );
    }
    if old.directories_open_count != new.directories_open_count {
        info!(
            "Root directories_open_count changed from {:?} to {:?}",
            &old.directories_open_count, &new.directories_open_count
        );
    }
    if old.directories_unsaved_count != new.directories_unsaved_count {
        info!(
            "Root directories_unsaved_count changed from {:?} to {:?}",
            &old.directories_unsaved_count, &new.directories_unsaved_count
        );
    }
    if old.files_open_count != new.files_open_count {
        info!(
            "Root files_open_count changed from {:?} to {:?}",
            &old.files_open_count, &new.files_open_count
        );
    }
    if old.files_open_for_writing_count != new.files_open_for_writing_count {
        info!(
            "Root files_open_for_writing_count changed from {:?} to {:?}",
            &old.files_open_for_writing_count, &new.files_open_for_writing_count
        );
    }
    if old.files_unflushed_count != new.files_unflushed_count {
        info!(
            "Root files_unflushed_count changed from {:?} to {:?}",
            &old.files_unflushed_count, &new.files_unflushed_count
        );
    }
}

async fn persist_root_on_change(
    root: Arc<OpenDirectory>,
    root_name: &str,
    blob_storage_update: &(dyn UpdateRoot + Sync),
    blob_storage_commit: Arc<(dyn CommitChanges + Sync + Send)>,
    save_status_sender: tokio::sync::mpsc::Sender<SaveStatus>,
) {
    let mut number_of_no_changes_in_a_row: u64 = 0;
    let mut receiver = root.watch().await;
    let mut previous_root_status: OpenDirectoryStatus = *receiver.borrow();
    loop {
        debug!("Waiting for root to change.");
        let maybe_changed = receiver.changed().await;
        match maybe_changed {
            Ok(_) => {
                debug!("changed() event!");
            }
            Err(error_) => {
                error!("Could not wait for change event: {:?}", &error_);
                return;
            }
        }

        let root_status = *receiver.borrow();
        if previous_root_status == root_status {
            debug!("Root didn't change");
            number_of_no_changes_in_a_row += 1;
            assert_ne!(10, number_of_no_changes_in_a_row);
        } else {
            log_differences(&previous_root_status, &root_status);
            number_of_no_changes_in_a_row = 0;
            if previous_root_status.digest.last_known_digest == root_status.digest.last_known_digest
            {
                debug!("Root status changed, but the last known digest stays the same.");
            } else {
                blob_storage_update
                    .update_root(root_name, &root_status.digest.last_known_digest)
                    .await;
                tokio::task::spawn_blocking({
                     let blob_storage_commit = blob_storage_commit.clone();
                     move || {
                         Handle::current().block_on(  blob_storage_commit.commit_changes()).unwrap(/*TODO*/);
                }})
                .await
                .unwrap();
            }
            let save_status = if root_status.digest.is_digest_up_to_date {
                assert_eq!(0, root_status.bytes_unflushed_count);
                assert_eq!(0, root_status.files_unflushed_count);
                assert_eq!(0, root_status.directories_unsaved_count);
                debug!("Root digest is up to date.");

                match root.request_save().await {
                    Ok(double_checked_status) => {
                        if double_checked_status.digest.is_digest_up_to_date {
                            assert_eq!(0, double_checked_status.bytes_unflushed_count);
                            assert_eq!(0, double_checked_status.files_unflushed_count);
                            assert_eq!(0, double_checked_status.directories_unsaved_count);
                            if double_checked_status == root_status {
                                SaveStatus::Saved {
                                    files_open_for_writing_count: root_status
                                        .files_open_for_writing_count,
                                }
                            } else {
                                info!("It turned out the status has changed in the meantime.");
                                SaveStatus::Saving
                            }
                        } else {
                            info!("It turned out we are in fact saving again.");
                            SaveStatus::Saving
                        }
                    }
                    Err(error) => {
                        error!("Status check failed: {:?}", &error);
                        SaveStatus::Saving
                    }
                }
            } else {
                assert_ne!(0, root_status.directories_unsaved_count);
                debug!("Root digest is not up to date.");
                SaveStatus::Saving
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(10),
                save_status_sender.send(save_status),
            )
            .await
            .unwrap()
            .unwrap();
            previous_root_status = root_status;
        }
    }
}

async fn run_dav_server(
    listener: TcpListener,
    database_file_name: &Path,
    modified_default: std::time::SystemTime,
    clock: WallClock,
    auto_save_interval: std::time::Duration,
) -> Result<
    (
        tokio::sync::mpsc::Receiver<SaveStatus>,
        Pin<
            Box<
                dyn std::future::Future<
                    Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
                >,
            >,
        >,
        Arc<OpenDirectory>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let database_existed = std::fs::exists(&database_file_name).unwrap();
    let sqlite_connection = rusqlite::Connection::open(&database_file_name)?;
    if !database_existed {
        match SQLiteStorage::create_schema(&sqlite_connection) {
            Ok(_) => {}
            Err(error) => {
                info!(
                    "Could not create SQL schema in {}: {:?}",
                    &database_file_name.display(),
                    &error
                );
                info!("Deleting {}", &database_file_name.display());
                std::fs::remove_file(&database_file_name).unwrap();
                panic!();
            }
        }
    }
    let blob_storage_database = Arc::new(SQLiteStorage::from(sqlite_connection)?);
    let root_name = "latest";
    let open_file_write_buffer_in_blocks = 200;
    let root = match blob_storage_database.load_root(&root_name).await {
        Some(found) => {
            OpenDirectory::load_directory(blob_storage_database.clone(), &found, modified_default, clock, open_file_write_buffer_in_blocks).await.unwrap(/*TODO*/)
        }
        None => {
            let dir = Arc::new(
                OpenDirectory::create_directory(blob_storage_database.clone(), clock,
                open_file_write_buffer_in_blocks)
                .await
                .unwrap(/*TODO*/),
            );
            let status = dir.request_save().await.unwrap();
            assert!(status.digest.is_digest_up_to_date);
            blob_storage_database
                .update_root(root_name, &status.digest.last_known_digest)
                .await;
            blob_storage_database.commit_changes().await.unwrap();
            dir
        }
    };
    let tree_editor = dogbox_tree_editor::TreeEditor::new(root.clone(), None);
    let dav_server = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(tree_editor)))
            .locksystem(FakeLs::new())
            .build_handler(),
    );
    let (save_status_sender, save_status_receiver) = tokio::sync::mpsc::channel(6);
    let result = {
        let root = root.clone();
        async move {
            let root = root.clone();
            let join_result = tokio::try_join!(
                {
                    let root = root.clone();
                    async move {
                        save_root_regularly(root.clone(), auto_save_interval).await;
                        Ok(())
                    }
                },
                {
                    let root = root.clone();
                    async move {
                        drop_all_read_caches_regularly(
                            root.clone(),
                            std::time::Duration::from_secs(60 * 5),
                        )
                        .await;
                        Ok(())
                    }
                },
                {
                    let root = root.clone();
                    let blob_storage_database = blob_storage_database.clone();
                    async move {
                        persist_root_on_change(
                            root,
                            &root_name,
                            &*blob_storage_database,
                            blob_storage_database.clone(),
                            save_status_sender,
                        )
                        .await;
                        Ok(())
                    }
                },
                async move {
                    handle_tcp_connections(listener, dav_server).await.unwrap();
                    Ok(())
                }
            );
            join_result.map(|_| ())
        }
    };
    Ok((save_status_receiver, Box::pin(result), root))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let address = SocketAddr::from(([0, 0, 0, 0], 4918));
    let database_file_name = std::env::current_dir()
        .unwrap()
        .join("dogbox_dav_server.sqlite");
    let listener = TcpListener::bind(address).await?;
    info!("Serving on http://{}", address);
    let clock = std::time::SystemTime::now;
    let modified_default = clock();
    {
        let time_string = chrono::DateTime::<chrono::Utc>::from(modified_default).to_rfc3339();
        info!("Last modification time defaults to {}", &time_string);
    }
    let (mut save_status_receiver, server, root_directory) = run_dav_server(
        listener,
        &database_file_name,
        modified_default,
        clock,
        std::time::Duration::from_secs(5),
    )
    .await?;
    tokio::try_join!(server, async move {
        let mut last_save_status = None;
        while let Some(status) = save_status_receiver.recv().await {
            if last_save_status.as_ref() != Some(&status) {
                info!("Save status: {:?}", &status);
                last_save_status = Some(status);
            }
        }
        Ok(())
    })?;
    root_directory.request_save().await.unwrap();
    Ok(())
}
