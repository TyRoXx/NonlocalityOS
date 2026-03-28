use astraea::{in_memory_storage::InMemoryTreeStorage, tree::BlobDigest};
use bytes::Bytes;
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use dogbox_tree_editor::OpenDirectory;
use dropbox_sdk::{default_async_client::UserAuthDefaultClient, oauth2::Authorization};
use futures::StreamExt;
use pretty_assertions::assert_eq;
use std::{collections::BTreeMap, sync::Arc};
use tracing::{error, info};

async fn clear_or_create_directory(
    dropbox_client: &UserAuthDefaultClient,
    path: &str,
) -> std::io::Result<()> {
    assert!(
        path.len() >= 20,
        "Let's not accidentally delete the root directory or other big directories"
    );
    use dropbox_sdk::async_routes::files;
    match files::delete_v2(dropbox_client, &files::DeleteArg::new(path.to_string())).await {
        Ok(_) => {
            info!("Deleted existing directory at {}", path);
        }
        Err(e) => match &e {
            dropbox_sdk::Error::Api(files::DeleteError::PathLookup(
                files::LookupError::NotFound,
            )) => {
                info!("Directory {} does not exist, will create it", path);
            }
            _ => {
                error!("Error deleting directory {}: {e}", path);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to delete directory {path}: {e}"),
                ));
            }
        },
    }
    match files::create_folder_v2(
        dropbox_client,
        &files::CreateFolderArg::new(path.to_string()),
    )
    .await
    {
        Ok(_) => {
            info!("Created directory at {}", path);
            Ok(())
        }
        Err(e) => {
            error!("Error creating directory {}: {e}", path);
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Failed to create directory {path}: {e}"),
            ))
        }
    }
}

async fn create_file(
    dropbox_client: &UserAuthDefaultClient,
    dropbox_test_directory: &str,
    file_name: &str,
    contents: Bytes,
) -> std::io::Result<()> {
    use dropbox_sdk::async_routes::files;
    match files::upload(
        dropbox_client,
        &files::UploadArg::new(format!("{}/{}", dropbox_test_directory, file_name))
            .with_mode(files::WriteMode::Overwrite),
        contents,
    )
    .await
    {
        Ok(_) => {
            info!("Created file {}/{}", dropbox_test_directory, file_name);
            Ok(())
        }
        Err(e) => {
            error!(
                "Error creating file {}/{}: {e}",
                dropbox_test_directory, file_name
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Failed to create file {}/{}: {e}",
                    dropbox_test_directory, file_name
                ),
            ))
        }
    }
}

async fn verify_import(
    test_case_name: &str,
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_test_directory: &str,
    set_up_test_directory: fn(
        Arc<UserAuthDefaultClient>,
        &str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>,
    >,
    verify_imported_directory: fn(
        Arc<OpenDirectory>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>,
    >,
    expected_digest: &BlobDigest,
) {
    info!("\n==== verify_import: {} ====", test_case_name);
    clear_or_create_directory(&dropbox_client, dropbox_test_directory)
        .await
        .expect("Failed to clear or create Dropbox test directory");
    set_up_test_directory(dropbox_client.clone(), dropbox_test_directory)
        .await
        .expect("Failed to set up Dropbox test directory");
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let importer = dropbox_importer::DropboxImporter {};
    let open_directory = importer
        .import_directory(&dropbox_client, dropbox_test_directory, storage, clock)
        .await
        .expect("Failed to import Dropbox directory");
    let status = open_directory
        .request_save()
        .await
        .expect("Failed to save imported directory");
    verify_imported_directory(open_directory)
        .await
        .expect("Failed to verify imported directory");
    assert!(status.digest.is_digest_up_to_date);
    assert_eq!(expected_digest, status.digest.last_known_digest.digest());
}

async fn assert_directory_entries(
    open_directory: &OpenDirectory,
    expected_entries: &BTreeMap<FileName, DirectoryEntryKind>,
) {
    let mut directory_reader = open_directory.read().await;
    let mut entries = BTreeMap::new();
    while let Some(entry) = directory_reader.next().await {
        entries.insert(entry.name.clone(), entry.kind);
    }
    assert_eq!(entries, *expected_entries);
}

pub async fn test_dropbox_importer(
    dropbox_api_app_key: &str,
    dropbox_oauth: &str,
    dropbox_test_directory: &str,
) {
    let auth = Authorization::load(dropbox_api_app_key.to_string(), dropbox_oauth)
        .expect("Failed to load Dropbox authorization");
    let dropbox_client = Arc::new(UserAuthDefaultClient::new(auth));

    verify_import(
        "Empty directory",
        &dropbox_client,
        dropbox_test_directory,
        |_client: Arc<UserAuthDefaultClient>, _directory: &str| Box::pin(async move { Ok(()) }),
        |imported_directory: Arc<OpenDirectory>| {
            Box::pin(async move {
                assert_directory_entries(&imported_directory, &BTreeMap::new()).await;
                Ok(())
            })
        },
        &BlobDigest::parse_hex_string(concat!(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a27",
            "01a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ))
        .unwrap(),
    )
    .await;

    const HELLO_WORLD_FILE_CONTENT: &str = "Hello, world!";
    verify_import(
        "Directory with one file",
        &dropbox_client,
        dropbox_test_directory,
        |client: Arc<UserAuthDefaultClient>, directory: &str| {
            let directory = directory.to_string();
            Box::pin(async move {
                create_file(
                    &client,
                    &directory,
                    "1.txt",
                    Bytes::from(HELLO_WORLD_FILE_CONTENT),
                )
                .await?;
                Ok(())
            })
        },
        |imported_directory: Arc<OpenDirectory>| {
            Box::pin(async move {
                assert_directory_entries(
                    &imported_directory,
                    &BTreeMap::from([(
                        FileName::try_from("1.txt").unwrap(),
                        DirectoryEntryKind::File(HELLO_WORLD_FILE_CONTENT.as_bytes().len() as u64),
                    )]),
                )
                .await;
                Ok(())
            })
        },
        &BlobDigest::parse_hex_string(concat!(
            "d3d127891bdcd4dd2deceb39391d4f76f13f6fae0fd367c8b20e5eada53b5af2",
            "5663706bc757215e339cc5ef49d7ac9231d367d1b8a8333778ae1bda765caf76"
        ))
        .unwrap(),
    )
    .await;
}
