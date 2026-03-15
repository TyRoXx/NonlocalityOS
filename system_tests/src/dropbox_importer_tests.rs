use astraea::{in_memory_storage::InMemoryTreeStorage, tree::BlobDigest};
use dropbox_sdk::{default_async_client::UserAuthDefaultClient, oauth2::Authorization};
use pretty_assertions::assert_eq;
use std::sync::Arc;
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

pub async fn test_dropbox_importer(
    dropbox_api_app_key: &str,
    dropbox_oauth: &str,
    dropbox_test_directory: &str,
) {
    let importer = dropbox_importer::DropboxImporter {};
    let auth = Authorization::load(dropbox_api_app_key.to_string(), dropbox_oauth)
        .expect("Failed to load Dropbox authorization");
    let dropbox_client = UserAuthDefaultClient::new(auth);
    clear_or_create_directory(&dropbox_client, dropbox_test_directory)
        .await
        .expect("Failed to clear or create Dropbox test directory");
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let open_directory = importer
        .import_directory(&dropbox_client, dropbox_test_directory, storage, clock)
        .await
        .expect("Failed to import Dropbox directory");
    let status = open_directory
        .request_save()
        .await
        .expect("Failed to save imported directory");
    assert!(status.digest.is_digest_up_to_date);
    assert_eq!(
        &BlobDigest::parse_hex_string(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a2701a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2")
            .unwrap(),
        status.digest.last_known_digest.digest()
    );
}
