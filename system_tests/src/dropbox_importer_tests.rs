use astraea::{in_memory_storage::InMemoryTreeStorage, storage::StrongReference, tree::BlobDigest};
use bytes::Bytes;
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use dogbox_tree_editor::{FileCreationMode, OpenDirectory, TreeEditor};
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

#[derive(Debug, PartialEq, Eq, Clone)]
enum ExpectedDirectoryEntryKind {
    Directory,
    File(Bytes),
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

async fn create_directory_contents(
    dropbox_client: &UserAuthDefaultClient,
    dropbox_test_directory: &str,
    expected_entries: &BTreeMap<FileName, ExpectedDirectoryEntryKind>,
) -> std::io::Result<()> {
    for (file_name, kind) in expected_entries {
        match kind {
            ExpectedDirectoryEntryKind::Directory => {
                let path = format!("{}/{}", dropbox_test_directory, file_name.as_str());
                use dropbox_sdk::async_routes::files;
                files::create_folder_v2(dropbox_client, &files::CreateFolderArg::new(path.clone()))
                    .await
                    .map_err(|e| {
                        error!("Error creating directory {}: {e}", path);
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to create directory {path}: {e}"),
                        )
                    })?;
            }
            ExpectedDirectoryEntryKind::File(contents) => {
                create_file(
                    dropbox_client,
                    dropbox_test_directory,
                    file_name.as_str(),
                    contents.clone(),
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn verify_import(
    test_case_name: &str,
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_test_directory: &str,
    set_up_test_directory: impl FnOnce(
        Arc<UserAuthDefaultClient>,
        &str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>,
    >,
    verify_imported_directory: impl FnOnce(
        Arc<OpenDirectory>,
        &StrongReference,
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
    let empty_file_reference = TreeEditor::store_empty_file(storage.clone())
        .await
        .expect("Storing an empty file should succeed");
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
    verify_imported_directory(open_directory, &empty_file_reference)
        .await
        .expect("Failed to verify imported directory");
    assert!(status.digest.is_digest_up_to_date);
    assert_eq!(expected_digest, status.digest.last_known_digest.digest());
}

async fn assert_directory_contents(
    open_directory: &Arc<OpenDirectory>,
    expected_entries: &BTreeMap<FileName, ExpectedDirectoryEntryKind>,
    empty_file_reference: &StrongReference,
) {
    let mut directory_reader = open_directory.read().await;
    let mut entries = BTreeMap::new();
    while let Some(entry) = directory_reader.next().await {
        let kind = match entry.kind {
            DirectoryEntryKind::Directory => ExpectedDirectoryEntryKind::Directory,
            DirectoryEntryKind::File(size) => {
                let open_file = open_directory
                    .clone()
                    .open_file(
                        &entry.name,
                        empty_file_reference,
                        FileCreationMode::open_existing(),
                    )
                    .await
                    .expect("Failed to open file");
                assert_eq!(size, open_file.size().await);
                let read_permission = open_file.get_read_permission();
                // Let's not read files larger than 10 MiB in tests
                let bounded_size = usize::try_from(std::cmp::min(size, 10 * 1024 * 1024)).unwrap();
                let read_content = open_file
                    .read_bytes(&read_permission, 0, bounded_size)
                    .await
                    .expect("Reading should succeed");
                assert_eq!(size, read_content.len() as u64);
                ExpectedDirectoryEntryKind::File(read_content)
            }
        };
        entries.insert(entry.name.clone(), kind);
    }
    assert_eq!(entries, *expected_entries);
}

async fn create_and_import_and_verify(
    test_case_name: &str,
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_test_directory: &str,
    entries: BTreeMap<FileName, ExpectedDirectoryEntryKind>,
    expected_digest: &BlobDigest,
) {
    verify_import(
        test_case_name,
        &dropbox_client,
        dropbox_test_directory,
        {
            let several_files = entries.clone();
            |client: Arc<UserAuthDefaultClient>, directory: &str| {
                let directory = directory.to_string();
                Box::pin(async move {
                    create_directory_contents(&client, &directory, &several_files).await?;
                    Ok(())
                })
            }
        },
        |imported_directory: Arc<OpenDirectory>, empty_file_reference: &StrongReference| {
            let empty_file_reference = empty_file_reference.clone();
            Box::pin(async move {
                assert_directory_contents(&imported_directory, &entries, &empty_file_reference)
                    .await;
                Ok(())
            })
        },
        expected_digest,
    )
    .await;
}

pub async fn test_dropbox_importer(
    dropbox_api_app_key: &str,
    dropbox_oauth: &str,
    dropbox_test_directory: &str,
) {
    let auth = Authorization::load(dropbox_api_app_key.to_string(), dropbox_oauth)
        .expect("Failed to load Dropbox authorization");
    let dropbox_client = Arc::new(UserAuthDefaultClient::new(auth));

    create_and_import_and_verify(
        "Empty directory",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::new(),
        &BlobDigest::parse_hex_string(concat!(
            "ddc92a915fca9a8ce7eebd29f715e8c6c7d58989090f98ae6d6073bbb04d7a27",
            "01a541d1d64871c4d8773bee38cec8cb3981e60d2c4916a1603d85a073de45c2"
        ))
        .unwrap(),
    )
    .await;

    const HELLO_WORLD_FILE_CONTENT: &str = "Hello, world!";

    create_and_import_and_verify(
        "Directory with one file",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("1.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(Bytes::from(HELLO_WORLD_FILE_CONTENT)),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "d3d127891bdcd4dd2deceb39391d4f76f13f6fae0fd367c8b20e5eada53b5af2",
            "5663706bc757215e339cc5ef49d7ac9231d367d1b8a8333778ae1bda765caf76"
        ))
        .unwrap(),
    )
    .await;

    create_and_import_and_verify(
        "Directory with several files",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from_iter((1..=10).map(|i| {
            (
                FileName::try_from(format!("{}.txt", i)).unwrap(),
                ExpectedDirectoryEntryKind::File(Bytes::from(HELLO_WORLD_FILE_CONTENT)),
            )
        })),
        &BlobDigest::parse_hex_string(concat!(
            "3e76abda096565975ed4a5db425a4c5fae376ffc943673830e90e24ec772b702",
            "e8d88c01d86292f465cc59bbca8456392bbd1d692c34656eacc276a4810ce77d"
        ))
        .unwrap(),
    )
    .await;
}
