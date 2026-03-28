use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    storage::StrongReference,
    tree::{BlobDigest, TREE_BLOB_MAX_LENGTH},
};
use bytes::Bytes;
use dogbox_tree::serialization::{DirectoryEntryKind, FileName};
use dogbox_tree_editor::{FileCreationMode, OpenDirectory, OpenFile, TreeEditor};
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
                return Err(std::io::Error::other(format!(
                    "Failed to delete directory {path}: {e}"
                )));
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
            Err(std::io::Error::other(format!(
                "Failed to create directory {path}: {e}"
            )))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum ExpectedDirectoryEntryKind {
    Directory(BTreeMap<FileName, ExpectedDirectoryEntryKind>),
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
            Err(std::io::Error::other(format!(
                "Failed to create file {}/{}: {e}",
                dropbox_test_directory, file_name
            )))
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
            ExpectedDirectoryEntryKind::Directory(entries) => {
                let path = format!("{}/{}", dropbox_test_directory, file_name.as_str());
                use dropbox_sdk::async_routes::files;
                files::create_folder_v2(dropbox_client, &files::CreateFolderArg::new(path.clone()))
                    .await
                    .map_err(|e| {
                        error!("Error creating directory {}: {e}", path);
                        std::io::Error::other(format!("Failed to create directory {path}: {e}"))
                    })?;
                Box::pin(create_directory_contents(dropbox_client, &path, entries)).await?;
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
    clear_or_create_directory(dropbox_client, dropbox_test_directory)
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
    let open_directory =
        dropbox_importer::import_directory(dropbox_client, dropbox_test_directory, storage, clock)
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

async fn read_to_end(open_file: &OpenFile) -> std::io::Result<Bytes> {
    let read_permission = open_file.get_read_permission();
    let file_size = open_file.size().await;
    let mut total_bytes_read = 0u64;
    let mut result = Bytes::new();
    while total_bytes_read < file_size {
        let bounded_size = usize::try_from(std::cmp::min(
            file_size - total_bytes_read,
            TREE_BLOB_MAX_LENGTH as u64,
        ))
        .unwrap();
        let buffer = open_file
            .read_bytes(&read_permission, total_bytes_read, bounded_size)
            .await
            .map_err(|e| {
                error!("Error reading file at offset {}: {e}", total_bytes_read);
                std::io::Error::other(format!(
                    "Failed to read file at offset {}: {e}",
                    total_bytes_read
                ))
            })?;
        total_bytes_read += buffer.len() as u64;
        result = [result, buffer].concat().into();
    }
    Ok(result)
}

async fn read_directory_recursively(
    open_directory: &Arc<OpenDirectory>,
    empty_file_reference: &StrongReference,
) -> BTreeMap<FileName, ExpectedDirectoryEntryKind> {
    let mut directory_reader = open_directory.read().await;
    let mut entries = BTreeMap::new();
    while let Some(entry) = directory_reader.next().await {
        let kind = match entry.kind {
            DirectoryEntryKind::Directory => {
                let open_subdirectory = open_directory
                    .clone()
                    .open_subdirectory(entry.name.clone())
                    .await
                    .expect("Failed to open subdirectory");
                let sub_entries = Box::pin(read_directory_recursively(
                    &open_subdirectory,
                    empty_file_reference,
                ))
                .await;
                ExpectedDirectoryEntryKind::Directory(sub_entries)
            }
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
                let read_content = read_to_end(&open_file)
                    .await
                    .expect("Failed to read file content");
                ExpectedDirectoryEntryKind::File(read_content)
            }
        };
        entries.insert(entry.name.clone(), kind);
    }
    entries
}

async fn assert_directory_contents(
    open_directory: &Arc<OpenDirectory>,
    expected_entries: &BTreeMap<FileName, ExpectedDirectoryEntryKind>,
    empty_file_reference: &StrongReference,
) {
    let entries = read_directory_recursively(open_directory, empty_file_reference).await;
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
        dropbox_client,
        dropbox_test_directory,
        {
            let entries = entries.clone();
            |client: Arc<UserAuthDefaultClient>, directory: &str| {
                let directory = directory.to_string();
                Box::pin(async move {
                    create_directory_contents(&client, &directory, &entries).await?;
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

async fn verify_illegal_character_handling(
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_test_directory: &str,
) {
    let expected_entries = BTreeMap::from([(
        FileName::try_from("1.txt").unwrap(),
        ExpectedDirectoryEntryKind::File(Bytes::from("Hello, world!")),
    )]);
    verify_import(
        "Illegal character handling",
        dropbox_client,
        dropbox_test_directory,
        {
            let expected_entries = expected_entries.clone();
            |client: Arc<UserAuthDefaultClient>, directory: &str| {
                let directory = directory.to_string();
                Box::pin(async move {
                    create_directory_contents(&client, &directory, &expected_entries).await?;
                    create_file(
                        &client,
                        &directory,
                        "illegal_in_dogbox_>.<",
                        Bytes::from("test"),
                    )
                    .await?;
                    let subdirectory_path = format!("{}/{}", directory, "|");
                    use dropbox_sdk::async_routes::files;
                    files::create_folder_v2(
                        client.as_ref(),
                        &files::CreateFolderArg::new(subdirectory_path.clone()),
                    )
                    .await
                    .map_err(|e| {
                        error!("Error creating directory {}: {e}", subdirectory_path);
                        std::io::Error::other(format!(
                            "Failed to create directory {subdirectory_path}: {e}"
                        ))
                    })?;
                    Ok(())
                })
            }
        },
        |imported_directory: Arc<OpenDirectory>, empty_file_reference: &StrongReference| {
            let empty_file_reference = empty_file_reference.clone();
            Box::pin(async move {
                assert_directory_contents(
                    &imported_directory,
                    &expected_entries,
                    &empty_file_reference,
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

    create_and_import_and_verify(
        "Empty subdirectory",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("sub").unwrap(),
            ExpectedDirectoryEntryKind::Directory(BTreeMap::new()),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "b275ce35f86326429e948f66f69c42f78358c371c02761ad6628e963dcf6a1fe",
            "7d8f8f87ed9cb78cdd2025f22b7c2262ef1b70ed69da7bcd032c91dc2831e9c8"
        ))
        .unwrap(),
    )
    .await;

    create_and_import_and_verify(
        "Directory with one file",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("1.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(Bytes::from_iter(std::iter::repeat_n(
                0u8,
                // Let's test a file that's larger than the chunk size used in the importer to make sure chunking works correctly.
                (TREE_BLOB_MAX_LENGTH * 2) + 1,
            ))),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "06a969e16edb31e7d384d87af0c30e316122e9f3d616bec3a165cd5a24c86751",
            "9355e54e8dc37530b1be8f512f2b917cb7e5142b3609ed01aea549b1270eb225"
        ))
        .unwrap(),
    )
    .await;

    create_and_import_and_verify(
        "Subdirectory with one file",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("sub").unwrap(),
            ExpectedDirectoryEntryKind::Directory(BTreeMap::from([(
                FileName::try_from("1.txt").unwrap(),
                ExpectedDirectoryEntryKind::File(/*test an empty file*/ Bytes::new()),
            )])),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "fc33471a22764870c4a6d3d34c3ab22ebf9e5184b3a82ad13ab11d621c943992",
            "b51476eb0db6551e0da6d23d0e6ce3603793b46958b902b42b417f26e6119019"
        ))
        .unwrap(),
    )
    .await;

    create_and_import_and_verify(
        "Directory with several files",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from_iter((1..=5).map(|i| {
            (
                FileName::try_from(format!("{}.txt", i)).unwrap(),
                ExpectedDirectoryEntryKind::File(Bytes::from(format!("This is file number {}", i))),
            )
        })),
        &BlobDigest::parse_hex_string(concat!(
            "eb43c6b8ae832f0c031661ddea8aca491deeb9aa0fc6f6314c70baefdfdae35c",
            "7821a9c978070176428428c61da996ad5022ac82ad3f82c4a70d112f6d2f318c"
        ))
        .unwrap(),
    )
    .await;

    verify_illegal_character_handling(&dropbox_client, dropbox_test_directory).await;
}
