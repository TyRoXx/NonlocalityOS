use astraea::{
    in_memory_storage::InMemoryTreeStorage,
    tree::{BlobDigest, TREE_BLOB_MAX_LENGTH},
};
use bytes::Bytes;
use dogbox_tree::serialization::FileName;
use dogbox_tree_editor::{
    expected_directory_entry_kind::{assert_directory_contents, ExpectedDirectoryEntryKind},
    OpenDirectory,
};
use dropbox_importer::{
    dropbox_api::RealDropboxApi,
    file_cache::{FileCacheMap, PersistableFileCacheEntry, Sha256ChunkCacheKey},
    importer::import_directory,
};
use dropbox_sdk::{default_async_client::UserAuthDefaultClient, oauth2::Authorization};
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

// Small chunk size so that we can test multi-chunk downloads in a reasonable amount of time.
const SMALLER_CHUNK_SIZE: u64 = 2 * (TREE_BLOB_MAX_LENGTH as u64);

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
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>,
    >,
    expected_digest: &BlobDigest,
    expected_file_cache_entries: u64,
) {
    info!("\n==== verify_import: {} ====", test_case_name);
    clear_or_create_directory(dropbox_client, dropbox_test_directory)
        .await
        .expect("Failed to clear or create Dropbox test directory");
    set_up_test_directory(dropbox_client.clone(), dropbox_test_directory)
        .await
        .expect("Failed to set up Dropbox test directory");
    let storage = Arc::new(InMemoryTreeStorage::empty());
    let clock = Arc::new(|| std::time::SystemTime::UNIX_EPOCH);
    let dropbox_api = RealDropboxApi {
        dropbox_client: dropbox_client.clone(),
    };
    // We do not reuse the cache across test runs because we want to test Dropbox API calls.
    let download_cache_tree = sorted_tree::prolly_tree_editable_node::EditableNode::<
        Sha256ChunkCacheKey,
        PersistableFileCacheEntry,
    >::new();
    let chunk_size = SMALLER_CHUNK_SIZE;
    let download_cache = FileCacheMap::new(download_cache_tree, &*storage, chunk_size);
    let open_directory = import_directory(
        dropbox_test_directory,
        storage.clone(),
        clock,
        &dropbox_api,
        &download_cache,
    )
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
    assert_eq!(
        download_cache.number_of_entries().await.unwrap(),
        expected_file_cache_entries
    );
}

async fn create_and_import_and_verify(
    test_case_name: &str,
    dropbox_client: &Arc<UserAuthDefaultClient>,
    dropbox_test_directory: &str,
    entries: BTreeMap<FileName, ExpectedDirectoryEntryKind>,
    expected_digest: &BlobDigest,
    expected_file_cache_entries: u64,
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
        |imported_directory: Arc<OpenDirectory>| {
            Box::pin(async move {
                assert_directory_contents(&imported_directory, &entries).await;
                Ok(())
            })
        },
        expected_digest,
        expected_file_cache_entries,
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
        |imported_directory: Arc<OpenDirectory>| {
            Box::pin(async move {
                assert_directory_contents(&imported_directory, &expected_entries).await;
                Ok(())
            })
        },
        &BlobDigest::parse_hex_string(concat!(
            "d3d127891bdcd4dd2deceb39391d4f76f13f6fae0fd367c8b20e5eada53b5af2",
            "5663706bc757215e339cc5ef49d7ac9231d367d1b8a8333778ae1bda765caf76"
        ))
        .unwrap(),
        1,
    )
    .await;
}

fn random_bytes(len: usize) -> Bytes {
    use rand::rngs::SmallRng;
    use rand::Rng;
    use rand::SeedableRng;
    let mut small_rng = SmallRng::seed_from_u64(123);
    Bytes::from_iter((0..len).map(|_| small_rng.gen()))
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
        0,
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
        0,
    )
    .await;

    create_and_import_and_verify(
        "Directory with one medium file",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("1.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(random_bytes(
                // Let's test a file that's larger than one tree blob.
                (TREE_BLOB_MAX_LENGTH * 2) + 1,
            )),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "26a61f26302d919de0b46d7993b762c35e45f3b373340c94e1964a332f495bb7",
            "32bfef97f8fa47396511abba4709d05c8ff6955ff41ada2a7edf3aab58988106"
        ))
        .unwrap(),
        2,
    )
    .await;

    create_and_import_and_verify(
        "Directory with one large file",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from([(
            FileName::try_from("1.txt").unwrap(),
            ExpectedDirectoryEntryKind::File(random_bytes((SMALLER_CHUNK_SIZE as usize * 2) + 1)),
        )]),
        &BlobDigest::parse_hex_string(concat!(
            "8c516cf5ec6bf00aac95f6c595330e398b4515d6d1a19b278c3bdca951cc6805",
            "569b6efa9c9b73cace24d7d461b21bbdca6afee67de21eadf786b3eae24aad0b"
        ))
        .unwrap(),
        3,
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
        0,
    )
    .await;

    create_and_import_and_verify(
        "Directory with several different files",
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
        5,
    )
    .await;

    // test file download caching
    create_and_import_and_verify(
        "Directory with several equal files",
        &dropbox_client,
        dropbox_test_directory,
        BTreeMap::from_iter((1..=3).map(|i| {
            (
                FileName::try_from(format!("{}.txt", i)).unwrap(),
                ExpectedDirectoryEntryKind::File(Bytes::from(
                    "This is the same content for all files",
                )),
            )
        })),
        &BlobDigest::parse_hex_string(concat!(
            "c9c5fded1d6abccfa626ca711483e2fd6bb49fffab2a40d12ac2394f87d77820",
            "5cd08bf9d2271af7eb7725d67bfb92e7eadb9237d2842b127146012651e9406b"
        ))
        .unwrap(),
        1,
    )
    .await;

    verify_illegal_character_handling(&dropbox_client, dropbox_test_directory).await;
}
