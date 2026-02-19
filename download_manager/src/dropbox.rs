use dropbox_sdk::async_routes::files;
use dropbox_sdk::default_async_client::{NoauthDefaultClient, UserAuthDefaultClient};
use dropbox_sdk::oauth2::{Authorization, AuthorizeUrlBuilder, Oauth2Type, PkceCode};
use tokio::io::AsyncBufReadExt;
use tracing::{debug, error, info, warn};

#[async_trait::async_trait]
pub trait Dropbox {
    async fn keep_moving_files(&self);
}

pub struct RealDropbox {
    pub dropbox_api_app_key: String,
    pub dropbox_oauth: Option<String>,
    pub from_directory: String,
    pub into_directory: String,
}

#[async_trait::async_trait]
impl Dropbox for RealDropbox {
    async fn keep_moving_files(&self) {
        run_dropbox_file_mover(
            &self.dropbox_api_app_key,
            self.dropbox_oauth.as_deref(),
            &self.from_directory,
            &self.into_directory,
        )
        .await;
    }
}

async fn authenticate(
    dropbox_api_app_key: &str,
    dropbox_oauth: Option<&str>,
) -> Option<Authorization> {
    match dropbox_oauth
        .and_then(|oauth_str| Authorization::load(dropbox_api_app_key.to_string(), oauth_str))
    {
        Some(oauth) => Some(oauth),
        None => {
            let oauth2_flow = Oauth2Type::PKCE(PkceCode::new());
            let url = AuthorizeUrlBuilder::new(dropbox_api_app_key, &oauth2_flow).build();
            info!("Open this URL in your browser: {}", url);
            info!("Then paste the code here");
            let mut input = String::new();
            let stdin = tokio::io::stdin();
            let mut reader = tokio::io::BufReader::new(stdin);
            reader
                .read_line(&mut input)
                .await
                .expect("Failed to read from stdin");
            let auth_code = input.trim().to_owned();
            let mut auth = Authorization::from_auth_code(
                dropbox_api_app_key.to_string(),
                oauth2_flow,
                auth_code.trim().to_owned(),
                None,
            );
            match auth
                .obtain_access_token_async(NoauthDefaultClient::default())
                .await
            {
                Ok(result) => {
                    info!("Successfully obtained access token: {}", result);
                }
                Err(e) => {
                    error!("Error obtaining access token: {e}");
                    return None;
                }
            }
            info!(
                "Set this variable in your .env: DROPBOX_OAUTH={}",
                auth.save()
                    .expect("Saving should work because we just authenticated successfully")
            );
            Some(auth)
        }
    }
}

pub fn is_file_to_be_moved(name: &str) -> bool {
    let lower_case_name = name.to_lowercase();
    lower_case_name.ends_with(".mp4")
        || lower_case_name.ends_with(".mov")
        || lower_case_name.ends_with(".webm")
        || lower_case_name.ends_with(".mkv")
}

pub fn join_dropbox_paths(left: &str, right: &str) -> String {
    let mut result = left.to_string();
    if !result.ends_with('/') {
        result.push('/');
    }
    result.push_str(right.trim_start_matches('/'));
    result
}

async fn get_file_content_hash(
    dropbox_client: &UserAuthDefaultClient,
    file_path: &str,
) -> Option<String> {
    let metadata = match files::get_metadata(
        dropbox_client,
        &files::GetMetadataArg::new(file_path.to_string()).with_include_deleted(true),
    )
    .await
    {
        Ok(metadata) => metadata,
        Err(error) => {
            error!("Error getting metadata for {}: {error}", file_path);
            return None;
        }
    };
    let file_metadata = match metadata {
        files::Metadata::File(file_metadata) => file_metadata,
        files::Metadata::Folder(folder_metadata) => {
            error!(
                "Expected file but got folder for path {}: {:?}",
                file_path, folder_metadata
            );
            return None;
        }
        files::Metadata::Deleted(deleted_metadata) => {
            error!(
                "Expected file but got deleted entry for path {}: {:?}",
                file_path, deleted_metadata
            );
            return None;
        }
    };
    let content_hash = match file_metadata.content_hash {
        Some(digest) => digest,
        None => {
            error!(
                "File metadata does not contain content hash for {}",
                file_path
            );
            return None;
        }
    };
    Some(content_hash)
}

async fn handle_move_file_error(
    dropbox_client: &UserAuthDefaultClient,
    from_path: &str,
    into_path: &str,
) {
    let (from_content_hash_result, into_content_hash_result) = tokio::join!(
        get_file_content_hash(dropbox_client, from_path),
        get_file_content_hash(dropbox_client, into_path)
    );
    let from_content_hash = match from_content_hash_result {
        Some(hash) => hash,
        None => {
            error!(
                "Could not get content hash for source file {}, cannot handle move error",
                from_path
            );
            return;
        }
    };
    let into_content_hash = match into_content_hash_result {
        Some(hash) => hash,
        None => {
            error!(
                "Could not get content hash for destination file {}, cannot handle move error",
                into_path
            );
            return;
        }
    };
    if from_content_hash == into_content_hash {
        info!(
            "Source and destination files have the same content hash ({}), deleting the source file {}.",
            from_content_hash, from_path
        );
        match files::delete_v2(
            dropbox_client,
            &files::DeleteArg::new(from_path.to_string()),
        )
        .await
        {
            Ok(result) => {
                info!("Source file deleted successfully: {:?}", result);
            }
            Err(e) => {
                error!("Error deleting source file: {e}");
            }
        }
    } else {
        error!(
            "Source and destination files have different content hashes ({} vs {}). Cannot ignore the move error.",
            from_content_hash, into_content_hash
        );
    }
}

async fn move_file(dropbox_client: &UserAuthDefaultClient, from_path: &str, into_path: &str) {
    info!("Moving file from {} to {}", from_path, into_path);
    match files::move_v2(
        dropbox_client,
        &files::RelocationArg::new(from_path.to_string(), into_path.to_string()),
    )
    .await
    {
        Ok(result) => {
            info!("File moved successfully: {:?}", result);
        }
        Err(e) => {
            warn!("Error moving file: {e}");
            handle_move_file_error(dropbox_client, from_path, into_path).await;
        }
    }
}

async fn move_files(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    into_directory: &str,
) -> Option<String> {
    info!("Listing Dropbox directory {}", from_directory);
    let mut list_folder_result = match files::list_folder(
        dropbox_client,
        &files::ListFolderArg::new(from_directory.to_string()).with_recursive(false),
    )
    .await
    {
        Ok(result) => result,
        Err(e) => {
            error!("Error from list_folder: {e}");
            return None;
        }
    };
    let mut cursor = list_folder_result.cursor;
    loop {
        info!("Directory entries: {}", list_folder_result.entries.len());
        for entry in list_folder_result.entries {
            match entry {
                files::Metadata::Folder(entry) => {
                    info!(
                        "Ignoring folder: {}",
                        entry.path_display.unwrap_or(entry.name)
                    );
                }
                files::Metadata::File(entry) => {
                    if is_file_to_be_moved(&entry.name) {
                        move_file(
                            dropbox_client,
                            &join_dropbox_paths(from_directory, &entry.name),
                            &join_dropbox_paths(into_directory, &entry.name),
                        )
                        .await;
                    } else {
                        info!("Ignoring file (not matching criteria): {}", entry.name);
                    }
                }
                files::Metadata::Deleted(entry) => {
                    info!("Ignoring deleted entry: {:?}", entry);
                }
            }
        }
        if !list_folder_result.has_more {
            break;
        }
        list_folder_result = match files::list_folder_continue(
            dropbox_client,
            &files::ListFolderContinueArg::new(cursor.clone()),
        )
        .await
        {
            Ok(result) => result,
            Err(e) => {
                error!("Error from list_folder_continue: {e}");
                return None;
            }
        };
        if cursor != list_folder_result.cursor {
            warn!(
                "Cursor changed from {} to {}. Normally it doesn't change.",
                cursor, list_folder_result.cursor
            );
        }
        cursor = list_folder_result.cursor;
    }
    Some(cursor)
}

async fn wait_for_changes(cursor: &str) {
    debug!("Waiting for Dropbox changes...");
    let client = NoauthDefaultClient::default();
    let mut next_delay = None;
    loop {
        if let Some(delay) = next_delay.take() {
            info!("Waiting for {:?} before polling Dropbox again", &delay);
            tokio::time::sleep(delay).await;
        }
        match files::list_folder_longpoll(
            &client,
            &files::ListFolderLongpollArg::new(cursor.to_string()),
        )
        .await
        {
            Ok(result) => {
                if result.changes {
                    info!("Changes detected");
                    break;
                } else {
                    debug!("No changes detected");
                }
                if let Some(backoff) = &result.backoff {
                    let delay = tokio::time::Duration::from_secs(*backoff);
                    next_delay = Some(delay);
                }
            }
            Err(e) => {
                error!("Error from list_folder_longpoll: {e}");
                next_delay = Some(tokio::time::Duration::from_mins(1));
            }
        };
    }
}

async fn keep_moving(
    dropbox_client: &UserAuthDefaultClient,
    from_directory: &str,
    into_directory: &str,
) {
    loop {
        let cursor = match move_files(dropbox_client, from_directory, into_directory).await {
            Some(success) => success,
            None => {
                let delay = tokio::time::Duration::from_mins(1);
                warn!("Could not move files, will try again in {:?}", delay);
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        wait_for_changes(&cursor).await;
    }
}

async fn run_dropbox_file_mover(
    dropbox_api_app_key: &str,
    dropbox_oauth: Option<&str>,
    from_directory: &str,
    into_directory: &str,
) {
    let auth = match authenticate(dropbox_api_app_key, dropbox_oauth).await {
        Some(auth) => auth,
        None => {
            error!("Failed to authenticate");
            return;
        }
    };
    let client = UserAuthDefaultClient::new(auth);
    keep_moving(&client, from_directory, into_directory).await;
}
