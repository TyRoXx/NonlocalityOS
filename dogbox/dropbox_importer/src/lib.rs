use astraea::storage::LoadStoreTree;
use dogbox_tree_editor::{OpenDirectory, WallClock};
use dropbox_sdk::{async_routes::files, default_async_client::UserAuthDefaultClient};
use std::{path::PathBuf, sync::Arc};
use tracing::{error, info, warn};

pub struct DropboxImporter {}

impl DropboxImporter {
    pub async fn import_directory(
        &self,
        dropbox_client: &UserAuthDefaultClient,
        from_directory: &str,
        storage: Arc<dyn LoadStoreTree + Send + Sync>,
        clock: WallClock,
    ) -> std::io::Result<OpenDirectory> {
        let open_directory = OpenDirectory::create_directory(
            PathBuf::new(),
            storage,
            clock,
            /*don't know which number would be good*/ 64,
        )
        .await
        .expect("Failed to create root directory in storage");
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
                todo!()
            }
        };
        let mut cursor = list_folder_result.cursor;
        loop {
            info!("Directory entries: {}", list_folder_result.entries.len());
            for entry in list_folder_result.entries {
                match entry {
                    files::Metadata::Folder(entry) => {
                        todo!()
                    }
                    files::Metadata::File(entry) => {
                        todo!()
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
                    todo!()
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
        Ok(open_directory)
    }
}
