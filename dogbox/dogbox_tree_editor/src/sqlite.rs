use crate::{
    FileCreationMode, NormalizedPath, OpenFile, OpenFileReadPermission, OpenFileStatus,
    OpenFileWritePermission, TreeEditor,
};
use rand::RngCore;
use relative_path::RelativePath;
use sqlite_vfs::{LockKind, OpenOptions, RegisterError, Vfs};
use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tracing::{error, info, warn};

#[derive(Default)]
struct LockState {
    read: usize,
    write: Option<bool>,
}

pub struct PagesVfs<const PAGE_SIZE: usize> {
    lock_state: Arc<Mutex<LockState>>,
    runtime: Handle,
    editor: TreeEditor,
    random_number_generator: Mutex<Box<dyn RngCore + Send>>,
    sync_directory: SyncDirectoryFunction,
}

impl<const PAGE_SIZE: usize> PagesVfs<PAGE_SIZE> {
    pub fn new(
        editor: TreeEditor,
        runtime: Handle,
        random_number_generator: Box<dyn RngCore + Send>,
        sync_directory: SyncDirectoryFunction,
    ) -> Self {
        info!("Creating PagesVfs with page size {}", PAGE_SIZE);
        PagesVfs {
            lock_state: Arc::new(Mutex::new(LockState {
                read: 0,
                write: None,
            })),
            runtime,
            editor,
            random_number_generator: Mutex::new(random_number_generator),
            sync_directory,
        }
    }
}

pub type SyncDirectoryFunction = Arc<dyn Fn() -> Result<(), io::Error> + Send + Sync>;

pub struct DatabaseFile<const PAGE_SIZE: usize> {
    name: String,
    lock_state: Arc<Mutex<LockState>>,
    lock: LockKind,
    open_file: Arc<OpenFile>,
    runtime: Handle,
    read_permission: Arc<OpenFileReadPermission>,
    write_permission: Option<Arc<OpenFileWritePermission>>,
    sync_directory: SyncDirectoryFunction,
}

impl<const PAGE_SIZE: usize> Vfs for PagesVfs<PAGE_SIZE> {
    type Handle = DatabaseFile<PAGE_SIZE>;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        let path = NormalizedPath::try_from(RelativePath::new(db)).map_err(|err| {
            let message = format!("Invalid database file path `{db}`: {err}");
            error!("{}", message);
            io::Error::new(ErrorKind::InvalidInput, message)
        })?;
        match &opts.kind {
            sqlite_vfs::OpenKind::MainDb => {}
            sqlite_vfs::OpenKind::MainJournal => {}
            sqlite_vfs::OpenKind::TempDb => {
                info!("Opening temporary database file: {}", db);
            }
            sqlite_vfs::OpenKind::TempJournal => todo!(),
            sqlite_vfs::OpenKind::TransientDb => todo!(),
            sqlite_vfs::OpenKind::SubJournal => todo!(),
            sqlite_vfs::OpenKind::SuperJournal => todo!(),
            sqlite_vfs::OpenKind::Wal => {
                info!("Opening WAL file: {}", db);
            }
        }
        self.runtime.block_on(async move {
            let creation_mode = match opts.access {
                sqlite_vfs::OpenAccess::Read => FileCreationMode::open_existing(),
                sqlite_vfs::OpenAccess::Write => FileCreationMode::open_existing(),
                sqlite_vfs::OpenAccess::Create => FileCreationMode::create(),
                sqlite_vfs::OpenAccess::CreateNew => FileCreationMode::create_new(),
            };
            let open_file = self
                .editor
                .open_file(path, creation_mode)
                .await
                .map_err(|err| {
                    let message = format!("Failed to open database file `{db}`: {err}");
                    error!("{}", message);
                    io::Error::other(message)
                })?;
            let read_permission = open_file.get_read_permission();
            let write_permission = match opts.access {
                sqlite_vfs::OpenAccess::Read => None,
                sqlite_vfs::OpenAccess::Write => Some(open_file.get_write_permission()),
                sqlite_vfs::OpenAccess::Create => Some(open_file.get_write_permission()),
                sqlite_vfs::OpenAccess::CreateNew => Some(open_file.get_write_permission()),
            };
            Ok(DatabaseFile {
                name: db.to_string(),
                lock_state: self.lock_state.clone(),
                lock: LockKind::None,
                open_file,
                runtime: self.runtime.clone(),
                read_permission,
                write_permission,
                sync_directory: self.sync_directory.clone(),
            })
        })
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        let path = NormalizedPath::try_from(RelativePath::new(db)).map_err(|err| {
            let message = format!("Invalid database file path `{db}`: {err}");
            error!("{}", message);
            io::Error::new(ErrorKind::InvalidInput, message)
        })?;
        self.runtime.block_on(async move {
            self.editor.remove(path).await.map_err(|err| {
                let message = format!("Failed to delete database file `{db}`: {err}");
                error!("{}", message);
                io::Error::other(message)
            })
        })
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        let path = NormalizedPath::try_from(RelativePath::new(db)).map_err(|err| {
            let message = format!("Invalid database file path `{db}`: {err}");
            error!("{}", message);
            io::Error::new(ErrorKind::InvalidInput, message)
        })?;
        self.runtime.block_on(async move {
            self.editor
                .get_meta_data(path)
                .await
                .map(|_| true)
                .or_else(|err| match err {
                    crate::Error::NotFound(_name) => {
                        info!("File not found: {}", db);
                        Ok(false)
                    }
                    _ => {
                        let message =
                            format!("Failed to check existence of database file `{db}`: {err}");
                        error!("{}", message);
                        Err(io::Error::other(message))
                    }
                })
        })
    }

    fn temporary_name(&self) -> String {
        let mut rng_locked = self.random_number_generator.lock().unwrap();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let random_number = rng_locked.next_u64();
            let temp_name = format!("sqlite-temp-{:016x}", random_number);
            match self.exists(&temp_name) {
                Ok(exists) => {
                    if !exists {
                        info!("Generated unique temporary name: {}", temp_name);
                        return temp_name;
                    }
                }
                Err(error) => {
                    error!("Failed to check existence of temporary name `{temp_name}`: {error}");
                    return "".to_string();
                }
            }
            let max_attempts = 10;
            if attempts >= max_attempts {
                error!(
                    "Failed to generate a unique temporary name after {} attempts.",
                    max_attempts
                );
                return "".to_string();
            }
        }
    }

    fn random(&self, buffer: &mut [i8]) {
        let mut rng_locked = self.random_number_generator.lock().unwrap();
        // cast the slice from &mut [i8] to &mut [u8]:
        let buffer_u8 =
            unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buffer.len()) };
        rng_locked.fill_bytes(buffer_u8);
    }

    fn sleep(&self, duration: Duration) -> Duration {
        let now = Instant::now();
        std::thread::sleep(duration);
        now.elapsed()
    }
}

// https://sqlite.org/c3ref/io_methods.html
impl<const PAGE_SIZE: usize> sqlite_vfs::DatabaseHandle for DatabaseFile<PAGE_SIZE> {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> Result<u64, io::Error> {
        self.runtime.block_on(async move {
            let meta_data = self.open_file.get_meta_data().await;
            Ok(meta_data.size)
        })
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        let open_file = self.open_file.clone();
        let read_permission = self.read_permission.clone();
        self.runtime.block_on(async move {
            let mut next_read_position = offset;
            let mut remaining = buf;
            while !remaining.is_empty() {
                let bytes_read = open_file
                    .read_bytes(&read_permission, next_read_position, remaining.len())
                    .await
                    .map_err(|err| {
                        let message = format!(
                            "Failed to read {} bytes at offset {}: {}",
                            remaining.len(),
                            next_read_position,
                            err
                        );
                        error!("{}", message);
                        io::Error::other(message)
                    })?;
                if bytes_read.is_empty() {
                    warn!(
                        "Reached end of file while reading at offset {}",
                        next_read_position
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Reached end of file",
                    ));
                }
                let split_remaining = remaining.split_at_mut(bytes_read.len());
                split_remaining.0.copy_from_slice(&bytes_read);
                remaining = split_remaining.1;
                next_read_position += bytes_read.len() as u64;
            }
            Ok(())
        })
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        let open_file = self.open_file.clone();
        let write_permission = match &self.write_permission {
            Some(permission) => permission.clone(),
            None => {
                let message = "Attempted to write to a read-only database file.".to_string();
                error!("{}", message);
                return Err(io::Error::new(ErrorKind::PermissionDenied, message));
            }
        };
        // unfortunately we have to copy the data here because OpenFile's write_bytes takes Bytes
        let data = bytes::Bytes::copy_from_slice(buf);
        self.runtime.block_on(async move {
            open_file
                .write_bytes(&write_permission, offset, data)
                .await
                .map_err(|err| {
                    let message = format!(
                        "Failed to write {} bytes at offset {}: {}",
                        buf.len(),
                        offset,
                        err
                    );
                    error!("{}", message);
                    io::Error::other(message)
                })
        })
    }

    fn sync(&mut self, data_only: bool) -> Result<(), io::Error> {
        let _open_file_status: OpenFileStatus = self.runtime.block_on(async {
            self.open_file.request_save().await.map_err(|err| {
                let message = format!("Failed to request_save() database file: {}", err);
                error!("{}", message);
                io::Error::other(message)
            })
        })?;
        if data_only {
            info!("Sync data only: {}", self.name);
        } else {
            info!("Sync data and directory: {}", self.name);
            match (*self.sync_directory)() {
                Ok(_) => {}
                Err(err) => {
                    let message = format!("Failed to sync directory: {}", err);
                    error!("{}", message);
                    return Err(io::Error::other(message));
                }
            }
        }
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        let write_permission = match &self.write_permission {
            Some(permission) => permission.clone(),
            None => {
                let message = "Attempted to resize a read-only database file.".to_string();
                error!("{}", message);
                return Err(io::Error::new(ErrorKind::PermissionDenied, message));
            }
        };
        self.runtime.block_on(async {
            self.open_file
                .resize(&write_permission, size)
                .await
                .map_err(|err| {
                    let message =
                        format!("Failed to resize database file to size {}: {}", size, err);
                    error!("{}", message);
                    io::Error::other(message)
                })
        })
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let locked = Self::lock(self, lock);
        info!("Lock requested: {:?}, result: {}", lock, locked);
        Ok(locked)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        let is_reserved = Self::reserved(self);
        info!("Reserved: {}", is_reserved);
        Ok(is_reserved)
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        info!("Current lock: {:?}", self.lock);
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        // WAL is not supported by sqlite-vfs yet: https://github.com/rkusa/sqlite-vfs?tab=readme-ov-file#limitations
        info!("wal_index returns disabled");
        Ok(sqlite_vfs::WalDisabled)
    }

    // https://sqlite.org/c3ref/c_fcntl_begin_atomic_write.html#sqlitefcntlchunksize
    fn set_chunk_size(&self, chunk_size: usize) -> Result<(), io::Error> {
        if chunk_size == PAGE_SIZE {
            Ok(())
        } else {
            error!("set_chunk_size={chunk_size} (rejected)");
            Err(io::Error::other("changing chunk size is not allowed"))
        }
    }
}

impl<const PAGE_SIZE: usize> DatabaseFile<PAGE_SIZE> {
    // https://sqlite.org/c3ref/io_methods.html
    fn lock(&mut self, to: LockKind) -> bool {
        if self.lock == to {
            return true;
        }

        let mut lock_state = self.lock_state.lock().unwrap();
        match to {
            LockKind::None => {
                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                } else if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::None;
                true
            }

            LockKind::Shared => {
                if lock_state.write == Some(true) && self.lock <= LockKind::Shared {
                    return false;
                }

                lock_state.read += 1;
                if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::Shared;
                true
            }

            LockKind::Reserved => {
                if lock_state.write.is_some() || self.lock != LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }
                lock_state.write = Some(false);
                self.lock = LockKind::Reserved;
                true
            }

            LockKind::Pending => {
                // cannot be requested directly
                false
            }

            LockKind::Exclusive => {
                if lock_state.write.is_some() && self.lock <= LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }

                lock_state.write = Some(true);
                if lock_state.read == 0 {
                    self.lock = LockKind::Exclusive;
                    true
                } else {
                    self.lock = LockKind::Pending;
                    false
                }
            }
        }
    }

    fn reserved(&self) -> bool {
        if self.lock > LockKind::Shared {
            return true;
        }

        let lock_state = self.lock_state.lock().unwrap();
        lock_state.write.is_some()
    }
}

impl<const PAGE_SIZE: usize> Drop for DatabaseFile<PAGE_SIZE> {
    fn drop(&mut self) {
        if self.lock != LockKind::None {
            self.lock(LockKind::None);
        }
    }
}

pub fn register_vfs(
    name: &str,
    editor: TreeEditor,
    runtime: Handle,
    random_number_generator: Box<dyn RngCore + Send>,
    sync_directory: SyncDirectoryFunction,
) -> Result<(), RegisterError> {
    sqlite_vfs::register(
        name,
        PagesVfs::<4096>::new(editor, runtime, random_number_generator, sync_directory),
        false,
    )
}
