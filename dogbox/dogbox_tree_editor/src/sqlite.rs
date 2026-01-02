use crate::{NormalizedPath, OpenFile, TreeEditor};
use relative_path::RelativePath;
use sqlite_vfs::{LockKind, OpenOptions, RegisterError, Vfs};
use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tracing::error;

struct LockState {
    read: usize,
    write: Option<bool>,
}

pub struct PagesVfs<const PAGE_SIZE: usize> {
    lock_state: Arc<Mutex<LockState>>,
    runtime: Handle,
    editor: TreeEditor,
}

impl<const PAGE_SIZE: usize> PagesVfs<PAGE_SIZE> {
    pub fn new(editor: TreeEditor, runtime: Handle) -> Self {
        PagesVfs {
            lock_state: Arc::new(Mutex::new(LockState {
                read: 0,
                write: None,
            })),
            runtime,
            editor,
        }
    }
}

pub struct DatabaseFile<const PAGE_SIZE: usize> {
    lock_state: Arc<Mutex<LockState>>,
    lock: LockKind,
    open_file: Arc<OpenFile>,
}

impl<const PAGE_SIZE: usize> Vfs for PagesVfs<PAGE_SIZE> {
    type Handle = DatabaseFile<PAGE_SIZE>;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        let path = NormalizedPath::try_from(RelativePath::new(db)).map_err(|err| {
            let message = format!("Invalid database file path `{db}`: {err}");
            error!("{}", message);
            io::Error::new(ErrorKind::InvalidInput, message)
        })?;
        self.runtime.block_on(async move {
            let open_file = self.editor.open_file(path).await.map_err(|err| {
                let message = format!("Failed to open database file `{db}`: {err}");
                error!("{}", message);
                io::Error::new(ErrorKind::Other, message)
            })?;
            Ok(DatabaseFile {
                lock_state: self.lock_state.clone(),
                lock: LockKind::None,
                open_file,
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
                io::Error::new(ErrorKind::Other, message)
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
                    crate::Error::NotFound(_name) => Ok(false),
                    _ => {
                        let message =
                            format!("Failed to check existence of database file `{db}`: {err}");
                        error!("{}", message);
                        Err(io::Error::new(ErrorKind::Other, message))
                    }
                })
        })
    }

    fn temporary_name(&self) -> String {
        todo!()
    }

    fn random(&self, buffer: &mut [i8]) {
        todo!()
    }

    fn sleep(&self, duration: Duration) -> Duration {
        let now = Instant::now();
        std::thread::sleep(duration);
        now.elapsed()
    }
}

impl<const PAGE_SIZE: usize> sqlite_vfs::DatabaseHandle for DatabaseFile<PAGE_SIZE> {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> Result<u64, io::Error> {
        let size = Self::page_count() * PAGE_SIZE;
        eprintln!("size={size}");
        Ok(size as u64)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        let index = offset as usize / PAGE_SIZE;
        let offset = offset as usize % PAGE_SIZE;

        let data = Self::get_page(index as u32);
        if data.len() < buf.len() + offset {
            eprintln!(
                "read {} < {} -> UnexpectedEof",
                data.len(),
                buf.len() + offset
            );
            return Err(ErrorKind::UnexpectedEof.into());
        }

        eprintln!("read index={} len={} offset={}", index, buf.len(), offset);
        buf.copy_from_slice(&data[offset..offset + buf.len()]);

        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if offset as usize % PAGE_SIZE > 0 {
            return Err(io::Error::new(
                ErrorKind::Other,
                "unexpected write across page boundaries",
            ));
        }

        let index = offset as usize / PAGE_SIZE;
        let page = buf.try_into().map_err(|_| {
            io::Error::new(
                ErrorKind::Other,
                format!(
                    "unexpected write size {}; expected {}",
                    buf.len(),
                    PAGE_SIZE
                ),
            )
        })?;
        eprintln!("write index={} len={}", index, buf.len());
        Self::put_page(index as u32, page);

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        // Everything is directly written to storage, so no extra steps necessary to sync.
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        eprintln!("set_len={size}");

        let mut page_count = size as usize / PAGE_SIZE;
        if size as usize % PAGE_SIZE > 0 {
            page_count += 1;
        }

        let current_page_count = Self::page_count();
        if page_count > 0 && page_count < current_page_count {
            for i in (page_count..current_page_count).into_iter().rev() {
                Self::del_page(i as u32);
            }
        }

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let ok = Self::lock(self, lock);
        // eprintln!("locked = {}", ok);
        Ok(ok)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        Ok(Self::reserved(self))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        Ok(sqlite_vfs::WalDisabled::default())
    }

    fn set_chunk_size(&self, chunk_size: usize) -> Result<(), io::Error> {
        if chunk_size != PAGE_SIZE {
            eprintln!("set_chunk_size={chunk_size} (rejected)");
            Err(io::Error::new(
                ErrorKind::Other,
                "changing chunk size is not allowed",
            ))
        } else {
            eprintln!("set_chunk_size={chunk_size}");
            Ok(())
        }
    }
}

impl<const PAGE_SIZE: usize> DatabaseFile<PAGE_SIZE> {
    fn get_page(ix: u32) -> [u8; PAGE_SIZE] {
        todo!()
    }

    fn put_page(ix: u32, data: &[u8; PAGE_SIZE]) {
        todo!()
    }

    fn del_page(ix: u32) {
        todo!()
    }

    fn page_count() -> usize {
        todo!()
    }

    fn lock(&mut self, to: LockKind) -> bool {
        if self.lock == to {
            return true;
        }

        let mut lock_state = self.lock_state.lock().unwrap();

        // eprintln!(
        //     "lock state={:?} from={:?} to={:?}",
        //     lock_state, self.lock, to
        // );

        // The following locking implementation is probably not sound (wouldn't be surprised if it
        // potentially dead-locks), but suffice for the experiment.

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

pub fn register_vfs(name: &str, editor: TreeEditor, runtime: Handle) -> Result<(), RegisterError> {
    sqlite_vfs::register(name, PagesVfs::<4096>::new(editor, runtime), true)
}
