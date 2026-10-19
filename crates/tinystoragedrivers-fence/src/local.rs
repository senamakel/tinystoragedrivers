//! Portable process-exclusive leases for hosts without a shared backend.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
};

use fs2::FileExt;
use sha2::{Digest, Sha256};
use tinystoragedrivers_core::{LeaseRecord, StorageError, Version};

use crate::{LeaseError, LeaseGrant, LeaseStore};

const LOCK_FILE: &str = ".lease";
const RECORD_FILE: &str = ".lease.json";
const RECORD_TMP_SUFFIX: &str = ".lease.json.tsd-tmp";
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug)]
struct Holding {
    file: File,
    record: LeaseRecord,
    version: Version,
}

/// Local lease store backed by portable advisory file locks.
///
/// It shares the `.lease` lock and `.lease.json` record format used by the
/// existing host implementation. A lock is held until release or process
/// exit; this store does not issue distributed storage fences.
#[derive(Debug)]
pub struct LocalLeases {
    root: PathBuf,
    holder: String,
    endpoint: Option<String>,
    held: Mutex<HashMap<String, Holding>>,
}

impl LocalLeases {
    /// Open a local lease store rooted at `root` for `holder`.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an empty holder or a backend error if the
    /// root directory cannot be created.
    pub fn new(root: impl AsRef<Path>, holder: impl Into<String>) -> Result<Self, StorageError> {
        let holder = holder.into();
        if holder.is_empty() {
            return Err(StorageError::invalid_input(
                "lease holder must not be empty",
            ));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_io)?;
        Ok(Self {
            root,
            holder,
            endpoint: None,
            held: Mutex::new(HashMap::new()),
        })
    }

    /// Set the endpoint recorded for this holder and returned to contenders.
    #[must_use]
    pub fn with_endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    fn held(&self) -> MutexGuard<'_, HashMap<String, Holding>> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn directory(&self, key: &str) -> Result<PathBuf, StorageError> {
        if key.is_empty() {
            return Err(StorageError::invalid_input("lease key must not be empty"));
        }
        let digest = Sha256::digest(key.as_bytes());
        let mut hex = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(hex, "{byte:02x}");
        }
        Ok(self.root.join(hex))
    }

    fn open_lock(&self, key: &str) -> Result<File, StorageError> {
        let dir = self.directory(key)?;
        fs::create_dir_all(&dir).map_err(storage_io)?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(LOCK_FILE))
            .map_err(storage_io)
    }

    fn read_record(&self, key: &str) -> Result<Option<LeaseRecord>, StorageError> {
        let path = self.directory(key)?.join(RECORD_FILE);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(storage_io(error)),
        };
        let mut contents = String::new();
        file.read_to_string(&mut contents).map_err(storage_io)?;
        serde_json::from_str(&contents).map(Some).map_err(|error| {
            StorageError::serialization(format!("lease record {}: {error}", path.display()))
        })
    }

    fn write_record(&self, key: &str, record: &LeaseRecord) -> Result<(), StorageError> {
        let dir = self.directory(key)?;
        let destination = dir.join(RECORD_FILE);
        // Keep the established host field names and non-expiring sentinel so
        // old binaries can read records written during a rolling migration.
        let wire = LegacyLocalRecord {
            owner: &record.holder,
            endpoint: &record.endpoint,
            epoch: record.epoch,
            expires_at_ms: u64::MAX,
            released: record.released,
        };
        let bytes = serde_json::to_vec(&wire).map_err(StorageError::from)?;
        for _ in 0..16 {
            let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temporary = dir.join(format!(
                "{}-{}-{sequence}.tmp",
                RECORD_TMP_SUFFIX,
                std::process::id()
            ));
            let mut file = match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(storage_io(error)),
            };
            let result = (|| {
                file.write_all(&bytes).map_err(storage_io)?;
                file.flush().map_err(storage_io)?;
                file.sync_all().map_err(storage_io)?;
                drop(file);
                fs::rename(&temporary, &destination).map_err(storage_io)
            })();
            if result.is_err() {
                let _ = fs::remove_file(temporary);
            }
            return result;
        }
        Err(StorageError::backend(
            "could not allocate a unique local lease temporary file",
        ))
    }

    fn is_contended(error: &std::io::Error) -> bool {
        error.kind() == std::io::ErrorKind::WouldBlock
            || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
    }
}

#[derive(serde::Serialize)]
struct LegacyLocalRecord<'a> {
    owner: &'a str,
    endpoint: &'a Option<String>,
    epoch: u64,
    expires_at_ms: u64,
    released: bool,
}

#[async_trait::async_trait]
impl LeaseStore for LocalLeases {
    async fn acquire(&self, key: &str, _now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        // This guard spans lookup, lock acquisition, record migration and
        // insertion. A same-instance acquire cannot race a release/takeover.
        let mut held = self.held();
        if let Some(holding) = held.get(key) {
            return Ok(LeaseGrant {
                key: key.to_owned(),
                holder: holding.record.holder.clone(),
                epoch: holding.record.epoch,
                version: holding.version,
                expires_ms: holding.record.expires_ms,
                previous_unclean: false,
            });
        }

        let file = self.open_lock(key)?;
        if let Err(error) = file.try_lock_exclusive() {
            if !Self::is_contended(&error) {
                return Err(LeaseError::Storage(storage_io(error)));
            }
            let record = self.read_record(key)?.unwrap_or(LeaseRecord {
                holder: "unknown".to_owned(),
                endpoint: None,
                epoch: 0,
                expires_ms: i64::MAX,
                released: false,
            });
            return Err(LeaseError::Held(record));
        }

        let previous = match self.read_record(key) {
            Ok(record) => record,
            Err(error) => {
                let _ = FileExt::unlock(&file);
                return Err(LeaseError::Storage(error));
            }
        };
        let epoch = match previous.as_ref() {
            None => 1,
            Some(record) => {
                if let Some(epoch) = record.epoch.checked_add(1) {
                    epoch
                } else {
                    let _ = FileExt::unlock(&file);
                    return Err(LeaseError::Storage(StorageError::backend(
                        "lease epoch space exhausted",
                    )));
                }
            }
        };
        let previous_unclean = previous.as_ref().is_some_and(|record| !record.released);
        let record = LeaseRecord {
            holder: self.holder.clone(),
            endpoint: self.endpoint.clone(),
            epoch,
            expires_ms: i64::MAX,
            released: false,
        };
        if let Err(error) = self.write_record(key, &record) {
            let _ = FileExt::unlock(&file);
            return Err(LeaseError::Storage(error));
        }
        held.insert(
            key.to_owned(),
            Holding {
                file,
                record: record.clone(),
                version: Version::FIRST,
            },
        );
        Ok(LeaseGrant {
            key: key.to_owned(),
            holder: self.holder.clone(),
            epoch,
            version: Version::FIRST,
            expires_ms: i64::MAX,
            previous_unclean,
        })
    }

    async fn renew(&self, grant: &LeaseGrant, _now_ms: u64) -> Result<LeaseGrant, LeaseError> {
        // Validation, generation advance and holding update are one critical
        // section. Exactly one concurrent renewal can consume a grant version.
        let mut held = self.held();
        let holding = held.get_mut(&grant.key).ok_or(LeaseError::Lost)?;
        if grant.holder != self.holder
            || holding.record.holder != grant.holder
            || holding.record.epoch != grant.epoch
            || holding.version != grant.version
        {
            return Err(LeaseError::Lost);
        }
        let next = holding.version.next().ok_or_else(|| {
            LeaseError::Storage(StorageError::backend("lease version space exhausted"))
        })?;
        holding.version = next;
        Ok(LeaseGrant {
            version: next,
            ..grant.clone()
        })
    }

    async fn release(&self, grant: LeaseGrant) -> Result<(), LeaseError> {
        let mut held = self.held();
        let holding = held.get(&grant.key).ok_or(LeaseError::Lost)?;
        if grant.holder != self.holder
            || holding.record.holder != grant.holder
            || holding.record.epoch != grant.epoch
            || holding.version != grant.version
        {
            return Err(LeaseError::Lost);
        }
        let record = LeaseRecord {
            released: true,
            ..holding.record.clone()
        };
        self.write_record(&grant.key, &record)?;
        if let Some(holding) = held.remove(&grant.key) {
            let _ = FileExt::unlock(&holding.file);
        }
        Ok(())
    }

    async fn inspect(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        if let Some(holding) = self.held().get(key) {
            return Ok(Some(holding.record.clone()));
        }
        Ok(self.read_record(key)?)
    }

    async fn holder(&self, key: &str) -> Result<Option<LeaseRecord>, LeaseError> {
        if let Some(holding) = self.held().get(key) {
            return Ok(Some(holding.record.clone()));
        }
        let Some(mut record) = self.read_record(key)? else {
            return Ok(None);
        };
        if !record.released {
            let lock = self.open_lock(key)?;
            match lock.try_lock_exclusive() {
                Ok(()) => {
                    let _ = FileExt::unlock(&lock);
                    // This is an observation only. Keep the returned record's
                    // identity and durable epoch while exposing that the OS
                    // ownership lock is currently free.
                    record.expires_ms = 0;
                }
                Err(error) if Self::is_contended(&error) => {}
                Err(error) => return Err(storage_io(error).into()),
            }
        }
        Ok(Some(record))
    }
}

fn storage_io(error: std::io::Error) -> StorageError {
    StorageError::backend("local lease file operation failed").with_source(error)
}
