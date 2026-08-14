//! Cross-process admission lock for receive-side dataset mutations.
//!
//! Every SSH recv channel is a separate `arctern` process, so an in-memory
//! mutex cannot prevent two `zfs recv` processes from targeting the same
//! dataset. Advisory `flock(2)` locks survive for the lifetime of the open
//! file descriptor and are released by the kernel if a dispatcher dies.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum RecvLockError {
    #[error("receive already active for {dataset}")]
    Busy { dataset: String },
    #[error("open receive lock for {dataset}: {source}")]
    Io {
        dataset: String,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug, Clone)]
pub struct RecvLocks {
    dir: PathBuf,
}

impl RecvLocks {
    pub fn new(state_dir: &Path) -> Self {
        Self {
            dir: state_dir.join("recv-locks"),
        }
    }

    pub fn acquire(&self, dataset: &str) -> Result<RecvLock, RecvLockError> {
        fs::create_dir_all(&self.dir).map_err(|source| RecvLockError::Io {
            dataset: dataset.to_string(),
            source,
        })?;
        let digest = Sha256::digest(dataset.as_bytes());
        let path = self.dir.join(format!("{digest:x}.lock"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
            .map_err(|source| RecvLockError::Io {
                dataset: dataset.to_string(),
                source,
            })?;

        // SAFETY: `file` owns a valid descriptor for this call and remains
        // alive in `RecvLock` for the entire critical section.
        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result == 0 {
            Ok(RecvLock { _file: file })
        } else {
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::WouldBlock {
                Err(RecvLockError::Busy {
                    dataset: dataset.to_string(),
                })
            } else {
                Err(RecvLockError::Io {
                    dataset: dataset.to_string(),
                    source,
                })
            }
        }
    }
}

#[derive(Debug)]
pub struct RecvLock {
    _file: File,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "arctern-recv-lock-{name}-{}-{}",
            std::process::id(),
            getrandom::u64().expect("random suffix")
        ))
    }

    #[test]
    fn excludes_same_dataset_and_releases_on_drop() {
        let dir = test_dir("exclusive");
        let locks = RecvLocks::new(&dir);
        let first = locks.acquire("tank/backup/home").unwrap();
        assert!(matches!(
            locks.acquire("tank/backup/home"),
            Err(RecvLockError::Busy { .. })
        ));
        drop(first);
        locks.acquire("tank/backup/home").unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn permits_different_datasets() {
        let dir = test_dir("parallel");
        let locks = RecvLocks::new(&dir);
        let _first = locks.acquire("tank/backup/home").unwrap();
        let _second = locks.acquire("tank/backup/root").unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}
