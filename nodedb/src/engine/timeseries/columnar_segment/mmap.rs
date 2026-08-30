// SPDX-License-Identifier: BUSL-1.1

//! mmap wrapper for columnar column files with access-pattern advice.
//!
//! Columnar scans are forward sequential reads of compressed column files.
//! Without MADV_SEQUENTIAL the kernel underreads and retains consumed pages;
//! without POSIX_FADV_DONTNEED after a scan, cold partitions pin page cache
//! away from hotter engines.
//!
//! For plaintext column files the backing storage is a `memmap2::Mmap`
//! (zero-copy). For encrypted files the backing storage is an owned
//! `Vec<u8>` of the decrypted plaintext — mmap zero-copy is not available
//! for encrypted on-disk blobs, which is acceptable given the one-time open
//! cost at partition open time.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;

/// Module-scoped counters for observing mmap advice + fadvise behaviour.
pub mod observability {
    use super::{AtomicU64, Ordering};
    pub(super) static MADV_SEQUENTIAL_COUNT: AtomicU64 = AtomicU64::new(0);
    pub(super) static FADV_DONTNEED_COUNT: AtomicU64 = AtomicU64::new(0);

    pub fn madv_sequential_count() -> u64 {
        MADV_SEQUENTIAL_COUNT.load(Ordering::Relaxed)
    }
    pub fn fadv_dontneed_count() -> u64 {
        FADV_DONTNEED_COUNT.load(Ordering::Relaxed)
    }
}

/// Backing storage for an open column file.
pub(super) enum BackingStore {
    /// Memory-mapped plaintext file (zero-copy, POSIX_FADV_DONTNEED on drop).
    Mmap {
        mmap: memmap2::Mmap,
        file: std::fs::File,
    },
    /// Heap-allocated decrypted plaintext (encrypted on-disk file).
    Decrypted(Vec<u8>),
}

impl BackingStore {
    pub(super) fn bytes(&self) -> &[u8] {
        match self {
            BackingStore::Mmap { mmap, .. } => mmap,
            BackingStore::Decrypted(v) => v,
        }
    }
}

/// Wrapper around a column-file backing store that advises `MADV_SEQUENTIAL`
/// on construction (plaintext path) and `POSIX_FADV_DONTNEED` on drop
/// (plaintext path). Returned by `ColumnarSegmentReader::mmap_column`.
///
/// For encrypted files the backing store is an owned decrypted buffer; the
/// MADV/fadvise calls are skipped since there is no mmap region to advise.
pub struct ColumnMmap {
    pub(super) backing: BackingStore,
    pub(super) path: PathBuf,
}

impl ColumnMmap {
    pub fn bytes(&self) -> &[u8] {
        self.backing.bytes()
    }

    pub fn len(&self) -> usize {
        self.backing.bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.backing.bytes().is_empty()
    }

    /// Returns `true` if the backing store is a mmap'd plaintext file.
    pub fn is_mmap(&self) -> bool {
        matches!(self.backing, BackingStore::Mmap { .. })
    }

    /// Returns `true` if the backing store is a decrypted owned buffer.
    pub fn is_decrypted_owned(&self) -> bool {
        matches!(self.backing, BackingStore::Decrypted(_))
    }
}

impl std::ops::Deref for ColumnMmap {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.backing.bytes()
    }
}

impl Drop for ColumnMmap {
    fn drop(&mut self) {
        let BackingStore::Mmap { ref mmap, ref file } = self.backing else {
            return;
        };
        let len = mmap.len();
        if len == 0 {
            return;
        }
        #[cfg(target_os = "linux")]
        {
            let rc = unsafe {
                libc::posix_fadvise(
                    file.as_raw_fd(),
                    0,
                    len as libc::off_t,
                    libc::POSIX_FADV_DONTNEED,
                )
            };
            if rc == 0 {
                observability::FADV_DONTNEED_COUNT.fetch_add(1, Ordering::Relaxed);
            } else {
                tracing::warn!(
                    path = %self.path.display(),
                    errno = rc,
                    "posix_fadvise(DONTNEED) failed on columnar mmap drop",
                );
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (file, &self.path);
        }
    }
}

/// Advise `MADV_SEQUENTIAL` on a freshly-mapped column region.
pub(super) fn advise_sequential(mmap: &memmap2::Mmap, col_path: &std::path::Path) {
    if mmap.is_empty() {
        return;
    }
    let rc = unsafe {
        libc::madvise(
            mmap.as_ptr() as *mut libc::c_void,
            mmap.len(),
            libc::MADV_SEQUENTIAL,
        )
    };
    if rc == 0 {
        observability::MADV_SEQUENTIAL_COUNT.fetch_add(1, Ordering::Relaxed);
    } else {
        tracing::warn!(
            path = %col_path.display(),
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "madvise(MADV_SEQUENTIAL) failed on column mmap",
        );
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::timeseries::MetricSample;
    use nodedb_wal::crypto::WalEncryptionKey;
    use tempfile::TempDir;

    use super::super::super::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
    use super::super::reader::ColumnarSegmentReader;
    use super::super::writer::ColumnarSegmentWriter;

    fn test_config() -> ColumnarMemtableConfig {
        ColumnarMemtableConfig {
            max_memory_bytes: 10 * 1024 * 1024,
            hard_memory_limit: 20 * 1024 * 1024,
            max_tag_cardinality: 1000,
        }
    }

    fn test_kek() -> WalEncryptionKey {
        WalEncryptionKey::from_bytes(&[0x42u8; 32]).unwrap()
    }

    fn build_simple_drain() -> (
        TempDir,
        crate::engine::timeseries::columnar_memtable::ColumnarDrainResult,
    ) {
        let tmp = TempDir::new().unwrap();
        let mut mt = ColumnarMemtable::new_metric(test_config());
        for i in 0..100 {
            mt.ingest_metric(
                1,
                MetricSample {
                    timestamp_ms: 1_000_000 + i * 1000,
                    value: i as f64 * 2.0,
                },
            );
        }
        (tmp, mt.drain())
    }

    #[test]
    fn columnar_segment_mmap_plaintext_owned_buffer_encrypted() {
        let kek = test_kek();
        let (tmp, drain) = build_simple_drain();
        let writer = ColumnarSegmentWriter::new(tmp.path());
        writer
            .write_partition("mmap-part", &drain.view(), 86_400_000, 1, Some(&kek))
            .unwrap();

        let part_dir = tmp.path().join("mmap-part");

        // Encrypted path returns an owned decrypted buffer.
        let col_mmap =
            ColumnarSegmentReader::mmap_column(&part_dir, "timestamp", Some(&kek)).unwrap();
        assert!(
            col_mmap.is_decrypted_owned(),
            "encrypted column must use owned buffer, not mmap"
        );
        assert!(!col_mmap.is_empty(), "decrypted buffer must not be empty");

        // Plaintext path uses an actual mmap.
        let (tmp2, drain2) = build_simple_drain();
        let writer2 = ColumnarSegmentWriter::new(tmp2.path());
        writer2
            .write_partition("plain-mmap", &drain2.view(), 86_400_000, 1, None)
            .unwrap();
        let part_dir2 = tmp2.path().join("plain-mmap");
        let plain_mmap = ColumnarSegmentReader::mmap_column(&part_dir2, "timestamp", None).unwrap();
        assert!(
            plain_mmap.is_mmap(),
            "plaintext column must use mmap, not owned buffer"
        );
    }
}
