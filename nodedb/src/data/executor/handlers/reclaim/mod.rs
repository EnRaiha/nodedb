// SPDX-License-Identifier: BUSL-1.1

//! Per-engine collection reclaim handlers.
//!
//! Each file in this module unlinks the persistent on-disk surface
//! for one engine for a single `(tenant, collection)` pair. Called
//! from `execute_unregister_collection` after in-memory state has
//! been evicted but before the JSON summary is built, so the handler
//! picks up per-file byte counts for the `bytes_reclaimed` metric.
//!
//! Engines whose persistent state is shared-redb (document,
//! document-strict, FTS, graph edges) or in-memory only (the KV hash
//! index) are documented inline in the parent handler — no separate
//! file unlinks are required. The modules here cover the engines that
//! write per-collection checkpoint or partition files under
//! `{data_dir}/...`.

use std::path::PathBuf;

use thiserror::Error;

pub mod crdt;
pub mod sparse_vector;
pub mod spatial;
pub mod timeseries;
pub mod vector;

/// A persistent L1 surface could not be fully reclaimed. Callers must not
/// release the collection lifecycle barrier after this error.
#[derive(Debug, Error)]
pub enum ReclaimError {
    #[error("{operation} failed for '{}': {source}", path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A checkpoint manifest could not be read, so the live generation — and
    /// therefore the set of files this collection still owns — is unknown.
    /// Fail-closed: releasing the barrier here would let a same-name CREATE
    /// proceed while the predecessor's files stay reachable.
    #[error("{engine} manifest at '{}' is unreadable: {detail}", path.display())]
    Manifest {
        engine: &'static str,
        path: PathBuf,
        detail: String,
    },
}

pub type Result<T> = std::result::Result<T, ReclaimError>;

/// Summary of a single engine's reclaim pass. Missing files count as zero;
/// actual I/O failures are returned to the lifecycle barrier.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimStats {
    pub files_unlinked: u32,
    pub bytes_freed: u64,
}

impl ReclaimStats {
    pub fn merge(&mut self, other: ReclaimStats) {
        self.files_unlinked = self.files_unlinked.saturating_add(other.files_unlinked);
        self.bytes_freed = self.bytes_freed.saturating_add(other.bytes_freed);
    }
}
