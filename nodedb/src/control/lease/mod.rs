// SPDX-License-Identifier: BUSL-1.1

//! Descriptor lease acquisition and release via the metadata raft group.
//!
//! Wraps the `MetadataEntry::DescriptorLeaseGrant` /
//! `DescriptorLeaseRelease` raft path that already exists in
//! `nodedb-cluster`. The cluster crate owns the canonical lease
//! state in `MetadataCache.leases` (a
//! `HashMap<(DescriptorId, node_id), DescriptorLease>`), populated
//! by every node's commit applier as soon as a grant or release
//! entry commits on the metadata raft group.
//!
//! This module provides the host-side API surface — `acquire_lease`
//! and `release_leases` — that proposes those entries and blocks
//! on the local applied watermark, mirroring the
//! `metadata_proposer::propose_catalog_entry` pattern.
//!
//! The planner acquires a lease before reading a descriptor to prevent
//! stale reads across DDL. DDL drain consumes the `MetadataCache.leases`
//! view before committing a new descriptor version. On `SIGTERM`, leases
//! are released explicitly so they drain faster than expiry.

use std::time::Duration;

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

pub mod drain;
pub mod drain_propose;
pub mod propose;
pub mod refcount;
pub mod release;
pub mod renewal;
pub mod shutdown_release;
mod wall_time;

pub(super) use wall_time::wall_now_ns;

pub use drain::{DescriptorDrainTracker, DrainEntry};
pub use drain_propose::{descriptor_id_and_prior_version, drain_for_ddl};
pub use propose::{DEFAULT_LEASE_DURATION, acquire_lease, compute_expires_at, force_refresh_lease};
pub(crate) use propose::{acquire_lease_after_admission, ensure_not_draining};
pub use refcount::{LeaseRefCount, QueryLeaseScope};
pub use release::release_leases;
pub use renewal::{LeaseRenewalConfig, LeaseRenewalLoop};

/// Same propose-and-wait timeout the catalog DDL path uses.
pub(super) const PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Encode `entry`, propose through the metadata raft handle, and
/// block on the local applied watermark until the proposed log
/// index is applied (or the timeout fires).
///
/// Shared by `acquire_lease` and `release_leases`. `operation` is a
/// short label used for diagnostic error messages — it appears in
/// both the encode-failure and timeout paths.
///
/// Caller must already have checked `shared.metadata_raft.get()`
/// and decided to take the cluster path; this helper does NOT
/// implement the single-node fallback, because the two callers
/// have different fallback semantics (acquire writes the lease
/// into the cache, release removes entries) and inlining the
/// fallback here would couple them artificially.
pub(super) fn propose_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
    operation: &'static str,
) -> Result<u64, Error> {
    let Some(handle) = shared.metadata_raft.get() else {
        // Programmer error — callers must check this themselves.
        return Err(Error::Config {
            detail: format!("descriptor lease {operation}: no metadata raft handle"),
        });
    };
    let raw = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("descriptor lease {operation} encode: {e}"),
    })?;
    let log_index = handle.propose(raw)?;

    // `wait_for` parks the calling thread on a Condvar — wrap in
    // `block_in_place` so tokio reassigns a fresh worker and the
    // raft tick that bumps the watcher is not starved.
    let watcher = shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
    let outcome = tokio::task::block_in_place(|| watcher.wait_for(log_index, PROPOSE_TIMEOUT));
    if !outcome.is_reached() {
        return Err(Error::Config {
            detail: format!(
                "descriptor lease {operation} did not apply within {PROPOSE_TIMEOUT:?} \
                 (log index {log_index}, current: {}, outcome: {outcome:?})",
                watcher.current()
            ),
        });
    }
    Ok(log_index)
}
