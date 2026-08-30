// SPDX-License-Identifier: BUSL-1.1

//! Propose-and-wait: encode a metadata entry, propose it through raft,
//! and block until it applies locally, retrying past a transient
//! leader election.

use std::time::Duration;

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

/// Same propose-and-wait timeout the catalog DDL path uses.
pub(in crate::control) const PROPOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Backoff schedule for re-proposing while the metadata group elects a leader.
///
/// Reads take a descriptor lease, so a lease proposal issued in the first
/// moments after a restart races the metadata election and is answered with
/// `NotLeader { leader_hint: None }`. That is an election in progress, not a
/// failed proposal, so it is waited out here rather than surfaced as a failed
/// statement. Bounded at ~1.6s total: long enough for a single-node or healthy
/// multi-node election, short enough that a genuinely leaderless group still
/// fails well inside [`PROPOSE_TIMEOUT`].
const LEADER_ELECTION_BACKOFF_MS: [u64; 7] = [10, 25, 50, 100, 200, 400, 800];

/// Propose `raw`, waiting out an in-progress metadata election.
///
/// Every error other than [`Error::MetadataLeaderUnavailable`] is returned
/// immediately — only the transient no-leader case is retried, and only for a
/// bounded number of attempts.
fn propose_once_leader_is_elected(
    handle: &dyn crate::control::metadata_proposer::MetadataRaftHandle,
    raw: Vec<u8>,
    operation: &'static str,
) -> Result<u64, Error> {
    for (attempt, backoff_ms) in LEADER_ELECTION_BACKOFF_MS.iter().enumerate() {
        match handle.propose(raw.clone()) {
            Ok(log_index) => return Ok(log_index),
            Err(Error::MetadataLeaderUnavailable) => {
                tracing::debug!(
                    attempt,
                    operation,
                    "descriptor lease: metadata election in progress; re-proposing"
                );
                tokio::task::block_in_place(|| {
                    std::thread::sleep(Duration::from_millis(*backoff_ms));
                });
            }
            Err(other) => return Err(other),
        }
    }
    // One final attempt so the caller sees a live verdict rather than a stale
    // one from before the last backoff.
    handle.propose(raw)
}

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
pub(in crate::control) fn propose_and_wait(
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
    let log_index = propose_once_leader_is_elected(handle.as_ref(), raw, operation)?;

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
