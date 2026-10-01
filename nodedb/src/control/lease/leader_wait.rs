// SPDX-License-Identifier: BUSL-1.1

//! Propose-and-wait: encode a metadata entry, propose it through raft,
//! and await its local apply, retrying past a transient leader election.

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
async fn propose_once_leader_is_elected(
    handle: &dyn crate::control::metadata_proposer::MetadataRaftHandle,
    raw: Vec<u8>,
    operation: &'static str,
) -> Result<u64, Error> {
    for (attempt, backoff_ms) in LEADER_ELECTION_BACKOFF_MS.iter().enumerate() {
        match handle.propose_async(raw.clone()).await {
            Ok(log_index) => return Ok(log_index),
            Err(Error::MetadataLeaderUnavailable) => {
                tracing::debug!(
                    attempt,
                    operation,
                    "descriptor lease: metadata election in progress; re-proposing"
                );
                tokio::time::sleep(Duration::from_millis(*backoff_ms)).await;
            }
            Err(other) => return Err(other),
        }
    }
    // One final attempt so the caller sees a live verdict rather than a stale
    // one from before the last backoff.
    handle.propose_async(raw).await
}

/// Encode `entry`, propose it through the metadata raft handle, and await
/// the local applied watermark until the proposed log index applies or the
/// timeout fires.
///
/// Shared by the lease grant and release paths. `operation` is a short label
/// for the encode-failure and timeout errors.
pub(in crate::control) async fn propose_and_wait(
    shared: &SharedState,
    entry: &MetadataEntry,
    operation: &'static str,
) -> Result<u64, Error> {
    let handle = shared.metadata_raft_handle()?;
    let raw = encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("descriptor lease {operation} encode: {e}"),
    })?;
    let log_index = propose_once_leader_is_elected(handle.as_ref(), raw, operation).await?;

    let watcher = shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
    let outcome = crate::control::metadata_proposer::wait::wait_applied(
        std::sync::Arc::clone(&watcher),
        log_index,
        PROPOSE_TIMEOUT,
    )
    .await?;
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
