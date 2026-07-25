// SPDX-License-Identifier: BUSL-1.1

//! Propose a `ReplicatedEntry` through Raft with transparent leader-change retry.
//!
//! Shared by the pgwire write dispatch path and the durable RESTORE re-issue
//! path: both must replicate a write to the vshard's Raft group and tolerate a
//! mid-flight leader change (the previous leader's entry being overwritten by a
//! new leader's election no-op) by re-proposing the same payload.

use std::sync::Arc;

use super::types::{AsyncRaftProposer, ReplicatedEntry};
use crate::control::state::SharedState;

/// Backoff schedule for `RetryableLeaderChange` re-proposals (5 attempts).
const BACKOFF_MS: [u64; 5] = [10, 25, 50, 100, 200];

/// Propose `entry` via `proposer` and return the Data Plane apply payload bytes
/// together with the write's per-collection version (as an
/// [`crate::types::Lsn`]): the written collection's `coll_write_lsn` after the
/// write, stamped by the applying replica from the WAL LSN it minted for the
/// entry's redo record. `Lsn::ZERO` when the write's plan names no single user
/// collection. See [`AsyncRaftProposer`] for why this is a WAL LSN and never the
/// Raft log index.
///
/// Retries transparently on [`crate::Error::RetryableLeaderChange`]: the
/// previous leader's entry was overwritten by a new leader's election no-op, so
/// the same write payload is re-proposed against the new leader. The encoded
/// `ReplicatedEntry` carries enough identity (collection, PK, surrogate) to be
/// replayable. Other propose errors map to [`crate::Error::Dispatch`].
pub(crate) async fn propose_replicated_entry(
    state: &SharedState,
    proposer: &Arc<AsyncRaftProposer>,
    entry: ReplicatedEntry,
) -> crate::Result<(Vec<u8>, crate::types::Lsn)> {
    let idempotency_key = entry.idempotency_key;
    let data = entry.to_bytes();
    let vshard_id = entry.vshard_id;

    let mut payload = None;
    let mut last_err: Option<crate::Error> = None;
    for (attempt, backoff_ms) in BACKOFF_MS.iter().enumerate() {
        match proposer(vshard_id, idempotency_key, data.clone()).await {
            Ok(p) => {
                payload = Some(p);
                break;
            }
            Err(crate::Error::RetryableLeaderChange {
                group_id,
                log_index,
            }) => {
                state
                    .raft_propose_leader_change_retries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::warn!(
                    attempt,
                    group_id,
                    log_index,
                    "raft entry overwritten by leader change — re-proposing"
                );
                last_err = Some(crate::Error::RetryableLeaderChange {
                    group_id,
                    log_index,
                });
                tokio::time::sleep(std::time::Duration::from_millis(*backoff_ms)).await;
                continue;
            }
            Err(other @ crate::Error::DataPlane(_)) => return Err(other),
            Err(other) => {
                return Err(crate::Error::Dispatch {
                    detail: format!("raft propose failed: {other}"),
                });
            }
        }
    }
    payload.ok_or_else(|| {
        last_err.unwrap_or_else(|| crate::Error::Dispatch {
            detail: "raft propose retries exhausted".into(),
        })
    })
}
