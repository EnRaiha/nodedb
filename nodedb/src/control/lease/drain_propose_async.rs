// SPDX-License-Identifier: BUSL-1.1

//! The descriptor lease drain, proposed and awaited.
//!
//! The flow is the one [`super::drain_propose`] describes. Every wait is
//! awaited: the drain variants' metadata commits apply while the proposer's
//! task yields, and the lease wait sleeps until a hold or lease changes or
//! the timer fires. No runtime flavor loses a worker to it.

use std::time::{Duration, Instant};

use nodedb_cluster::{DescriptorId, DrainOwner, MetadataEntry};

use crate::control::metadata_proposer::wait::wait_applied;
use crate::control::state::SharedState;
use crate::error::Error;

use super::drain_propose::{
    DRAIN_PROPOSE_TIMEOUT, POLL_INTERVAL, drain_applied_or_error, drain_start_entry,
    drained_or_timed_out, encode_drain,
};

/// Drain every lease on `id` at `version <= up_to_version` for a `Put*` DDL.
///
/// The drain is owned by [`DrainOwner::Ddl`], so the apply of the DDL's own
/// catalog entry ends it and no other owner's drain.
///
/// Returns `Ok(())` once they have drained. Errors on timeout or propose
/// failure.
///
/// `own_holds` is how many of those refcount units the requesting transaction
/// holds itself — `0` for a caller with no lease scope of its own. A
/// transaction altering a descriptor it also holds a statement-time lease on
/// cannot wait for its own hold: it cannot release that lease until this
/// call returns.
pub async fn drain_for_ddl_async(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    start_drain(
        shared,
        id,
        DrainOwner::Ddl,
        up_to_version,
        max_wait,
        own_holds,
    )
    .await
}

/// Drain every lease on `id`, at any version, under `owner`.
///
/// For a holder that stops all writes for a span of work rather than for one
/// version bump. Covering every version keeps the drain in force when a DDL
/// bumps the descriptor's version meanwhile. Only [`end_drain_async`] with the
/// same `owner`, or the owner's own implicit clear, ends it.
pub async fn drain_for_owner_async(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
    max_wait: Duration,
) -> Result<(), Error> {
    start_drain(shared, id, owner, u64::MAX, max_wait, 0).await
}

async fn start_drain(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // No prior version means no lease can exist.
    if up_to_version == 0 {
        return Ok(());
    }
    propose_drain_async(
        shared,
        drain_start_entry(shared, &id, &owner, up_to_version, max_wait),
        "drain_start",
    )
    .await?;

    let deadline = Instant::now() + max_wait;
    let drained = loop {
        // A hold given back or a lease release applied wakes the wait. The
        // timer covers what no event marks: a lease expiry, a holder's death.
        let changed = shared.lease_drain.holds_changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        match drained_or_timed_out(shared, &id, up_to_version, max_wait, own_holds, deadline) {
            Ok(true) => break Ok(()),
            Ok(false) => {
                tokio::select! {
                    _ = changed.as_mut() => {}
                    _ = tokio::time::sleep(POLL_INTERVAL) => {}
                }
            }
            Err(error) => break Err(error),
        }
    };
    if let Err(error) = drained {
        // `is_draining` has no expiry backstop, so this explicit propose is
        // the only thing that clears the drain after a timeout. Its own
        // errors are logged and dropped.
        if let Err(cleanup_err) = end_drain_async(shared, id, owner).await {
            tracing::warn!(
                error = %cleanup_err,
                "descriptor lease drain: cleanup propose failed after timeout"
            );
        }
        return Err(error);
    }
    Ok(())
}

/// End `owner`'s drain on `id` on every node, and wait until the end applies
/// on this node. Other owners' drains on `id` stay.
///
/// For a DDL whose own catalog entry never commits, so no implicit clear
/// runs, and for every [`drain_for_owner_async`] holder. Ending a drain that
/// is not active is a no-op on every node.
pub async fn end_drain_async(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
) -> Result<(), Error> {
    propose_drain_async(
        shared,
        MetadataEntry::DescriptorDrainEnd {
            descriptor_id: id,
            owner,
        },
        "drain_end",
    )
    .await
}

/// Propose a drain variant and await its apply on this node.
async fn propose_drain_async(
    shared: &SharedState,
    entry: MetadataEntry,
    operation: &'static str,
) -> Result<(), Error> {
    let handle = shared.metadata_raft_handle()?;
    let log_index = handle
        .propose_async(encode_drain(&entry, operation)?)
        .await?;
    let watcher = shared.applied_index_watcher(nodedb_cluster::METADATA_GROUP_ID);
    let outcome = wait_applied(
        std::sync::Arc::clone(&watcher),
        log_index,
        DRAIN_PROPOSE_TIMEOUT,
    )
    .await?;
    drain_applied_or_error(outcome, operation, log_index, watcher.current())
}
