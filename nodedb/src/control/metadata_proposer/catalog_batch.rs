// SPDX-License-Identifier: BUSL-1.1

//! Propose several catalog entries as one metadata commit.

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry};

use crate::control::catalog_entry::{self, CatalogEntry};
use crate::control::security::catalog::SystemCatalog;
use crate::control::server::shared::session::ddl_buffer;
use crate::control::state::SharedState;
use crate::error::Error;

use super::catalog::{catalog_ddl_entry, propose_prepared};
use super::ddl_prepare::{acquire_ddl_prepare_lease_async, lock_ddl_preparation_async};
use super::handle::MetadataRaftHandle;
use super::timeouts::{DEFAULT_DRAIN_TIMEOUT, DEFAULT_PROPOSE_TIMEOUT};

/// What happened to the entries a batch plan built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchOutcome {
    /// Replicated through the metadata group and applied here at this index.
    Replicated { log_index: u64 },
    /// Every entry joined the open DDL transaction.
    Buffered,
    /// The plan built no entries: nothing was proposed, buffered, or applied.
    Empty,
}

/// The entries a batch plan built, and what happened to them.
pub struct CatalogBatch {
    pub outcome: BatchOutcome,
    /// Stamped as applied on the `Replicated` path.
    pub entries: Vec<CatalogEntry>,
}

impl CatalogBatch {
    fn empty() -> Self {
        Self {
            outcome: BatchOutcome::Empty,
            entries: Vec::new(),
        }
    }
}

/// Build entries with `plan` and propose them as one metadata commit, on any
/// runtime flavor.
///
/// `plan` runs under the DDL preparation lock and lease, so no other DDL
/// changes the catalog between the plan and the commit. The batch applies at
/// one log index on every node, entry by entry in plan order, and a restart
/// replays it as one unit.
///
/// Every wait is awaited: the preparation lock and lease, the descriptor
/// drains, the commit's apply, and the authorization barrier.
pub async fn propose_catalog_batch_async(
    shared: &SharedState,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> Result<CatalogBatch, Error> {
    let catalog = shared.credentials.catalog();
    if ddl_buffer::is_active() {
        let entries = plan(catalog)?;
        if entries.is_empty() {
            return Ok(CatalogBatch::empty());
        }
        for entry in &entries {
            ddl_buffer::try_buffer(entry.clone());
        }
        return Ok(CatalogBatch {
            outcome: BatchOutcome::Buffered,
            entries,
        });
    }
    let handle = shared.metadata_raft_handle()?;

    // The guards drop before the authorization barrier below, as on the
    // single-entry path.
    let Some((log_index, entries)) = propose_replicated(shared, handle.as_ref(), plan).await?
    else {
        return Ok(CatalogBatch::empty());
    };
    if entries.iter().any(CatalogEntry::bears_authorization) {
        crate::control::security::auth_lease::authorization_barrier(
            shared,
            vec![nodedb_cluster::GroupCoverage {
                group_id: METADATA_GROUP_ID,
                through: log_index,
            }],
        )
        .await?;
    }
    Ok(CatalogBatch {
        outcome: BatchOutcome::Replicated { log_index },
        entries,
    })
}

/// Plan, drain, stamp, and propose a batch under the preparation lock and
/// lease, and wait until this node applied it. Returns its log index and its
/// stamped entries, or `None` for a plan with no entries. Both guards are
/// released before it returns.
async fn propose_replicated(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> Result<Option<(u64, Vec<CatalogEntry>)>, Error> {
    let _local_ddl_guard = lock_ddl_preparation_async(shared).await;
    let lease = acquire_ddl_prepare_lease_async(shared, handle).await?;
    let proposed: Result<Option<(u64, Vec<CatalogEntry>)>, Error> = async {
        let Some(entries) = plan_drain_and_stamp(shared, plan).await? else {
            return Ok::<_, Error>(None);
        };
        let mut wrapped = Vec::with_capacity(entries.len());
        for entry in &entries {
            wrapped.push(catalog_ddl_entry(entry)?);
        }
        // The wait follows apply progress: a batch of slow purges keeps it
        // alive, and only a stall of `DEFAULT_PROPOSE_TIMEOUT` ends it.
        let log_index = propose_prepared(
            shared,
            handle,
            lease.token(),
            MetadataEntry::Batch { entries: wrapped },
            DEFAULT_PROPOSE_TIMEOUT,
        )
        .await?;
        Ok::<_, Error>(Some((log_index, entries)))
    }
    .await;
    lease.release().await;
    proposed
}

/// Plan, drain, and stamp a batch. The caller holds the DDL preparation lock
/// and lease. `None` is a plan with no entries.
///
/// The batch's apply ends every drain it started. A failure before the batch
/// is proposed ends them here: a drain has no wall-clock expiry.
async fn plan_drain_and_stamp(
    shared: &SharedState,
    plan: impl FnOnce(&SystemCatalog) -> crate::Result<Vec<CatalogEntry>>,
) -> Result<Option<Vec<CatalogEntry>>, Error> {
    let catalog = shared.credentials.catalog();
    let entries = plan(catalog)?;
    if entries.is_empty() {
        return Ok(None);
    }
    if let Err(error) = drain_batch(shared, &entries).await {
        return Err(end_batch_drains(shared, &entries, error).await);
    }
    match catalog_entry::descriptor_stamp::stamp_batch(entries.clone(), &shared.hlc_clock, catalog)
    {
        Ok(stamped) => Ok(Some(stamped)),
        Err(error) => Err(end_batch_drains(shared, &entries, error).await),
    }
}

/// End the drain of every entry of a batch that failed before it was
/// proposed. Returns `error`, the failure that stopped the batch.
///
/// Every node installed the drains, so each DDL drain ends through the
/// metadata group.
async fn end_batch_drains(shared: &SharedState, entries: &[CatalogEntry], error: Error) -> Error {
    for entry in entries {
        if let Err(end) = end_replicated_drain(shared, entry).await {
            tracing::warn!(
                kind = entry.kind(),
                error = %end,
                "metadata batch: the drain of an unapplied entry did not end"
            );
        }
    }
    error
}

/// End, through the metadata group, the DDL drain [`drain_batch`] started for
/// `entry`. Ending a drain that is not active is a no-op on every node.
async fn end_replicated_drain(shared: &SharedState, entry: &CatalogEntry) -> Result<(), Error> {
    match crate::control::lease::descriptor_id_and_prior_version(entry, shared) {
        Some((descriptor_id, prior_version)) if prior_version > 0 => {
            crate::control::lease::end_drain_async(
                shared,
                descriptor_id,
                nodedb_cluster::DrainOwner::Ddl,
            )
            .await
        }
        _ => Ok(()),
    }
}

/// Drain the prior version of every descriptor the batch changes.
async fn drain_batch(shared: &SharedState, entries: &[CatalogEntry]) -> Result<(), Error> {
    for entry in entries {
        if let Some((descriptor_id, prior_version)) =
            crate::control::lease::descriptor_id_and_prior_version(entry, shared)
            && prior_version > 0
        {
            crate::control::lease::drain_for_ddl_async(
                shared,
                descriptor_id,
                prior_version,
                DEFAULT_DRAIN_TIMEOUT,
                0,
            )
            .await?;
        }
    }
    Ok(())
}
