// SPDX-License-Identifier: BUSL-1.1

//! CDC change-event publishing for dispatched writes: turning the metadata
//! [`super::extract`] derived from a plan into `ChangeEvent`s on the
//! change stream, at the position of the write in its partition's feed.

use std::sync::Arc;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::change_stream::{ChangeEvent, ChangePartition, ChangeRun, PositionedChange};
use crate::control::state::SharedState;
use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, TenantId};

use super::extract::{WriteChangeMeta, extract_write_metadata};

const NANOS_PER_MS: u64 = 1_000_000;

/// The change-event timestamp of a write that committed at `commit_hlc`
/// (HLC wall time, nanoseconds). Every replica reads the same commit HLC
/// from the committed entry, so every node stamps the same value.
fn commit_timestamp_ms(commit_hlc: u64) -> u64 {
    commit_hlc / NANOS_PER_MS
}

/// Check if a timeseries collection has CDC enabled.
///
/// Returns `false` (CDC off) by default for timeseries to prevent
/// high-cardinality metric streams from flooding the ChangeStream bus.
/// Users opt in via `CREATE TIMESERIES name WITH (cdc = 'true')`.
fn is_timeseries_cdc_enabled(
    shared: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let catalog = shared.credentials.catalog();
    if let Ok(Some(coll)) = catalog.get_collection(database_id, tenant_id.as_u64(), collection)
        && coll.collection_type.is_timeseries()
    {
        if let Some(config) = coll.get_timeseries_config()
            && let Some(cdc_val) = config.get("cdc")
        {
            return cdc_val.as_str() == Some("true") || cdc_val.as_bool() == Some(true);
        }
        // Default: CDC off for timeseries.
        return false;
    }
    // Not timeseries or catalog unavailable — allow publishing.
    true
}

/// The change event of one row change, or `None` when its collection
/// publishes none: a timeseries collection publishes only with `cdc`
/// enabled.
///
/// The plan names the collection database-qualified. The event names it
/// bare, as the catalog and every subscriber filter do; its database travels
/// beside it.
fn change_event(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_meta: WriteChangeMeta,
    lsn: nodedb_types::Lsn,
    commit_hlc: u64,
) -> Option<ChangeEvent> {
    let (qualified, document_id, op) = change_meta;
    let collection = match nodedb_types::CollectionKey::from_qualified_str(database_id, &qualified)
    {
        Ok(key) => key.name().to_owned(),
        Err(error) => {
            tracing::error!(
                %error,
                collection = %qualified,
                "a row change names a collection outside its database; it publishes no event"
            );
            return None;
        }
    };
    if !is_timeseries_cdc_enabled(shared, database_id, tenant_id, &collection) {
        return None;
    }
    Some(ChangeEvent {
        lsn,
        tenant_id,
        collection,
        document_id,
        operation: op,
        timestamp_ms: commit_timestamp_ms(commit_hlc),
        after: None,
    })
}

/// The Control-Plane change events one write plan yields.
///
/// Extraction is split from publishing because the write funnel consumes the
/// plan — it moves into the `Request` — long before the `Response` the event's
/// LSN comes from exists.
pub(crate) struct WriteChangeSet {
    /// One tuple per logical row change — see `extract_write_metadata`.
    metas: Vec<WriteChangeMeta>,
}

/// Derive a plan's change events. Pure: it matches over the plan and clones out
/// collection / document identity, touching no shared state, so it is safe to
/// call at the one point where the plan is still owned.
pub(crate) fn extract_write_change_set(plan: &PhysicalPlan, tenant_id: TenantId) -> WriteChangeSet {
    WriteChangeSet {
        metas: extract_write_metadata(plan, tenant_id),
    }
}

/// The change events of a committed transaction's redo record `redo`: one per
/// row it installs. A Calvin commit publishes these, as the data-group apply
/// of a `TransactionRedo` entry does.
pub(crate) fn redo_change_set(redo: &[u8]) -> WriteChangeSet {
    WriteChangeSet {
        metas: super::redo::redo_change_meta(redo),
    }
}

fn change_events(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    change_set: WriteChangeSet,
    lsn: nodedb_types::Lsn,
    commit_hlc: u64,
) -> Vec<ChangeEvent> {
    change_set
        .metas
        .into_iter()
        .filter_map(|meta| change_event(shared, tenant_id, database_id, meta, lsn, commit_hlc))
        .collect()
}

impl WriteChangeSet {
    /// Whether the write yields no change event.
    pub(crate) fn is_empty(&self) -> bool {
        self.metas.is_empty()
    }
}

/// A replicated write's change set, staged under its data-group entry once
/// the write applied.
pub(crate) struct PendingChanges {
    change_set: WriteChangeSet,
    /// The data group of the committed entry the write applies.
    group_id: u64,
    /// The entry's log index in its group.
    log_index: u64,
    /// HLC wall time, in nanoseconds, at which the write committed.
    commit_hlc: u64,
}

impl PendingChanges {
    /// Staged under the data-group entry `(group_id, log_index)` once it
    /// applies, and published when the apply loop settles the entry in log
    /// order ([`publish_settled_changes`]).
    pub(crate) fn staged(change_set: WriteChangeSet, group_id: u64, log_index: u64) -> Self {
        Self {
            change_set,
            group_id,
            log_index,
            commit_hlc: 0,
        }
    }

    /// Stamp the write's commit HLC, which dates its events. A replicated
    /// write takes the HLC its proposer stamped on the entry.
    pub(crate) fn committed_at(mut self, commit_hlc: u64) -> Self {
        self.commit_hlc = commit_hlc;
        self
    }

    /// Stage the write's events, at the LSN of its Data-Plane [`Response`],
    /// under its entry. Almost every write plan yields exactly one event; a
    /// handful of multi-row / multi-collection ops yield more than one, and
    /// reads / DDL / index maintenance yield none.
    pub(crate) fn publish(
        self,
        shared: &SharedState,
        tenant_id: TenantId,
        database_id: DatabaseId,
        response: &Response,
    ) {
        let events = change_events(
            shared,
            tenant_id,
            database_id,
            self.change_set,
            response.watermark_lsn,
            self.commit_hlc,
        );
        shared
            .change_stream
            .stage(self.group_id, self.log_index, database_id, events);
    }
}

/// Publish the staged changes of `group_id`'s entries `first..=last`, which
/// the apply loop settled in log order, and forward them to the nodes that
/// do not replicate the group when this node leads it.
///
/// Every replica publishes the group's writes at their log positions, so
/// every node serves the same feed.
pub(crate) fn publish_settled_changes(
    shared: &Arc<SharedState>,
    group_id: u64,
    first: u64,
    last: u64,
) {
    shared.change_stream.settle_group(group_id, first, last);
    shared
        .change_stream
        .forward(shared, ChangePartition::Group(group_id));
}

/// Publish the changes of the Calvin transaction at `(sequencer_epoch,
/// position)` that vShard `vshard` applied, and forward them when this node
/// leads the vShard's data group.
///
/// Every replica's scheduler applies the vShard's transactions in sequencer
/// order, so every replica publishes them at the same positions.
pub(crate) fn publish_calvin_change_sets(
    shared: &Arc<SharedState>,
    calvin: CalvinApply,
    change_sets: Vec<WriteChangeSet>,
    lsn: nodedb_types::Lsn,
) {
    let CalvinApply {
        tenant_id,
        database_id,
        vshard,
        sequencer_epoch,
        position,
        commit_hlc,
    } = calvin;
    let base = u64::from(position);
    let changes: Vec<PositionedChange> = change_sets
        .into_iter()
        .flat_map(|change_set| {
            change_events(shared, tenant_id, database_id, change_set, lsn, commit_hlc)
        })
        .enumerate()
        .map(|(ordinal, event)| PositionedChange {
            position: CdcOffset::data_event_in(0, sequencer_epoch, base, ordinal as u64 + 1),
            database_id,
            event,
        })
        .collect();
    if changes.is_empty() {
        return;
    }
    shared.change_stream.publish_run(ChangeRun {
        partition: ChangePartition::Calvin(vshard),
        after: None,
        through: CdcOffset::data_event_in(0, sequencer_epoch, base, u64::MAX).correction(),
        changes,
    });
    shared
        .change_stream
        .forward(shared, ChangePartition::Calvin(vshard));
}

/// The Calvin transaction a published change set belongs to.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CalvinApply {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard: u32,
    pub sequencer_epoch: u64,
    pub position: u32,
    /// The transaction's commit HLC, which every replica derives alike.
    pub commit_hlc: u64,
}
