// SPDX-License-Identifier: BUSL-1.1

//! Write-metadata extraction and CDC change-event publishing for dispatched
//! writes.

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::change_stream::ChangeOperation;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, TimeseriesOp};

/// Current wall-clock time as milliseconds since Unix epoch.
///
/// Returns 0 if the system clock is before the epoch (should never happen
/// on correctly configured systems).
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Extract write metadata from a physical plan for change event publishing.
///
/// `_tenant_id` is reserved for future tenant-scoped change stream filtering.
pub(super) fn extract_write_metadata(
    plan: &PhysicalPlan,
    _tenant_id: TenantId,
) -> Option<(String, String, ChangeOperation)> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )),
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Update,
        )),
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            ..
        }) => Some((
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Update))
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        // Timeseries ingest: batch write. CDC is opt-in for timeseries
        // collections (high-cardinality metrics would flood the bus).
        // The change event uses document_id="*" to indicate a batch.
        // Consumers can subscribe with collection_filter to get these events.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Insert))
        }
        // KV engine write operations.
        PhysicalPlan::Kv(KvOp::Put {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Insert,
        )),
        PhysicalPlan::Kv(KvOp::Delete { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Update,
        )),
        PhysicalPlan::Kv(KvOp::BatchPut { collection, .. }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Insert))
        }
        PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
            Some((collection.clone(), "*".into(), ChangeOperation::Delete))
        }
        PhysicalPlan::Kv(KvOp::Incr {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::IncrFloat {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Cas {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::GetSet {
            collection, key, ..
        }) => Some((
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Update,
        )),
        _ => None,
    }
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

/// Publish a change event (and cluster-wide NOTIFY) for a successful write.
///
/// CDC opt-in check for timeseries: skip publishing unless `cdc_enabled`.
/// Document collections always publish (backward compatible).
pub(super) fn publish_change_event(
    shared: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    is_columnar_collection: bool,
    change_meta: (String, String, ChangeOperation),
    response: &Response,
) {
    let (collection, doc_id, op) = change_meta;
    let should_publish = if is_columnar_collection {
        is_timeseries_cdc_enabled(shared, database_id, tenant_id, &collection)
    } else {
        true
    };
    if !should_publish {
        return;
    }

    use crate::control::change_stream::ChangeEvent;
    let event = ChangeEvent {
        lsn: response.watermark_lsn,
        tenant_id,
        collection,
        document_id: doc_id,
        operation: op,
        timestamp_ms: current_timestamp_ms(),
        after: None,
    };

    // Cluster-wide NOTIFY: broadcast to all peers via QUIC.
    if let (Some(transport), Some(topology)) = (&shared.cluster_transport, &shared.cluster_topology)
    {
        use std::sync::atomic::Ordering;
        static NOTIFY_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let seq = NOTIFY_SEQ.fetch_add(1, Ordering::Relaxed);
        crate::control::change_stream::broadcast_notify_to_cluster(
            &event,
            shared.node_id,
            seq,
            transport,
            topology,
        );
    }

    shared.change_stream.publish(event);
}
