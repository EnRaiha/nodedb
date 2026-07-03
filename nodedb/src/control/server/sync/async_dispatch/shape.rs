// SPDX-License-Identifier: BUSL-1.1

//! Shape-subscription snapshot dispatch helpers (subscribe + resync).

use std::time::Duration;

use tracing::{info, warn};

use crate::control::state::SharedState;

use super::super::wire::{SyncFrame, SyncMessageType};

/// Handle ShapeSubscribe with real WAL LSN and Data Plane snapshot.
pub(crate) async fn handle_shape_subscribe_async(
    shared: &SharedState,
    session: &super::super::session::SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use crate::types::TenantId;

    let msg: super::super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
    let tenant_id = session.tenant_id.map(|t| t.as_u64()).unwrap_or(0);

    // Quota enforcement — reject before dispatch.
    let tid = TenantId::new(tenant_id);
    if let Err(e) = shared.check_tenant_quota(tid) {
        warn!(tenant_id, error = %e, "sync: shape subscribe rejected by quota");
        return None;
    }

    // Get current WAL LSN — this is the watermark for the snapshot.
    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data =
        take_shape_snapshot_async(shared, &session.session_id, &msg.shape, tid).await;

    // Register the shape subscription in the persistent registry.
    let response = super::super::shape::handler::handle_subscribe(
        &session.session_id,
        tenant_id,
        &msg,
        &shared.shape_registry,
        current_lsn,
        |_shape, _lsn| snapshot_data,
    );

    info!(
        session = %session.session_id,
        shape_id = %msg.shape.shape_id,
        lsn = current_lsn,
        "shape subscribed with WAL LSN watermark"
    );

    response
}

/// Produce the initial snapshot payload for a shape definition.
///
/// Dispatches into the Data Plane for Document shapes; returns lightweight
/// or empty payloads for Vector / Graph / Array (see inline comments).
/// Caller is responsible for quota accounting (tenant_request_start/end).
async fn take_shape_snapshot_async(
    shared: &SharedState,
    session_id: &str,
    shape: &nodedb_types::sync::shape::ShapeDefinition,
    tid: crate::types::TenantId,
) -> super::super::shape::handler::ShapeSnapshotData {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::shared::ddl::sync_dispatch::dispatch_async;
    use nodedb_physical::physical_plan::DocumentOp;

    shared.tenant_request_start(tid);
    let result = match &shape.shape_type {
        nodedb_types::sync::shape::ShapeType::Document {
            collection,
            predicate,
        } => {
            let plan = PhysicalPlan::Document(DocumentOp::RangeScan {
                collection: collection.clone(),
                field: String::new(),
                lower: None,
                upper: None,
                limit: 10_000,
            });
            // A5 (deferred): shape snapshot has no session database.
            match dispatch_async(
                shared,
                tid,
                crate::types::DatabaseId::DEFAULT,
                collection,
                plan,
                Duration::from_secs(10),
            )
            .await
            {
                Ok(payload) => filter_snapshot_by_predicate(payload, predicate, &shape.shape_id),
                Err(e) => {
                    warn!(
                        shape_id = %shape.shape_id,
                        error = %e,
                        "shape snapshot query failed, sending empty snapshot"
                    );
                    super::super::shape::handler::ShapeSnapshotData::empty()
                }
            }
        }
        nodedb_types::sync::shape::ShapeType::Vector { collection, .. } => {
            super::super::shape::handler::ShapeSnapshotData {
                data: collection.as_bytes().to_vec(),
                doc_count: 0,
            }
        }
        nodedb_types::sync::shape::ShapeType::Graph { .. } => {
            super::super::shape::handler::ShapeSnapshotData::empty()
        }
        nodedb_types::sync::shape::ShapeType::Array {
            array_name,
            coord_range,
        } => {
            let array_known = shared.array_sync_schemas.schema_hlc(array_name).is_some();
            if !array_known {
                warn!(
                    session = session_id,
                    array = %array_name,
                    "array shape subscribe: array not known to Origin schema registry"
                );
                shared.tenant_request_end(tid);
                return super::super::shape::handler::ShapeSnapshotData::empty();
            }
            shared
                .array_subscriber_cursors
                .register(session_id, array_name, coord_range.clone());
            info!(
                session = session_id,
                array = %array_name,
                "array shape subscribed; cursor initialized at HLC::ZERO"
            );
            super::super::shape::handler::ShapeSnapshotData::empty()
        }
        _ => {
            warn!(
                session = session_id,
                "shape subscribe: unknown shape_type variant, sending empty snapshot"
            );
            super::super::shape::handler::ShapeSnapshotData::empty()
        }
    };
    shared.tenant_request_end(tid);
    result
}

/// Re-snapshot a previously subscribed shape in response to a ResyncRequest.
///
/// Decodes the request, enforces tenant quota, looks up the shape in the
/// persistent registry, runs the same snapshot machinery as subscribe, and
/// returns a ShapeSnapshot frame re-based at the current WAL LSN.
pub(crate) async fn handle_resync_request_async(
    shared: &SharedState,
    session: &super::super::session::SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use crate::types::TenantId;
    use nodedb_types::sync::wire::ResyncRequestMsg;

    let msg: ResyncRequestMsg = frame.decode_body()?;
    let tenant_id = session.tenant_id.map(|t| t.as_u64()).unwrap_or(0);
    let tid = TenantId::new(tenant_id);

    if let Err(e) = shared.check_tenant_quota(tid) {
        warn!(tenant_id, error = %e, "sync: resync request rejected by quota");
        return None;
    }

    if msg.shape_id.is_empty() {
        warn!(
            session = %session.session_id,
            "resync request missing shape_id; ignoring"
        );
        return None;
    }

    let shape = match shared
        .shape_registry
        .get_shape(&session.session_id, &msg.shape_id)
    {
        Some(s) => s,
        None => {
            warn!(
                session = %session.session_id,
                shape_id = %msg.shape_id,
                "resync for unknown or unsubscribed shape; ignoring"
            );
            return None;
        }
    };

    let current_lsn = shared.wal.next_lsn().as_u64().saturating_sub(1);

    let snapshot_data = take_shape_snapshot_async(shared, &session.session_id, &shape, tid).await;

    let snapshot = super::super::shape::handler::ShapeSnapshotMsg {
        shape_id: msg.shape_id.clone(),
        data: snapshot_data.data,
        snapshot_lsn: current_lsn,
        doc_count: snapshot_data.doc_count,
    };

    info!(
        session = %session.session_id,
        shape_id = %msg.shape_id,
        lsn = current_lsn,
        doc_count = snapshot.doc_count,
        "resync snapshot sent"
    );

    SyncFrame::try_encode(SyncMessageType::ShapeSnapshot, &snapshot)
}

// ── Snapshot predicate filtering ──────────────────────────────────────────────

/// Filter a raw snapshot payload by a shape predicate.
///
/// Decodes the msgpack document rows, evaluates each document's data bytes
/// against the `MetadataFilter` decoded from `predicate_bytes`, and re-encodes
/// only the matching rows. An empty predicate returns the payload unchanged.
/// A predicate that fails to decode is logged as a warning and the entire
/// snapshot is returned empty (fail-closed, consistent with delta routing).
fn filter_snapshot_by_predicate(
    payload: Vec<u8>,
    predicate_bytes: &[u8],
    shape_id: &str,
) -> super::super::shape::handler::ShapeSnapshotData {
    use crate::data::executor::response_codec::{
        decode_raw_scan_to_docs, encode_raw_document_rows,
    };
    use nodedb_query::metadata_filter::matches_metadata_filter;
    use nodedb_types::filter::MetadataFilter;

    if predicate_bytes.is_empty() {
        let doc_count = decode_raw_scan_to_docs(&payload).len();
        return super::super::shape::handler::ShapeSnapshotData {
            data: payload,
            doc_count,
        };
    }

    let filter = match zerompk::from_msgpack::<MetadataFilter>(predicate_bytes) {
        Ok(f) => f,
        Err(err) => {
            warn!(
                shape_id,
                error = %err,
                "shape snapshot: failed to decode predicate; sending empty snapshot"
            );
            return super::super::shape::handler::ShapeSnapshotData::empty();
        }
    };

    let docs = decode_raw_scan_to_docs(&payload);
    let mut matching: Vec<(String, Vec<u8>)> = Vec::new();

    for (doc_id, data_bytes) in docs {
        let doc_json = crate::control::server::sync::security::delta_bytes_to_json(&data_bytes);
        if matches_metadata_filter(&doc_json, &filter) {
            matching.push((doc_id, data_bytes));
        }
    }

    let doc_count = matching.len();
    match encode_raw_document_rows(&matching) {
        Ok(data) => super::super::shape::handler::ShapeSnapshotData { data, doc_count },
        Err(err) => {
            // Fail closed: a re-encode failure must not ship a header whose
            // doc_count disagrees with its (empty) body. Drop the snapshot,
            // matching the predicate-decode failure path above.
            warn!(
                shape_id,
                error = %err,
                "shape snapshot: failed to encode filtered rows; sending empty snapshot"
            );
            super::super::shape::handler::ShapeSnapshotData::empty()
        }
    }
}
