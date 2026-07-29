// SPDX-License-Identifier: BUSL-1.1

//! Shape snapshot production: plan construction, RLS, and predicate filtering.

use std::time::Duration;

use tracing::{info, warn};

use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::sync::shape::handler::ShapeSnapshotData;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// Everything a snapshot needs, resolved from the authorized session.
///
/// Tenant and database come from the handshake identity, never from the shape
/// body — a client cannot point a subscription at another tenant's or another
/// database's copy of a collection name.
pub(super) struct SnapshotRequest<'a> {
    pub shared: &'a SharedState,
    pub session_id: &'a str,
    pub shape: &'a ShapeDefinition,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
}

/// Produce the initial snapshot payload for a shape definition.
///
/// Dispatches into the Data Plane for Document shapes; returns lightweight or
/// empty payloads for Vector / Graph / Array (see inline comments).
///
/// Returns `None` when the snapshot could not be produced — a policy refusal or
/// a failed query. The caller sends no `ShapeSnapshot` at all in that case: an
/// empty snapshot is an assertion that the shape matches nothing, and a client
/// that believes it has a complete empty baseline will never ask again. An
/// intentionally empty snapshot (a Graph shape, an unmatched Array) is still
/// `Some`, because that answer is real.
pub(super) async fn take_shape_snapshot(req: SnapshotRequest<'_>) -> Option<ShapeSnapshotData> {
    let SnapshotRequest {
        shared,
        session_id,
        shape,
        identity,
        tenant_id,
        database_id,
    } = req;

    let _request = shared.tenant_request_guard(tenant_id);
    match &shape.shape_type {
        ShapeType::Document {
            collection,
            predicate,
        } => {
            document_snapshot(DocumentSnapshot {
                shared,
                shape_id: &shape.shape_id,
                collection,
                predicate,
                identity,
                database_id,
            })
            .await
        }
        ShapeType::Vector { collection, .. } => Some(ShapeSnapshotData {
            data: collection.as_bytes().to_vec(),
            doc_count: 0,
        }),
        ShapeType::Graph { .. } => Some(ShapeSnapshotData::empty()),
        ShapeType::Array {
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
                return Some(ShapeSnapshotData::empty());
            }
            shared
                .array_subscriber_cursors
                .register(session_id, array_name, coord_range.clone());
            info!(
                session = session_id,
                array = %array_name,
                "array shape subscribed; cursor initialized at HLC::ZERO"
            );
            Some(ShapeSnapshotData::empty())
        }
        _ => {
            warn!(
                session = session_id,
                "shape subscribe: unknown shape_type variant, sending empty snapshot"
            );
            Some(ShapeSnapshotData::empty())
        }
    }
}

struct DocumentSnapshot<'a> {
    shared: &'a SharedState,
    shape_id: &'a str,
    collection: &'a str,
    predicate: &'a [u8],
    identity: &'a AuthenticatedIdentity,
    database_id: DatabaseId,
}

/// Scan a document collection for the subscription's initial dataset.
///
/// The scan carries row-level security: it is a read on the subscriber's
/// behalf, so the subscriber's policies apply to it exactly as they would to
/// the same rows fetched over SQL. A `RangeScan` has no filter slot, so a
/// collection carrying a read policy refuses here rather than streaming
/// unfiltered rows into a client's local replica — where the policy would have
/// no further chance to apply.
async fn document_snapshot(req: DocumentSnapshot<'_>) -> Option<ShapeSnapshotData> {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity;
    use nodedb_physical::physical_plan::DocumentOp;

    let plan = PhysicalPlan::Document(DocumentOp::RangeScan {
        collection: req.collection.to_string(),
        field: String::new(),
        lower: None,
        upper: None,
        limit: 10_000,
        rls_filters: Vec::new(),
    });

    // The subscriber's own capability, not the system door: the scan is
    // authorized into a task, row-level security is applied to it, and that
    // exact plan is what reaches storage.
    match dispatch_for_identity(
        req.shared,
        req.identity,
        req.database_id,
        req.collection,
        plan,
        Duration::from_secs(10),
    )
    .await
    {
        Ok(payload) => Some(filter_snapshot_by_predicate(
            payload,
            req.predicate,
            req.shape_id,
        )),
        Err(error) => {
            warn!(
                shape_id = %req.shape_id,
                %error,
                "shape snapshot query failed; sending no snapshot"
            );
            None
        }
    }
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
) -> ShapeSnapshotData {
    use crate::control::server::sync::shape::handler::decode_document_or_empty;
    use crate::data::executor::response_codec::{
        decode_raw_scan_to_docs, encode_raw_document_rows,
    };
    use nodedb_query::metadata_filter::matches_metadata_filter;
    use nodedb_types::filter::MetadataFilter;

    if predicate_bytes.is_empty() {
        let doc_count = decode_raw_scan_to_docs(&payload).len();
        return ShapeSnapshotData {
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
            return ShapeSnapshotData::empty();
        }
    };

    let docs = decode_raw_scan_to_docs(&payload);
    let mut matching: Vec<(String, Vec<u8>)> = Vec::new();

    for (doc_id, data_bytes) in docs {
        let doc_json = decode_document_or_empty(&data_bytes);
        if matches_metadata_filter(&doc_json, &filter) {
            matching.push((doc_id, data_bytes));
        }
    }

    let doc_count = matching.len();
    match encode_raw_document_rows(&matching) {
        Ok(data) => ShapeSnapshotData { data, doc_count },
        Err(err) => {
            // Fail closed: a re-encode failure must not ship a header whose
            // doc_count disagrees with its (empty) body. Drop the snapshot,
            // matching the predicate-decode failure path above.
            warn!(
                shape_id,
                error = %err,
                "shape snapshot: failed to encode filtered rows; sending empty snapshot"
            );
            ShapeSnapshotData::empty()
        }
    }
}
