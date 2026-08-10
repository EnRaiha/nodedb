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
    /// The sync session's real remote address (`session.device_metadata.remote_addr`),
    /// threaded down to the one `RequestAdmission::NotYetAdmitted`
    /// `dispatch_for_identity` call this snapshot path makes, for the
    /// IP-blacklist half of `check_request_admission`.
    pub peer_addr: &'a str,
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
        peer_addr,
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
                peer_addr,
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
    peer_addr: &'a str,
}

/// Scan a document collection for the subscription's initial dataset.
///
/// The scan carries row-level security: it is a read on the subscriber's
/// behalf, so the subscriber's policies apply to it exactly as they would to
/// the same rows fetched over SQL. A `RangeScan` has no filter slot, so a
/// collection carrying a read policy refuses here rather than streaming
/// unfiltered rows into a client's local replica — where the policy would have
/// no further chance to apply.
///
/// Column redaction applies for the same reason, and is applied to the
/// delivered payload here rather than left to the SELECT-path shaping core
/// this dispatch never reaches — see [`super::payload`].
async fn document_snapshot(req: DocumentSnapshot<'_>) -> Option<ShapeSnapshotData> {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::shared::ddl::user_dispatch::dispatch_for_identity;
    use nodedb_physical::physical_plan::DocumentOp;

    use super::payload::{
        SnapshotPayload, finalize_snapshot, predicate_redacted_field, snapshot_redaction,
    };

    let plan = PhysicalPlan::Document(DocumentOp::RangeScan {
        collection: req.collection.to_string(),
        field: String::new(),
        lower: None,
        upper: None,
        limit: 10_000,
        rls_filters: Vec::new(),
    });

    // Resolved once for the whole snapshot, before `plan` is moved into the
    // dispatch, and from the subscriber's own identity — every row of this
    // payload is delivered under it.
    let redaction = snapshot_redaction(req.shared, req.identity, req.database_id, &plan);

    // A predicate that probes a redacted column discloses it through row
    // presence no matter how the delivered cell is masked, so the subscription
    // is refused rather than answered.
    if let Some(field) = predicate_redacted_field(req.predicate, &redaction, &req.shared.redaction)
    {
        warn!(
            shape_id = %req.shape_id,
            collection = %req.collection,
            field = %field,
            "shape snapshot refused: the shape predicate filters on a redacted column"
        );
        return None;
    }

    // The subscriber's own capability, not the system door: the scan is
    // authorized into a task, row-level security is applied to it, and that
    // exact plan is what reaches storage.
    //
    // Unlike the DDL/DSL passthrough handlers, this call is NOT reached
    // through `shared::ddl::dispatch` — `handle_shape_subscribe_async`
    // deliberately runs only blacklist + account status + quota before this
    // point (shape subscription is not the per-query traffic the
    // rate-limiter's cost table models), so this is the one place this
    // request is ever admitted. `NotYetAdmitted` keeps it that way.
    match dispatch_for_identity(
        crate::control::server::shared::ddl::user_dispatch::DispatchRequest {
            state: req.shared,
            identity: req.identity,
            database_id: req.database_id,
            collection: req.collection,
            plan,
            timeout: Duration::from_secs(10),
            admission: crate::control::server::shared::ddl::user_dispatch::RequestAdmission::NotYetAdmitted {
                peer_addr: req.peer_addr,
            },
        },
    )
    .await
    {
        Ok(payload) => finalize_snapshot(SnapshotPayload {
            payload,
            predicate: req.predicate,
            shape_id: req.shape_id,
            redaction: &redaction,
            store: &req.shared.redaction,
        }),
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
