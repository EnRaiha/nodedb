// SPDX-License-Identifier: BUSL-1.1

//! Async Data Plane dispatch helpers for the sync WebSocket listener.
//!
//! Contains async functions that cross the Control Plane / Data Plane boundary
//! via the SPSC bridge: shape-subscription snapshot queries and CRDT delta
//! constraint validation.

use std::time::Duration;

use tracing::{info, warn};

use nodedb_types::sync::wire::{EngineKind, SyncProvenance, stream_id_for};

use crate::control::state::SharedState;

use super::wire::{CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType};

/// Handle ShapeSubscribe with real WAL LSN and Data Plane snapshot.
pub(super) async fn handle_shape_subscribe_async(
    shared: &SharedState,
    session: &super::session::SyncSession,
    frame: &SyncFrame,
) -> Option<SyncFrame> {
    use crate::types::TenantId;

    let msg: super::shape::handler::ShapeSubscribeMsg = frame.decode_body()?;
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
    let response = super::shape::handler::handle_subscribe(
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
) -> super::shape::handler::ShapeSnapshotData {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::control::server::pgwire::ddl::sync_dispatch::dispatch_async;
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
            match dispatch_async(shared, tid, collection, plan, Duration::from_secs(10)).await {
                Ok(payload) => filter_snapshot_by_predicate(payload, predicate, &shape.shape_id),
                Err(e) => {
                    warn!(
                        shape_id = %shape.shape_id,
                        error = %e,
                        "shape snapshot query failed, sending empty snapshot"
                    );
                    super::shape::handler::ShapeSnapshotData::empty()
                }
            }
        }
        nodedb_types::sync::shape::ShapeType::Vector { collection, .. } => {
            super::shape::handler::ShapeSnapshotData {
                data: collection.as_bytes().to_vec(),
                doc_count: 0,
            }
        }
        nodedb_types::sync::shape::ShapeType::Graph { .. } => {
            super::shape::handler::ShapeSnapshotData::empty()
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
                return super::shape::handler::ShapeSnapshotData::empty();
            }
            shared
                .array_subscriber_cursors
                .register(session_id, array_name, coord_range.clone());
            info!(
                session = session_id,
                array = %array_name,
                "array shape subscribed; cursor initialized at HLC::ZERO"
            );
            super::shape::handler::ShapeSnapshotData::empty()
        }
        _ => {
            warn!(
                session = session_id,
                "shape subscribe: unknown shape_type variant, sending empty snapshot"
            );
            super::shape::handler::ShapeSnapshotData::empty()
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
pub(super) async fn handle_resync_request_async(
    shared: &SharedState,
    session: &super::session::SyncSession,
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

    let snapshot = super::shape::handler::ShapeSnapshotMsg {
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

/// Apply a CRDT delta on the Data Plane, converting the outcome into the final
/// client frame.
///
/// The in-memory session already produced a `DeltaAck`; this step performs the
/// actual durable apply (`CrdtOp::Apply`) and, if the Data Plane rejects it,
/// rewrites the ack into a `DeltaReject` carrying a typed [`CompensationHint`]
/// classified from the Data Plane's typed error code (never from a substring of
/// the message). On success it rebuilds the ack with the gate's `applied_seq`
/// and status.
pub(super) async fn validate_delta_constraints(
    shared: &SharedState,
    delta_msg: &DeltaPushMsg,
    ack_frame: SyncFrame,
    session_producer_id: u64,
    session_epoch: u64,
) -> Option<SyncFrame> {
    use crate::bridge::envelope::PhysicalPlan;
    use crate::types::TenantId;
    use nodedb_physical::physical_plan::CrdtOp;

    // Dispatch a CrdtApply plan to the Data Plane. If the CRDT engine
    // rejects it (constraint violation), we get an error back.
    // Uses EventSource::CrdtSync so triggers are NOT fired on replicated deltas.
    let tenant_id = TenantId::new(0); // Trust mode default tenant.

    // Quota enforcement — reject before dispatch.
    if let Err(e) = shared.check_tenant_quota(tenant_id) {
        warn!(error = %e, "sync: delta validation rejected by quota");
        let reject = DeltaRejectMsg {
            mutation_id: delta_msg.mutation_id,
            reason: e.to_string(),
            compensation: Some(CompensationHint::Custom {
                constraint: "quota".into(),
                detail: e.to_string(),
            }),
        };
        return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
    }

    let surrogate = match shared
        .surrogate_assigner
        .assign(&delta_msg.collection, delta_msg.document_id.as_bytes())
    {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, "sync: surrogate assignment failed");
            let reject = DeltaRejectMsg {
                mutation_id: delta_msg.mutation_id,
                reason: e.to_string(),
                compensation: Some(CompensationHint::Custom {
                    constraint: "surrogate".into(),
                    detail: e.to_string(),
                }),
            };
            return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
        }
    };

    // Server-authoritative provenance: producer_id + epoch come from the
    // session's handshake-assigned identity, never from the wire message — a
    // client cannot spoof another producer's id or replay a fenced epoch. Only
    // `seq` is client-owned (the per-producer monotonic counter the gate validates).
    let prov = SyncProvenance {
        producer_id: session_producer_id,
        epoch: session_epoch,
        stream_id: stream_id_for(EngineKind::Crdt, &delta_msg.collection),
        seq: delta_msg.seq,
    };

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: delta_msg.collection.clone(),
        document_id: delta_msg.document_id.clone(),
        delta: delta_msg.delta.clone(),
        peer_id: delta_msg.peer_id,
        mutation_id: delta_msg.mutation_id,
        surrogate,
        provenance: Some(prov),
    });

    shared.tenant_request_start(tenant_id);
    let dispatch_result = super::raft_dispatch::dispatch_sync_bytes(
        shared,
        tenant_id,
        &delta_msg.collection,
        plan,
        Duration::from_secs(10),
        crate::event::EventSource::CrdtSync,
    )
    .await;
    shared.tenant_request_end(tenant_id);

    match dispatch_result {
        Ok(payload) => {
            // Decode the SyncAckResult from the Data Plane response payload.
            // On success, rebuild the DeltaAck with the correct applied_seq and status.
            // The original ack_frame carries mutation_id and clock_skew_warning_ms which
            // we preserve; applied_seq and status come from the gate result.
            let gate_result = match zerompk::from_msgpack::<nodedb_types::sync::wire::SyncAckResult>(
                &payload,
            ) {
                Ok(r) => r,
                Err(err) => {
                    // Payload decode failed: fall back to the original ack frame so
                    // the client still gets an ack (the delta was applied).
                    warn!(
                        collection = %delta_msg.collection,
                        error = %err,
                        "sync: failed to decode SyncAckResult from Data Plane; using default ack"
                    );
                    return Some(ack_frame);
                }
            };

            // Extract mutation_id and clock_skew_warning_ms from the pre-built ack_frame
            // so we don't lose them when rebuilding.
            let (mutation_id, clock_skew_warning_ms) =
                if let Some(existing_ack) = ack_frame.decode_body::<super::wire::DeltaAckMsg>() {
                    (existing_ack.mutation_id, existing_ack.clock_skew_warning_ms)
                } else {
                    (delta_msg.mutation_id, None)
                };

            let ack = super::wire::DeltaAckMsg {
                mutation_id,
                lsn: 0, // WAL LSN is not surfaced by dispatch_async_with_source; left as 0.
                clock_skew_warning_ms,
                applied_seq: gate_result.applied_seq,
                status: gate_result.status,
            };
            SyncFrame::try_encode(SyncMessageType::DeltaAck, &ack)
        }
        Err(e) => {
            // The Data Plane rejected the apply. Classify by the *typed* error
            // (preserved across the bridge) — never by substring-matching the
            // human message — and rewrite the ack into a DeltaReject.
            let hint = compensation_hint_for_dispatch_error(&e);
            warn!(
                collection = %delta_msg.collection,
                doc = %delta_msg.document_id,
                hint = hint.code(),
                error = %e,
                "sync: delta rejected by Data Plane"
            );
            let reject = DeltaRejectMsg {
                mutation_id: delta_msg.mutation_id,
                reason: e.to_string(),
                compensation: Some(hint),
            };
            SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
        }
    }
}

/// Map a Data-Plane dispatch failure to a typed wire [`CompensationHint`].
///
/// Classification is by error **type**, never by substring-matching the message.
/// The error arrives either as a preserved Data-Plane [`ErrorCode`] (single-node
/// sync path) or as a typed [`crate::Error`] (Raft path / Control-Plane checks);
/// both are handled.
///
/// Each arm carries only what the typed error actually tells us. In particular
/// the precise [`CompensationHint::UniqueViolation`] /
/// [`CompensationHint::ForeignKeyMissing`] variants are intentionally **not**
/// fabricated here: they require the offending field and conflicting/referenced
/// value, which the flattened constraint error does not carry. Surfacing those
/// requires threading the structured violation produced by the CRDT validator
/// through the apply path; until then `Custom { constraint, detail }` is the
/// honest, machine-readable representation (it preserves the constraint name and
/// human detail without inventing values).
fn compensation_hint_for_dispatch_error(e: &crate::Error) -> CompensationHint {
    use crate::bridge::envelope::ErrorCode;

    match e {
        crate::Error::DataPlane(code) => match code {
            ErrorCode::RejectedConstraint { constraint, detail } => CompensationHint::Custom {
                constraint: constraint.clone(),
                detail: detail.clone(),
            },
            ErrorCode::RejectedPrevalidation { reason } => CompensationHint::Custom {
                constraint: "prevalidation".into(),
                detail: reason.clone(),
            },
            ErrorCode::RejectedAuthz => CompensationHint::PermissionDenied,
            ErrorCode::RateExceeded { retry_after_ms, .. } => CompensationHint::RateLimited {
                retry_after_ms: *retry_after_ms,
            },
            other => CompensationHint::Custom {
                constraint: "apply_failed".into(),
                detail: format!("{other:?}"),
            },
        },
        crate::Error::RejectedConstraint {
            constraint, detail, ..
        } => CompensationHint::Custom {
            constraint: constraint.clone(),
            detail: detail.clone(),
        },
        crate::Error::RejectedPrevalidation { constraint, reason } => CompensationHint::Custom {
            constraint: constraint.clone(),
            detail: reason.clone(),
        },
        crate::Error::RejectedAuthz { .. } => CompensationHint::PermissionDenied,
        crate::Error::RateExceeded { retry_after_ms, .. } => CompensationHint::RateLimited {
            retry_after_ms: *retry_after_ms,
        },
        other => CompensationHint::Custom {
            constraint: "apply_failed".into(),
            detail: other.to_string(),
        },
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
) -> super::shape::handler::ShapeSnapshotData {
    use crate::data::executor::response_codec::{
        decode_raw_scan_to_docs, encode_raw_document_rows,
    };
    use nodedb_query::metadata_filter::matches_metadata_filter;
    use nodedb_types::filter::MetadataFilter;

    if predicate_bytes.is_empty() {
        let doc_count = decode_raw_scan_to_docs(&payload).len();
        return super::shape::handler::ShapeSnapshotData {
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
            return super::shape::handler::ShapeSnapshotData::empty();
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
        Ok(data) => super::shape::handler::ShapeSnapshotData { data, doc_count },
        Err(err) => {
            // Fail closed: a re-encode failure must not ship a header whose
            // doc_count disagrees with its (empty) body. Drop the snapshot,
            // matching the predicate-decode failure path above.
            warn!(
                shape_id,
                error = %err,
                "shape snapshot: failed to encode filtered rows; sending empty snapshot"
            );
            super::shape::handler::ShapeSnapshotData::empty()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compensation_hint_for_dispatch_error;
    use crate::bridge::envelope::ErrorCode;
    use crate::types::TenantId;
    use nodedb_types::sync::compensation::CompensationHint;

    #[test]
    fn preserved_data_plane_constraint_maps_to_custom_with_real_name() {
        // A Data-Plane RejectedConstraint carries the constraint name + detail,
        // but not the offending field/value — so the honest hint is Custom with
        // the real name, never a fabricated UniqueViolation.
        let e = crate::Error::DataPlane(ErrorCode::RejectedConstraint {
            constraint: "users_email_unique".into(),
            detail: "value 'a@b.com' already exists".into(),
        });
        match compensation_hint_for_dispatch_error(&e) {
            CompensationHint::Custom { constraint, detail } => {
                assert_eq!(constraint, "users_email_unique");
                assert!(detail.contains("a@b.com"));
            }
            other => panic!("expected Custom, got {other:?}"),
        }
    }

    #[test]
    fn data_plane_authz_maps_to_permission_denied() {
        let e = crate::Error::DataPlane(ErrorCode::RejectedAuthz);
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::PermissionDenied
        );
    }

    #[test]
    fn data_plane_rate_exceeded_preserves_retry_after() {
        let e = crate::Error::DataPlane(ErrorCode::RateExceeded {
            gate: "writes".into(),
            retry_after_ms: 1500,
        });
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::RateLimited {
                retry_after_ms: 1500
            }
        );
    }

    #[test]
    fn import_failure_maps_to_apply_failed_not_fabricated_constraint() {
        // The realistic CRDT-apply failure is a Loro import error, surfaced as
        // ErrorCode::Internal. It must NOT be guessed into a UNIQUE/FK hint.
        let e = crate::Error::DataPlane(ErrorCode::Internal {
            detail: "loro import failed".into(),
        });
        match compensation_hint_for_dispatch_error(&e) {
            CompensationHint::Custom { constraint, .. } => assert_eq!(constraint, "apply_failed"),
            other => panic!("expected Custom apply_failed, got {other:?}"),
        }
    }

    #[test]
    fn typed_authz_error_also_maps_to_permission_denied() {
        // Errors that arrive already typed (e.g. via the Raft path) classify too.
        let e = crate::Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: "users".into(),
        };
        assert_eq!(
            compensation_hint_for_dispatch_error(&e),
            CompensationHint::PermissionDenied
        );
    }
}
