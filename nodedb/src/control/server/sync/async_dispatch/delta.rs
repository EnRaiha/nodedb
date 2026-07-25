// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta apply dispatch and dispatch-failure compensation classification.

use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use nodedb_types::sync::wire::{EngineKind, SyncProvenance, stream_id_for};

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::super::wire::{
    CompensationHint, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};

/// Apply a CRDT delta on the Data Plane, converting the outcome into the final
/// client frame.
///
/// The in-memory session already produced a `DeltaAck`; this step performs the
/// actual durable apply (`CrdtOp::Apply`) and finalizes the client frame.
///
/// A delta can be refused in two structurally different ways:
///
/// 1. **Applied-then-rejected by the validator.** The delta is Raft-committed and
///    imported on every replica (a committed CRDT delta cannot be un-imported),
///    but the post-import constraint check (UNIQUE / NOT NULL) found a violation.
///    The Data Plane surfaces this as a structured [`ViolationType`] in
///    `SyncAckResult.reject`; we map it precisely to a typed
///    [`CompensationHint`] via [`ViolationType::to_compensation_hint`] — this is
///    the only path that can name the offending field and conflicting value.
/// 2. **Never applied (dispatch failure).** Quota, surrogate assignment, timeout,
///    or transport error — the apply never reached (or never left) the Data
///    Plane. These carry only a typed error code, classified by
///    [`compensation_hint_for_dispatch_error`] (never by substring-matching).
///
/// On a clean apply it rebuilds the ack with the gate's `applied_seq` and status.
pub(crate) async fn apply_delta_and_finalize(
    shared: &SharedState,
    delta_msg: &DeltaPushMsg,
    ack_frame: SyncFrame,
    identity: Option<&AuthenticatedIdentity>,
    session_producer_id: u64,
    session_epoch: u64,
) -> Option<SyncFrame> {
    use crate::bridge::envelope::PhysicalPlan;
    use nodedb_physical::physical_plan::CrdtOp;

    // Authorize before quota, surrogate allocation, catalog lookup, plan
    // construction, or dispatch. The tenant is derived only from the
    // handshake-bound identity; an absent identity fails closed without a
    // fallback tenant or reconstructed principal.
    let tenant_id = match authorize_delta_write(shared, identity, &delta_msg.collection) {
        Ok(tenant_id) => tenant_id,
        Err(_) => return permission_denied_delta_reject(delta_msg),
    };
    let identity = match identity {
        Some(identity) => identity,
        None => return permission_denied_delta_reject(delta_msg),
    };
    let audit = ArcAuditEmitter(std::sync::Arc::clone(&shared.audit));
    let policy = crate::control::crdt_post_image_policy::ExternalCrdtPostImagePolicy::from_identity(
        tenant_id,
        DatabaseId::DEFAULT,
        &delta_msg.collection,
        identity,
        "sync".into(),
        &shared.rls,
        &audit,
    );

    // Dispatch a CrdtApply plan to the Data Plane. If the CRDT engine
    // rejects it (constraint violation), we get an error back.
    // Uses EventSource::CrdtSync so triggers are NOT fired on replicated deltas.

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

    let surrogate = match shared.surrogate_assigner.assign(
        DatabaseId::DEFAULT,
        tenant_id,
        &delta_msg.collection,
        delta_msg.document_id.as_bytes(),
    ) {
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

    // Stamp the constraint-set version this delta is admitted against. The
    // apply-time write-gate compares it on every replica against the
    // constraint version that replica has installed, rejecting a delta that
    // outran its `SetConstraints` (the reconcile loop installs constraints
    // asynchronously, so a create-race delta can commit first). Read the raw
    // `constraint_version` field from the catalog under the session tenant —
    // the exact value the constraint reconcile loop
    // (`bootstrap::constraint_reconcile`) replicates via `ConstraintChange`
    // and every replica installs into its validator. Admission and install
    // MUST use identical normalization or the gate mis-fences, so this takes
    // the value verbatim — no `.max(1)` (the install side applies none).
    // `constraint_version` bumps only when the derived constraint set
    // actually changes; a collection with no constraints stamps `0` (no
    // fence, gate open). Missing collection ⇒ `0` (gate open; safe).
    let constraint_version_required = shared
        .credentials
        .catalog()
        .get_collection(
            DatabaseId::DEFAULT,
            tenant_id.as_u64(),
            &delta_msg.collection,
        )
        .ok()
        .flatten()
        .map(|col| col.constraint_version)
        .unwrap_or(0);

    let plan = PhysicalPlan::Crdt(CrdtOp::Apply {
        collection: delta_msg.collection.clone(),
        document_id: delta_msg.document_id.clone(),
        delta: delta_msg.delta.clone(),
        peer_id: delta_msg.peer_id,
        mutation_id: delta_msg.mutation_id,
        surrogate,
        provenance: Some(prov),
        constraint_version_required,
        expected_frontier_digest: None,
    });

    let vshard_id = crate::types::VShardId::from_collection_in_database(
        DatabaseId::DEFAULT,
        &delta_msg.collection,
    );
    let authorized = super::super::raft_dispatch::authorize_sync_task(
        shared,
        Some(identity),
        tenant_id,
        DatabaseId::DEFAULT,
        vshard_id,
        plan,
    );
    shared.tenant_request_start(tenant_id);
    let dispatch_result = match authorized {
        Ok(authorized) => {
            super::super::raft_dispatch::dispatch_sync_bytes(
                shared,
                &delta_msg.collection,
                authorized,
                Duration::from_secs(10),
                crate::event::EventSource::CrdtSync,
                &policy,
            )
            .await
        }
        Err(error) => Err(error),
    };
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

            // Applied-then-rejected: the delta committed and imported, but the
            // post-import validator flagged a constraint violation and enqueued a
            // DLQ compensation. Rewrite the ack into a DeltaReject carrying the
            // precise, structured hint (field + conflicting value) — this is the
            // only place that information is available.
            if let Some(violation) = gate_result.reject {
                let hint = violation.to_compensation_hint();
                warn!(
                    collection = %delta_msg.collection,
                    doc = %delta_msg.document_id,
                    violation = %violation,
                    "sync: delta applied but rejected by CRDT validator"
                );
                let reject = DeltaRejectMsg {
                    mutation_id: delta_msg.mutation_id,
                    reason: violation.to_string(),
                    compensation: Some(hint),
                };
                return SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject);
            }

            // Extract mutation_id and clock_skew_warning_ms from the pre-built ack_frame
            // so we don't lose them when rebuilding.
            let (mutation_id, clock_skew_warning_ms) = if let Some(existing_ack) =
                ack_frame.decode_body::<super::super::wire::DeltaAckMsg>()
            {
                (existing_ack.mutation_id, existing_ack.clock_skew_warning_ms)
            } else {
                (delta_msg.mutation_id, None)
            };

            let ack = super::super::wire::DeltaAckMsg {
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

/// Fail closed unless the handshake-bound identity has write access to the
/// target collection. This is used at both the session boundary (before its
/// bookkeeping and validation side effects) and again immediately before plan
/// construction, so a permission revocation between those points cannot reach
/// the Data Plane.
pub(in crate::control::server::sync) fn authorize_delta_write(
    shared: &SharedState,
    identity: Option<&AuthenticatedIdentity>,
    collection: &str,
) -> Result<TenantId, DeltaAuthorizationFailure> {
    let audit = ArcAuditEmitter(Arc::clone(&shared.audit));
    authorize_delta_write_with(
        identity,
        collection,
        &shared.permissions,
        &shared.roles,
        &audit,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::control::server::sync) enum DeltaAuthorizationFailure {
    IdentityNotEstablished,
    PermissionDenied,
}

fn authorize_delta_write_with(
    identity: Option<&AuthenticatedIdentity>,
    collection: &str,
    permissions: &crate::control::security::permission::PermissionStore,
    roles: &crate::control::security::role::RoleStore,
    audit: &dyn crate::control::security::audit::AuditEmitter,
) -> Result<TenantId, DeltaAuthorizationFailure> {
    let identity = identity.ok_or(DeltaAuthorizationFailure::IdentityNotEstablished)?;
    authorize_collection(
        identity,
        DatabaseId::DEFAULT,
        collection,
        Permission::Write,
        permissions,
        roles,
        audit,
    )
    .map_err(|_| DeltaAuthorizationFailure::PermissionDenied)?;
    Ok(identity.tenant_id)
}

pub(in crate::control::server::sync) fn permission_denied_delta_reject(
    delta_msg: &DeltaPushMsg,
) -> Option<SyncFrame> {
    let reject = DeltaRejectMsg {
        mutation_id: delta_msg.mutation_id,
        reason: "permission denied".into(),
        compensation: Some(CompensationHint::PermissionDenied),
    };
    SyncFrame::try_encode(SyncMessageType::DeltaReject, &reject)
}

/// Map a Data-Plane dispatch failure to a typed wire [`CompensationHint`].
///
/// Classification is by error **type**, never by substring-matching the message.
/// The error arrives either as a preserved Data-Plane [`ErrorCode`] (single-node
/// sync path) or as a typed [`crate::Error`] (Raft path / Control-Plane checks);
/// both are handled.
///
/// This path handles only **dispatch failures** — the apply never completed
/// (quota, surrogate assignment, timeout, transport). Constraint violations from
/// a *successful* apply do NOT arrive here: they come back as a structured
/// [`ViolationType`] in `SyncAckResult.reject` and are mapped by the caller via
/// [`ViolationType::to_compensation_hint`], which is the only path with the
/// offending field and conflicting value. Accordingly, the precise
/// [`CompensationHint::UniqueViolation`] / [`CompensationHint::ForeignKeyMissing`]
/// variants are intentionally **not** fabricated here — a flattened dispatch
/// error does not carry those values — so `Custom { constraint, detail }` is the
/// honest, machine-readable representation.
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

#[cfg(test)]
mod tests {
    use super::{
        DeltaAuthorizationFailure, authorize_delta_write_with,
        compensation_hint_for_dispatch_error, permission_denied_delta_reject,
    };
    use crate::bridge::envelope::ErrorCode;
    use crate::control::security::audit::NoopAuditEmitter;
    use crate::control::security::audit::emitter::test_helpers::CapturingEmitter;
    use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Permission};
    use crate::control::security::permission::PermissionStore;
    use crate::control::security::role::RoleStore;
    use crate::types::TenantId;
    use nodedb_types::sync::compensation::CompensationHint;
    use nodedb_types::sync::wire::{DeltaPushMsg, DeltaRejectMsg, SyncMessageType};

    fn identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity {
            user_id: 7,
            username: "writer".into(),
            tenant_id: TenantId::new(9),
            auth_method: AuthMethod::ApiKey,
            roles: Vec::new(),
            is_superuser: false,
            default_database: None,
            accessible_databases: AuthenticatedIdentity::default_database_set(false),
        }
    }

    fn delta() -> DeltaPushMsg {
        DeltaPushMsg {
            collection: "orders".into(),
            document_id: "order-1".into(),
            delta: Vec::new(),
            peer_id: 1,
            mutation_id: 42,
            checksum: 0,
            device_valid_time_ms: None,
            producer_id: 0,
            epoch: 0,
            seq: 0,
        }
    }

    #[test]
    fn ungranted_delta_is_denied_and_audited_once() {
        let identity = identity();
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        let result =
            authorize_delta_write_with(Some(&identity), "orders", &permissions, &roles, &audit);

        assert_eq!(result, Err(DeltaAuthorizationFailure::PermissionDenied));
        assert_eq!(audit.recorded().len(), 1);
        let frame =
            permission_denied_delta_reject(&delta()).expect("permission rejection must encode");
        assert_eq!(frame.msg_type, SyncMessageType::DeltaReject);
        let reject: DeltaRejectMsg = frame
            .decode_body()
            .expect("permission rejection must decode");
        assert_eq!(
            reject.compensation,
            Some(CompensationHint::PermissionDenied)
        );
    }

    #[test]
    fn missing_identity_fails_closed_without_audit_principal() {
        let permissions = PermissionStore::new();
        let roles = RoleStore::new();
        let audit = CapturingEmitter::new();

        assert_eq!(
            authorize_delta_write_with(None, "orders", &permissions, &roles, &audit),
            Err(DeltaAuthorizationFailure::IdentityNotEstablished)
        );
        assert!(audit.recorded().is_empty());
    }

    #[test]
    fn write_granted_delta_uses_handshake_identity_tenant() {
        let identity = identity();
        let permissions = PermissionStore::new();
        permissions
            .grant(
                "collection:9:orders",
                "user:writer",
                Permission::Write,
                "admin",
                None,
            )
            .expect("in-memory grant must succeed");
        let roles = RoleStore::new();
        let tenant_id = authorize_delta_write_with(
            Some(&identity),
            "orders",
            &permissions,
            &roles,
            &NoopAuditEmitter,
        )
        .expect("write grant must authorize dispatch");

        assert_eq!(tenant_id, TenantId::new(9));
    }

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
