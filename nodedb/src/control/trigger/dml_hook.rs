// SPDX-License-Identifier: BUSL-1.1

//! DML trigger hook: intercepts write dispatches to fire BEFORE/AFTER/INSTEAD OF triggers.
//!
//! Sits between the Control Plane query router and the Data Plane dispatch.
//! For each DML write task:
//! 1. Classify the operation (INSERT/UPDATE/DELETE) and extract collection + doc ID
//! 2. Fetch OLD row data for UPDATE/DELETE (needed for OLD.* bindings)
//! 3. Fire INSTEAD OF triggers — if handled, skip normal dispatch
//! 4. Fire BEFORE triggers — may abort the DML via RAISE EXCEPTION
//! 5. Dispatch to Data Plane (normal write path)
//! 6. Fire SYNC AFTER triggers (same logical transaction)
//!
//! ASYNC AFTER triggers are handled by the Event Plane via WriteEvents — not here.

use std::collections::HashMap;

use sonic_rs;

use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::{authorize_collection, authorize_task_set};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::registry::DmlEvent;

/// Classification of a DML write for trigger purposes.
#[derive(Debug)]
pub struct DmlWriteInfo {
    /// Collection name targeted by this write.
    pub collection: String,
    /// Document ID (for point operations). None for bulk operations.
    pub document_id: Option<String>,
    /// DML event type.
    ///
    /// For UPSERT the initial value is a best guess — the true event is
    /// not known until the routing layer probes the pre-write row via
    /// `fetch_old_row`. When `needs_existence_probe` is set, routing
    /// overrides this field based on probe results before firing
    /// post-dispatch triggers.
    pub event: DmlEvent,
    /// NEW row fields extracted from the write plan. None for DELETE.
    pub new_fields: Option<HashMap<String, nodedb_types::Value>>,
    /// True when the operation's real event type depends on whether the
    /// target row already exists (currently: UPSERT / INSERT ... ON
    /// CONFLICT). Routing uses this flag to force a pre-dispatch
    /// existence probe so the correct AFTER INSERT vs AFTER UPDATE
    /// triggers fire — otherwise an UPSERT onto an existing row would
    /// silently fire AFTER INSERT, which is the wrong trigger class.
    pub needs_existence_probe: bool,
}

/// Attempt to classify a PhysicalPlan as a document DML write.
///
/// Returns `None` for non-write operations (reads, DDL, scans, etc.)
/// and for non-document engines (vector, graph, etc. — those emit WriteEvents
/// for ASYNC triggers but don't participate in the BEFORE/SYNC AFTER path).
pub fn classify_dml_write(plan: &crate::bridge::envelope::PhysicalPlan) -> Option<DmlWriteInfo> {
    match plan {
        crate::bridge::envelope::PhysicalPlan::Document(doc_op) => classify_document_op(doc_op),
        // KV, Vector, Graph, etc. writes emit WriteEvents for ASYNC triggers
        // but don't participate in BEFORE/SYNC AFTER trigger hooks.
        // Those engines handle triggers via Event Plane only.
        _ => None,
    }
}

fn classify_document_op(op: &DocumentOp) -> Option<DmlWriteInfo> {
    match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            ..
        }
        | DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            ..
        } => {
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.clone(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert,
                new_fields: Some(new_fields),
                needs_existence_probe: false,
            })
        }
        DocumentOp::Upsert {
            collection,
            document_id,
            value,
            ..
        } => {
            // UPSERT's event type depends on whether the primary key
            // already exists — routing must probe before firing
            // post-dispatch SYNC triggers. `event` starts at Insert as a
            // harmless default; the probe result overrides it.
            let new_fields = deserialize_value_to_fields(value);
            Some(DmlWriteInfo {
                collection: collection.clone(),
                document_id: Some(document_id.clone()),
                event: DmlEvent::Insert,
                new_fields: Some(new_fields),
                needs_existence_probe: true,
            })
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: Some(document_id.clone()),
            event: DmlEvent::Update,
            new_fields: None, // NEW fields computed after applying updates to OLD
            needs_existence_probe: false,
        }),
        DocumentOp::BatchInsert { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None, // Batch — individual rows not available here
            needs_existence_probe: false,
        }),
        DocumentOp::BulkUpdate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::BulkDelete { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::Truncate { collection, .. } => Some(DmlWriteInfo {
            collection: collection.clone(),
            document_id: None,
            event: DmlEvent::Delete,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.clone(),
            document_id: None,
            event: DmlEvent::Insert,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::UpdateFromJoin {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.clone(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        DocumentOp::Merge {
            target_collection, ..
        } => Some(DmlWriteInfo {
            collection: target_collection.clone(),
            document_id: None,
            event: DmlEvent::Update,
            new_fields: None,
            needs_existence_probe: false,
        }),
        // Not a write operation.
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // A derived balance write carries no user DML intent: the statement
        // that caused it already fired its own triggers on the source row.
        | DocumentOp::ApplyBalanceDelta { .. } => None,
    }
}

/// Deserialize a MessagePack/JSON value blob into a HashMap for trigger bindings.
fn deserialize_value_to_fields(value: &[u8]) -> HashMap<String, nodedb_types::Value> {
    // Try MessagePack first (primary format), fall back to JSON.
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(value) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(value) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    HashMap::new()
}

/// Patch a `PhysicalTask` with mutated fields from a BEFORE trigger.
///
/// Serializes the mutated fields to MessagePack and replaces the value
/// payload in the underlying `PointPut` or `Upsert` operation.
/// For `PointUpdate`, the updates are re-derived from the mutated fields.
pub fn patch_task_with_mutated_fields(
    task: &mut nodedb_physical::physical_task::PhysicalTask,
    mutated: &HashMap<String, nodedb_types::Value>,
) {
    use crate::bridge::envelope::PhysicalPlan;

    let json_obj: serde_json::Map<String, serde_json::Value> = mutated
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::from(v.clone())))
        .collect();
    let json_val = serde_json::Value::Object(json_obj);
    let new_bytes = match nodedb_types::value_to_msgpack(&nodedb_types::Value::from(json_val)) {
        Ok(b) => b,
        Err(_) => return,
    };

    match &mut task.plan {
        PhysicalPlan::Document(DocumentOp::PointPut { value, .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { value, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { value, .. }) => {
            *value = new_bytes;
        }
        PhysicalPlan::Document(DocumentOp::PointUpdate { updates, .. }) => {
            // Re-derive field-level updates from the full mutated row. Trigger
            // mutations are fully-evaluated post-trigger values, so they ship
            // as `UpdateValue::Literal`.
            *updates = mutated
                .iter()
                .filter_map(|(k, v)| {
                    nodedb_types::value_to_msgpack(v).ok().map(|b| {
                        (
                            k.clone(),
                            nodedb_physical::physical_plan::UpdateValue::Literal(b),
                        )
                    })
                })
                .collect();
        }
        _ => {}
    }
}

/// Fetch the current document as a field map (for OLD row bindings).
///
/// This is a user-derived read, even when it is performed as part of a write
/// hook. It therefore authorizes `READ` before touching surrogate/catalog state,
/// injects the session's RLS predicate, and dispatches only an exact authorized
/// task. An empty map means the surrogate or row is genuinely absent; all other
/// failures propagate so callers cannot misclassify them as an absent OLD row.
pub async fn fetch_old_row(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    auth: &AuthContext,
    collection: &str,
    document_id: &str,
) -> crate::Result<HashMap<String, nodedb_types::Value>> {
    let tenant_id = identity.tenant_id;
    if auth.tenant_id != tenant_id || auth.database_id != Some(database_id) {
        return Err(crate::Error::RejectedAuthz {
            tenant_id,
            resource: "OLD-row fetch auth context is not aligned to the selected database"
                .to_owned(),
        });
    }

    let audit = ArcAuditEmitter(std::sync::Arc::clone(&state.audit));
    authorize_collection(
        identity,
        database_id,
        collection,
        Permission::Read,
        &state.permissions,
        &state.roles,
        &audit,
    )?;

    let pk_bytes = document_id.as_bytes().to_vec();
    let Some(surrogate) =
        state
            .surrogate_assigner
            .lookup(database_id, tenant_id, collection, &pk_bytes)?
    else {
        return Ok(HashMap::new());
    };
    let mut plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes,
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &state.rls,
        auth,
    )?;
    crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        auth,
        &state.redaction,
    )?;

    let task = PhysicalTask {
        tenant_id,
        database_id,
        vshard_id: VShardId::from_key(document_id.as_bytes()),
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };
    let authorized = authorize_task_set(
        identity,
        std::slice::from_ref(&task),
        &state.permissions,
        &state.roles,
        &audit,
    )?
    .into_tasks()
    .into_iter()
    .next()
    .ok_or_else(|| crate::Error::Internal {
        detail: "authorization returned no task capability for OLD-row fetch".into(),
    })?;
    let resp = crate::control::server::dispatch_utils::dispatch_authorized_to_data_plane(
        state,
        authorized,
        TraceId::ZERO,
    )
    .await?;

    if resp.payload.is_empty() {
        return Ok(HashMap::new());
    }

    // Decode the response payload (MessagePack or JSON). A non-object payload
    // is a transport/protocol failure, not evidence that the row is absent.
    let bytes = resp.payload.as_ref();
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(bytes) {
        return Ok(map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect());
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return Ok(map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect());
    }

    Err(crate::Error::PlanError {
        detail: format!("invalid OLD-row response payload for collection '{collection}'"),
    })
}

/// Check if any triggers exist for this collection+event combination.
///
/// Quick check to avoid fetch_old_row and other overhead when no triggers are defined.
pub fn has_triggers(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> bool {
    let tid = tenant_id.as_u64();
    !state
        .trigger_registry
        .get_matching(database_id, tid, collection, DmlEvent::Insert)
        .is_empty()
        || !state
            .trigger_registry
            .get_matching(database_id, tid, collection, DmlEvent::Update)
            .is_empty()
        || !state
            .trigger_registry
            .get_matching(database_id, tid, collection, DmlEvent::Delete)
            .is_empty()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::auth_context::AuthContext;
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::state::SharedState;
    use crate::types::{DatabaseId, TenantId};
    use crate::wal::WalManager;

    use super::fetch_old_row;

    fn regular_identity(database_id: DatabaseId) -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            42,
            "trigger-reader",
            TenantId::new(7),
            AuthMethod::Trust,
            vec![Role::Custom("trigger_observer".into())],
            Some(database_id),
            DatabaseSet::Some(smallvec::smallvec![database_id]),
        )
    }

    #[tokio::test]
    async fn fetch_old_row_denies_unreadable_collection_before_lookup_or_dispatch() {
        let directory = tempfile::tempdir().expect("create trigger hook test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("trigger-hook.wal"))
                .expect("open trigger hook test WAL"),
        );
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 1);
        let state = SharedState::new(dispatcher, wal).expect("construct trigger hook state");
        let database_id = DatabaseId::new(17);
        let identity = regular_identity(database_id);
        let mut auth = AuthContext::from_identity(&identity, "trigger-hook-session".into());
        auth.database_id = Some(database_id);
        let collection = "orders";
        let document_id = "order-42";
        let initial_hwm = state
            .surrogate_registry
            .read()
            .expect("read surrogate registry")
            .current_hwm();

        let error = fetch_old_row(
            &state,
            &identity,
            database_id,
            &auth,
            collection,
            document_id,
        )
        .await
        .expect_err("custom role without READ grant must not fetch OLD row");

        assert!(matches!(
            error,
            crate::Error::RejectedAuthz { tenant_id, resource }
                if tenant_id == TenantId::new(7)
                    && resource == "permission denied: user 'trigger-reader' lacks Read permission on 'orders'"
        ));
        assert_eq!(
            state
                .surrogate_registry
                .read()
                .expect("read surrogate registry")
                .current_hwm(),
            initial_hwm,
            "authorization denial must not allocate a surrogate"
        );
        assert_eq!(
            state
                .surrogate_assigner
                .lookup(
                    database_id,
                    identity.tenant_id,
                    collection,
                    document_id.as_bytes()
                )
                .expect("inspect surrogate binding after denial"),
            None,
            "authorization denial must not create a surrogate binding"
        );
        assert!(
            data_sides.remove(0).request_rx.try_pop().is_err(),
            "authorization denial must not dispatch an OLD-row read"
        );
    }

    #[tokio::test]
    async fn fetch_old_row_rejects_misaligned_auth_context_before_authorization() {
        let directory = tempfile::tempdir().expect("create trigger hook test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("trigger-hook.wal"))
                .expect("open trigger hook test WAL"),
        );
        let (dispatcher, mut data_sides) = Dispatcher::new(1, 1);
        let state = SharedState::new(dispatcher, wal).expect("construct trigger hook state");
        let database_id = DatabaseId::new(17);
        let identity = regular_identity(database_id);
        let mut auth = AuthContext::from_identity(&identity, "trigger-hook-session".into());
        auth.database_id = Some(DatabaseId::new(18));
        let initial_hwm = state
            .surrogate_registry
            .read()
            .expect("read surrogate registry")
            .current_hwm();

        let error = fetch_old_row(&state, &identity, database_id, &auth, "orders", "order-42")
            .await
            .expect_err("misaligned auth context must be rejected before authorization");

        assert!(matches!(
            error,
            crate::Error::RejectedAuthz { tenant_id, resource }
                if tenant_id == TenantId::new(7)
                    && resource == "OLD-row fetch auth context is not aligned to the selected database"
        ));
        assert_eq!(
            state
                .surrogate_registry
                .read()
                .expect("read surrogate registry")
                .current_hwm(),
            initial_hwm,
            "misaligned context must not allocate a surrogate"
        );
        assert!(
            state.audit.lock().expect("read audit log").is_empty(),
            "context rejection must occur before collection authorization"
        );
        assert!(
            data_sides.remove(0).request_rx.try_pop().is_err(),
            "context rejection must not dispatch an OLD-row read"
        );
    }
}
