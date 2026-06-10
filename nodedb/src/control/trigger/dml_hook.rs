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

use crate::control::state::SharedState;
use crate::types::{TenantId, TraceId};
use nodedb_physical::physical_plan::DocumentOp;

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
        | DocumentOp::MaterializeScan { .. } => None,
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
/// Issues a PointGet to the Data Plane and deserializes the response.
/// Returns an empty map if the document doesn't exist or can't be read.
pub async fn fetch_old_row(
    state: &SharedState,
    tenant_id: TenantId,
    collection: &str,
    document_id: &str,
) -> HashMap<String, nodedb_types::Value> {
    let pk_bytes = document_id.as_bytes().to_vec();
    let surrogate = state
        .surrogate_assigner
        .lookup(collection, &pk_bytes)
        .ok()
        .flatten()
        .unwrap_or(nodedb_types::Surrogate::ZERO);
    let plan = crate::bridge::envelope::PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes,
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let vshard_id = crate::types::VShardId::from_key(document_id.as_bytes());

    let resp = match crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        vshard_id,
        plan,
        TraceId::ZERO,
    )
    .await
    {
        Ok(r) => r,
        Err(_) => return HashMap::new(),
    };

    if resp.payload.is_empty() {
        return HashMap::new();
    }

    // Decode the response payload (MessagePack or JSON).
    let bytes = resp.payload.as_ref();
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(bytes) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return map
            .into_iter()
            .map(|(k, v)| (k, nodedb_types::Value::from(v)))
            .collect();
    }

    HashMap::new()
}

/// Check if any triggers exist for this collection+event combination.
///
/// Quick check to avoid fetch_old_row and other overhead when no triggers are defined.
pub fn has_triggers(state: &SharedState, tenant_id: TenantId, collection: &str) -> bool {
    let tid = tenant_id.as_u64();
    !state
        .trigger_registry
        .get_matching(tid, collection, DmlEvent::Insert)
        .is_empty()
        || !state
            .trigger_registry
            .get_matching(tid, collection, DmlEvent::Update)
            .is_empty()
        || !state
            .trigger_registry
            .get_matching(tid, collection, DmlEvent::Delete)
            .is_empty()
}
