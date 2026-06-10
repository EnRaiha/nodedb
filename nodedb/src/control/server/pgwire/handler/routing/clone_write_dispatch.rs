// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW write-path interception for the pgwire handler.
//!
//! Hooked into `dispatch_task_loop` before the normal "dispatch_task" call.
//! For `PointUpdate` and `PointDelete` targeting a `Shadowed` or `Materializing`
//! clone, applies the copy-up / tombstone protocol so the source database is
//! never modified.
//!
//! Non-cloned collections and `Materialized` clones return `None` — zero overhead.

use std::sync::Arc;
use std::time::Duration;

use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use nodedb_types::{CloneStatus, DatabaseId, Lsn, Surrogate, TenantId};

use crate::bridge::envelope::{Priority, Request, Response, Status};
use crate::control::clone::copyup::{
    CopyUpParams, KvCopyUpParams, perform_clone_copyup, perform_kv_clone_copyup,
};
use crate::control::clone::tombstone::{
    KvTombstoneParams, TombstoneParams, perform_clone_tombstone, perform_kv_clone_tombstone,
};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, KvOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::core::NodeDbPgHandler;

/// Outcome of write-path clone interception.
pub(super) enum CloneWriteOutcome {
    /// No interception needed — caller must dispatch normally.
    Passthrough,
    /// The write was fully handled by the clone path. Caller uses this response.
    Handled(Response),
}

impl NodeDbPgHandler {
    /// Intercept a single write task for a cloned collection.
    ///
    /// Must be called for every `PointUpdate` and `PointDelete` task before
    /// normal dispatch. Returns `Passthrough` when the collection is not a
    /// Shadowed/Materializing clone (zero overhead for non-clone paths).
    pub(super) async fn maybe_intercept_clone_write(
        &self,
        task: &PhysicalTask,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate { .. })
            | PhysicalPlan::Document(DocumentOp::PointDelete { .. }) => {
                self.intercept_doc_clone_write(task, tenant_id).await
            }
            PhysicalPlan::Kv(KvOp::FieldSet { .. }) | PhysicalPlan::Kv(KvOp::Delete { .. }) => {
                self.intercept_kv_clone_write(task, tenant_id).await
            }
            _ => Ok(CloneWriteOutcome::Passthrough),
        }
    }

    /// Handle Document CoW write interception (PointUpdate / PointDelete).
    async fn intercept_doc_clone_write(
        &self,
        task: &PhysicalTask,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let (collection_qualified, document_id, surrogate, is_delete) = match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointUpdate {
                collection,
                document_id,
                surrogate,
                ..
            }) => (collection.as_str(), document_id.as_str(), *surrogate, false),
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection,
                document_id,
                surrogate,
                ..
            }) => (collection.as_str(), document_id.as_str(), *surrogate, true),
            _ => return Ok(CloneWriteOutcome::Passthrough),
        };

        let catalog_arc = self.state.credentials.catalog();
        let Some(catalog) = catalog_arc.as_ref() else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, collection_qualified);

        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone write: get_collection: {e}")))?;
        let Some(desc) = desc else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let Some(ref origin) = desc.cloned_from else {
            return Ok(CloneWriteOutcome::Passthrough);
        };
        match desc.clone_status {
            CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        let row_in_target = probe_row_in_target(
            &self.state,
            tenant_id,
            db_id,
            collection_qualified,
            document_id,
            surrogate,
        )
        .await
        .map_err(|e| write_err(&format!("clone write probe: {e}")))?;

        if row_in_target {
            return Ok(CloneWriteOutcome::Passthrough);
        }

        if is_delete {
            perform_clone_tombstone(TombstoneParams {
                state: &self.state,
                target_db_id: db_id,
                target_collection: coll_name,
                source_surrogate: surrogate,
            })
            .map_err(|e| write_err(&format!("clone tombstone: {e}")))?;

            let synthetic_resp = synthetic_ok_response(self.next_request_id(), Lsn::new(0));
            return Ok(CloneWriteOutcome::Handled(synthetic_resp));
        }

        let source_db_id = origin.source_database;
        let source_coll = origin.source_collection.as_str();
        let source_coll_qualified =
            crate::control::planner::sql_plan_convert::convert::db_qualified(
                source_db_id,
                source_coll,
            );

        let source_row_bytes = fetch_source_row(
            &self.state,
            tenant_id,
            source_db_id,
            &source_coll_qualified,
            document_id,
            surrogate,
        )
        .await
        .map_err(|e| write_err(&format!("clone write fetch source: {e}")))?;

        let Some(source_row_bytes) = source_row_bytes else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        perform_clone_copyup(CopyUpParams {
            state: &Arc::clone(&self.state),
            tenant_id,
            target_db_id: db_id,
            target_collection: coll_name,
            origin,
            source_surrogate: surrogate,
            source_doc_id: document_id.to_string(),
            source_row_bytes,
        })
        .await
        .map_err(|e| write_err(&format!("clone copyup: {e}")))?;

        Ok(CloneWriteOutcome::Passthrough)
    }

    /// Handle KV CoW write interception (FieldSet / Delete).
    async fn intercept_kv_clone_write(
        &self,
        task: &PhysicalTask,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let (collection_qualified, kv_key, is_delete) = match &task.plan {
            PhysicalPlan::Kv(KvOp::FieldSet {
                collection, key, ..
            }) => (collection.as_str(), key.clone(), false),
            PhysicalPlan::Kv(KvOp::Delete { collection, keys }) => {
                // Delete may have multiple keys; handle each. We serialize here
                // (one tombstone per key) and return Handled with synthetic OK.
                let collection_qualified = collection.as_str();
                let db_id = task.database_id;
                let coll_name = strip_db_prefix(db_id, collection_qualified);

                let catalog_arc = self.state.credentials.catalog();
                let Some(catalog) = catalog_arc.as_ref() else {
                    return Ok(CloneWriteOutcome::Passthrough);
                };

                let desc = catalog
                    .get_collection(db_id, tenant_id.as_u64(), coll_name)
                    .map_err(|e| write_err(&format!("clone kv delete: get_collection: {e}")))?;
                let Some(desc) = desc else {
                    return Ok(CloneWriteOutcome::Passthrough);
                };
                if desc.cloned_from.is_none() {
                    return Ok(CloneWriteOutcome::Passthrough);
                }
                match desc.clone_status {
                    CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
                    CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
                }

                // Split each key into one of two paths:
                //   • key absent in target (source-only) → record a tombstone
                //     so future scans hide the source row.
                //   • key present in target (already copied up or written
                //     in this clone) → dispatch a real KV Delete to remove
                //     the target row, then ALSO record a tombstone so any
                //     surviving source row remains hidden after deletion.
                //
                // Tombstoning unconditionally for target-resident keys is
                // safe: the source row (if any) must always be hidden in
                // this clone after the user has issued a DELETE.
                let mut keys_to_dispatch: Vec<Vec<u8>> = Vec::new();
                for key in keys {
                    let key_str = String::from_utf8_lossy(key).into_owned();
                    let key_in_target = probe_kv_key_in_target(
                        &self.state,
                        tenant_id,
                        db_id,
                        collection_qualified,
                        key,
                    )
                    .await
                    .map_err(|e| write_err(&format!("clone kv delete probe: {e}")))?;

                    perform_kv_clone_tombstone(KvTombstoneParams {
                        state: &self.state,
                        target_db_id: db_id,
                        target_collection: coll_name,
                        kv_key: key_str,
                    })
                    .map_err(|e| write_err(&format!("clone kv tombstone: {e}")))?;

                    if key_in_target {
                        keys_to_dispatch.push(key.clone());
                    }
                }

                if !keys_to_dispatch.is_empty() {
                    // Dispatch a real Delete for keys that exist in target.
                    let delete_plan = PhysicalPlan::Kv(KvOp::Delete {
                        collection: collection_qualified.to_string(),
                        keys: keys_to_dispatch,
                    });
                    let vshard_id =
                        VShardId::from_collection_in_database(db_id, collection_qualified);
                    let resp = dispatch_data_plane_raw(
                        &self.state,
                        tenant_id,
                        vshard_id,
                        db_id,
                        delete_plan,
                    )
                    .await
                    .map_err(|e| write_err(&format!("clone kv delete dispatch: {e}")))?;
                    return Ok(CloneWriteOutcome::Handled(resp));
                }

                let synthetic_resp = synthetic_ok_response(self.next_request_id(), Lsn::new(0));
                return Ok(CloneWriteOutcome::Handled(synthetic_resp));
            }
            _ => return Ok(CloneWriteOutcome::Passthrough),
        };

        // FieldSet path: check clone status, copy-up if needed.
        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, collection_qualified);

        let catalog_arc = self.state.credentials.catalog();
        let Some(catalog) = catalog_arc.as_ref() else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone kv write: get_collection: {e}")))?;
        let Some(desc) = desc else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let Some(ref origin) = desc.cloned_from else {
            return Ok(CloneWriteOutcome::Passthrough);
        };
        match desc.clone_status {
            CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        // FieldSet is not a delete.
        let _ = is_delete;

        let key_in_target =
            probe_kv_key_in_target(&self.state, tenant_id, db_id, collection_qualified, &kv_key)
                .await
                .map_err(|e| write_err(&format!("clone kv write probe: {e}")))?;

        if key_in_target {
            // Row exists in target — let the normal FieldSet proceed.
            return Ok(CloneWriteOutcome::Passthrough);
        }

        // Fetch source KV row and copy it up to target.
        let source_db_id = origin.source_database;
        let source_coll = origin.source_collection.as_str();
        let source_coll_qualified =
            crate::control::planner::sql_plan_convert::convert::db_qualified(
                source_db_id,
                source_coll,
            );

        let source_value = fetch_kv_source_value(
            &self.state,
            tenant_id,
            source_db_id,
            &source_coll_qualified,
            &kv_key,
        )
        .await
        .map_err(|e| write_err(&format!("clone kv copyup fetch: {e}")))?;

        let Some(source_value) = source_value else {
            // Row absent in source — let normal FieldSet run (no-op or error from DP).
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let kv_key_str = String::from_utf8_lossy(&kv_key).into_owned();

        perform_kv_clone_copyup(KvCopyUpParams {
            state: &Arc::clone(&self.state),
            tenant_id,
            target_db_id: db_id,
            target_collection: coll_name,
            kv_key,
            source_value_bytes: source_value,
        })
        .await
        .map_err(|e| write_err(&format!("clone kv copyup: {e}")))?;

        // Tombstone the source key so future clone reads do not merge in the
        // now-superseded source row.  The copy-up wrote the row to the target
        // and the FieldSet will overwrite it; the source copy must be hidden.
        perform_kv_clone_tombstone(KvTombstoneParams {
            state: &self.state,
            target_db_id: db_id,
            target_collection: coll_name,
            kv_key: kv_key_str,
        })
        .map_err(|e| write_err(&format!("clone kv tombstone after copyup: {e}")))?;

        // Fall through: let the original FieldSet dispatch to the target.
        Ok(CloneWriteOutcome::Passthrough)
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

/// Probe whether `document_id` exists in target storage.
///
/// Issues a synchronous PointGet to the local Data Plane and returns `true`
/// if the row is present.  Uses `Surrogate::ZERO` when the catalog has no
/// registered surrogate for the PK — the handler will return "not found".
async fn probe_row_in_target(
    state: &SharedState,
    tenant_id: TenantId,
    db_id: DatabaseId,
    collection_qualified: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<bool> {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: collection_qualified.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let vshard_id = VShardId::from_collection_in_database(db_id, collection_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, db_id, plan).await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw msgpack bytes for a row from the source collection.
///
/// Returns `None` when the row is absent in source (PointGet returned empty).
async fn fetch_source_row(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    document_id: &str,
    surrogate: Surrogate,
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Document(DocumentOp::PointGet {
        collection: source_coll_qualified.to_string(),
        document_id: document_id.to_string(),
        surrogate,
        pk_bytes: document_id.as_bytes().to_vec(),
        rls_filters: Vec::new(),
        system_time: nodedb_types::SystemTimeScope::Current,
        valid_at_ms: None,
    });
    let vshard_id = VShardId::from_collection_in_database(source_db_id, source_coll_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, source_db_id, plan).await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Probe whether `kv_key` exists in target KV storage.
///
/// Issues a KvOp::Get to the local Data Plane and returns `true` if the key
/// is present.
async fn probe_kv_key_in_target(
    state: &SharedState,
    tenant_id: TenantId,
    db_id: DatabaseId,
    collection_qualified: &str,
    kv_key: &[u8],
) -> crate::Result<bool> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: collection_qualified.to_string(),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Internal probe of the clone's own target collection — never
        // delegated to source, so no isolation ceiling applies.
        surrogate_ceiling: None,
    });
    let vshard_id = VShardId::from_collection_in_database(db_id, collection_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, db_id, plan).await?;
    Ok(!resp.payload.is_empty() && resp.status == Status::Ok)
}

/// Fetch the raw value bytes for a KV row from the source collection.
///
/// Returns `None` when the key is absent in source (KvOp::Get returned empty).
async fn fetch_kv_source_value(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    kv_key: &[u8],
) -> crate::Result<Option<Vec<u8>>> {
    let plan = PhysicalPlan::Kv(KvOp::Get {
        collection: source_coll_qualified.to_string(),
        key: kv_key.to_vec(),
        rls_filters: Vec::new(),
        // Copy-up reads must see every binding in the source — the
        // post-copy target write reflects the latest source state, and
        // a missed source row would silently drop data on the clone.
        surrogate_ceiling: None,
    });
    let vshard_id = VShardId::from_collection_in_database(source_db_id, source_coll_qualified);
    let resp = dispatch_data_plane_raw(state, tenant_id, vshard_id, source_db_id, plan).await?;
    if resp.payload.is_empty() || resp.status != Status::Ok {
        return Ok(None);
    }
    Ok(Some(resp.payload.as_ref().to_vec()))
}

/// Dispatch a plan directly to the local Data Plane, bypassing WAL and Raft.
/// Used only for read probes inside the clone write helper.
async fn dispatch_data_plane_raw(
    state: &SharedState,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
) -> crate::Result<Response> {
    let req_id = RequestId::new(
        state
            .request_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    );
    let deadline_secs = state.tuning.network.default_deadline_secs;
    let deadline_dur = Duration::from_secs(deadline_secs);
    let req = Request {
        request_id: req_id,
        tenant_id,
        vshard_id,
        database_id,
        plan,
        deadline: std::time::Instant::now() + deadline_dur,
        priority: Priority::Normal,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
    };
    let mut rx = state.tracker.register(req_id);
    match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(req)?,
        Err(p) => p.into_inner().dispatch(req)?,
    }
    tokio::time::timeout(deadline_dur, rx.recv())
        .await
        .map_err(|_| crate::Error::DeadlineExceeded { request_id: req_id })?
        .ok_or(crate::Error::Dispatch {
            detail: "clone write probe: response channel closed".into(),
        })
}

/// Build a synthetic OK response with no payload (used for tombstone success).
fn synthetic_ok_response(request_id: RequestId, watermark_lsn: Lsn) -> Response {
    Response {
        request_id,
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Vec::<u8>::new().into(),
        watermark_lsn,
        error_code: None,
    }
}

/// Strip the `"<db_id>/"` db-qualified prefix.
fn strip_db_prefix(db_id: DatabaseId, qualified: &str) -> &str {
    if db_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", db_id.as_u64());
    qualified.strip_prefix(prefix.as_str()).unwrap_or(qualified)
}

/// Convert a clone write error to a PgWireError.
fn write_err(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        msg.to_owned(),
    )))
}
