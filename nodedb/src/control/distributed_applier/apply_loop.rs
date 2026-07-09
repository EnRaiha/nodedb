// SPDX-License-Identifier: BUSL-1.1

//! Background apply loop — reads committed Raft entries from the mpsc channel,
//! dispatches them through the SPSC bridge to the Data Plane, and resolves
//! propose waiters with the result.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
use crate::control::array_sync::raft_apply::{AppliedPosition, apply_array_op, apply_array_schema};
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, from_replicated_entry};
use crate::types::{DatabaseId, ReadConsistency, TenantId, TraceId, VShardId};

use super::applier::ApplyBatch;
use super::propose_tracker::ProposeTracker;

/// Outcome of dispatching a single request through the SPSC bridge.
enum DispatchOutcome {
    Ok(Vec<u8>),
    /// Failure with a human-readable reason (dispatch error, timeout,
    /// closed channel, or an error response from the Data Plane).
    Failed(String),
}

/// Build a `Request` for a committed write with default deadline / priority.
fn build_request(
    state: &Arc<SharedState>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    event_source: crate::event::EventSource,
) -> Request {
    Request {
        request_id: state.next_request_id(),
        tenant_id,
        database_id: DatabaseId::DEFAULT,
        vshard_id,
        plan,
        deadline: Instant::now() + Duration::from_secs(30),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: crate::bridge::envelope::Admission::Exempt(
            crate::bridge::envelope::ExemptReason::AlreadyOrdered,
        ),
    }
}

/// Dispatch a request through the SPSC bridge and await its response.
///
/// Returns:
/// - `Ok(payload)` on success
/// - `Failed(reason)` for dispatch error, channel close, deadline exceeded,
///   or an error-status response from the Data Plane
async fn dispatch_and_await(state: &Arc<SharedState>, request: Request) -> DispatchOutcome {
    let request_id = request.request_id;
    let mut rx = state.tracker.register(request_id);

    let dispatch_result = match state.dispatcher.lock() {
        Ok(mut d) => d.dispatch(request),
        Err(poisoned) => poisoned.into_inner().dispatch(request),
    };

    if let Err(e) = dispatch_result {
        return DispatchOutcome::Failed(format!("dispatch failed: {e}"));
    }

    match tokio::time::timeout(Duration::from_secs(30), async { rx.recv().await.ok_or(()) }).await {
        Ok(Ok(resp)) => {
            if resp.status == Status::Error {
                let reason = resp
                    .error_code
                    .as_ref()
                    .map(|c| format!("{c:?}"))
                    .unwrap_or_else(|| "execution error".into());
                DispatchOutcome::Failed(reason)
            } else {
                DispatchOutcome::Ok(resp.payload.to_vec())
            }
        }
        Ok(Err(_)) => DispatchOutcome::Failed("response channel closed".to_string()),
        Err(_) => DispatchOutcome::Failed("deadline exceeded".to_string()),
    }
}

/// Run the background loop that applies committed Raft entries to the local Data Plane.
///
/// This task reads from the apply channel, deserializes each entry, dispatches
/// the write to the Data Plane via SPSC, and notifies proposers.
pub async fn run_apply_loop(
    mut apply_rx: mpsc::Receiver<ApplyBatch>,
    state: Arc<SharedState>,
    tracker: Arc<ProposeTracker>,
    calvin_read_result_senders: Arc<
        std::sync::Mutex<std::collections::BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>,
    >,
) {
    while let Some(batch) = apply_rx.recv().await {
        for entry in &batch.entries {
            // Decode once; reused for both the idempotency key and the
            // Array/Calvin fast-path match below. Returns 0 for
            // unparseable / pre-key entries; the tracker treats 0 as
            // "no key" (no mismatch detection).
            let replicated_opt = ReplicatedEntry::from_bytes(&entry.data);
            let applied_key = replicated_opt
                .as_ref()
                .map(|e| e.idempotency_key)
                .unwrap_or(0);

            // ── Array CRDT variants — handled on the Control Plane, bypass Data Plane ──
            if let Some(replicated) = replicated_opt {
                let target_vshard = replicated.vshard_id;
                match replicated.write {
                    ReplicatedWrite::ArrayOp {
                        ref array,
                        ref op_bytes,
                        ref provenance,
                        ..
                    } => {
                        let applied_ok = apply_array_op(
                            &state,
                            &tracker,
                            AppliedPosition {
                                group_id: batch.group_id,
                                log_index: entry.index,
                                applied_key,
                            },
                            array,
                            op_bytes,
                            provenance.as_deref(),
                        )
                        .await;
                        // Advance the compaction watermark only when the op
                        // durably applied — same safe-watermark rule as the
                        // Data Plane write path below.
                        if applied_ok {
                            maybe_compact_log(&state, batch.group_id, entry.index);
                        }
                        continue;
                    }
                    ReplicatedWrite::ArraySchema {
                        ref array,
                        ref snapshot_payload,
                        schema_hlc_bytes,
                    } => {
                        let applied_ok = apply_array_schema(
                            &state,
                            &tracker,
                            AppliedPosition {
                                group_id: batch.group_id,
                                log_index: entry.index,
                                applied_key,
                            },
                            crate::control::array_sync::raft_apply::ArraySchemaPayload {
                                array,
                                snapshot_payload,
                                schema_hlc_bytes,
                            },
                        );
                        // Advance the compaction watermark only when the
                        // schema snapshot durably imported.
                        if applied_ok {
                            maybe_compact_log(&state, batch.group_id, entry.index);
                        }
                        continue;
                    }
                    ReplicatedWrite::CalvinReadResult {
                        epoch,
                        position,
                        passive_vshard,
                        tenant_id,
                        ref values,
                    } => {
                        let decoded_values: Vec<(
                            nodedb_physical::physical_plan::meta::PassiveReadKeyId,
                            nodedb_types::Value,
                        )> = match zerompk::from_msgpack(values) {
                            Ok(decoded) => decoded,
                            Err(e) => {
                                tracing::warn!(
                                    group_id = batch.group_id,
                                    index = entry.index,
                                    error = %e,
                                    "failed to decode CalvinReadResult payload"
                                );
                                tracker.complete(
                                    batch.group_id,
                                    entry.index,
                                    applied_key,
                                    Err(crate::Error::Internal {
                                        detail: format!("decode CalvinReadResult payload: {e}"),
                                    }),
                                );
                                continue;
                            }
                        };

                        let event = ReadResultEvent {
                            epoch,
                            position,
                            passive_vshard,
                            tenant_id: TenantId::new(tenant_id),
                            values: decoded_values,
                        };

                        let send_result = calvin_read_result_senders
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .get(&target_vshard)
                            .cloned()
                            .map(|sender| sender.try_send(event));

                        if let Some(Err(e)) = send_result {
                            tracing::warn!(
                                group_id = batch.group_id,
                                index = entry.index,
                                error = %e,
                                "failed to forward CalvinReadResult to scheduler"
                            );
                        }
                        tracker.complete(batch.group_id, entry.index, applied_key, Ok(vec![]));
                        continue;
                    }
                    _ => {}
                }
            }

            let decoded =
                from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
            let (tenant_id, vshard_id, plan) = match decoded {
                Ok(Some(t)) => t,
                Ok(None) => {
                    // Couldn't deserialize — might be a different format or corrupted.
                    debug!(
                        group_id = batch.group_id,
                        index = entry.index,
                        "skipping non-ReplicatedEntry commit"
                    );
                    tracker.complete(batch.group_id, entry.index, applied_key, Ok(vec![]));
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        error = %e,
                        "failed to decode replicated entry (surrogate bind error)"
                    );
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Err(crate::Error::Internal {
                            detail: format!("decode replicated entry: {e}"),
                        }),
                    );
                    continue;
                }
            };

            let request = build_request(
                &state,
                tenant_id,
                vshard_id,
                plan,
                crate::event::EventSource::User,
            );

            let result = match dispatch_and_await(&state, request).await {
                DispatchOutcome::Ok(payload) => Ok(payload),
                DispatchOutcome::Failed(reason) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        reason = %reason,
                        "applying committed write failed"
                    );
                    Err(crate::Error::Internal { detail: reason })
                }
            };

            let applied_ok = result.is_ok();
            tracker.complete(batch.group_id, entry.index, applied_key, result);

            // Auto-compact the group's Raft log if its configured
            // threshold has been reached. This MUST run only after the
            // entry has been DURABLY applied to the Data Plane above —
            // `entry.index` is the data-plane applied watermark at this
            // point, NOT raft's commit index. Compacting on commit while
            // the SPSC apply lags would let the `SnapshotBuilder`
            // serialize incomplete engine state and corrupt a lagging
            // follower's snapshot. Skip on apply failure: the engines did
            // not persist this index, so it is not a safe compaction
            // boundary.
            if applied_ok {
                maybe_compact_log(&state, batch.group_id, entry.index);
            }
        }
    }
}

/// Fire the Raft log-compaction trigger for `group_id` up to the
/// data-plane applied index `applied_index`, if a compactor is wired.
///
/// Gated by the caller on data-plane apply completion. A no-op when no
/// compactor is installed (single-node mode) or when the group's
/// `log_compaction_threshold` is `None`.
fn maybe_compact_log(state: &Arc<SharedState>, group_id: u64, applied_index: u64) {
    let Some(compactor) = state.raft_compactor.get() else {
        return;
    };
    match compactor(group_id, applied_index) {
        Ok(true) => {
            debug!(
                group_id,
                applied_index, "raft log compacted past data-plane applied watermark"
            );
        }
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(
                group_id,
                applied_index,
                error = %e,
                "raft log compaction failed"
            );
        }
    }
}
