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
            // Apply the entry, then auto-compact the group's Raft log if its
            // configured threshold has been reached. Compaction MUST run only
            // after the entry has been DURABLY applied to the Data Plane —
            // `entry.index` is the data-plane applied watermark at this point,
            // NOT raft's commit index. Compacting on commit while the SPSC
            // apply lags would let the `SnapshotBuilder` serialize incomplete
            // engine state and corrupt a lagging follower's snapshot. Skip on
            // apply failure: the engines did not persist this index, so it is
            // not a safe compaction boundary.
            let entry_applied_ok = apply_one_entry(
                &state,
                &tracker,
                &calvin_read_result_senders,
                batch.group_id,
                entry,
            )
            .await;

            if entry_applied_ok {
                maybe_compact_log(&state, batch.group_id, entry.index);
            }
        }
    }
}

/// Apply a single committed Raft entry to the local Data Plane.
///
/// Returns `true` iff the entry was successfully applied (or applied as a
/// no-op), `false` if it failed and the proposer was resolved with an `Err`.
/// All variants flow through this one function so the caller has a single
/// `entry_applied_ok` signal to gate Raft log compaction — no early return
/// may bypass that gate.
async fn apply_one_entry(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    calvin_read_result_senders: &Arc<
        std::sync::Mutex<std::collections::BTreeMap<u32, mpsc::Sender<ReadResultEvent>>>,
    >,
    group_id: u64,
    entry: &nodedb_raft::message::LogEntry,
) -> bool {
    // Extract idempotency key once at the top so every tracker.complete on
    // this entry can pass it. Returns 0 for unparseable / pre-key entries;
    // the tracker treats 0 as "no key" (no mismatch detection).
    let applied_key = ReplicatedEntry::from_bytes(&entry.data)
        .map(|e| e.idempotency_key)
        .unwrap_or(0);

    // ── Array CRDT / Calvin variants — handled on the Control Plane, bypass Data Plane ──
    if let Some(replicated) = ReplicatedEntry::from_bytes(&entry.data) {
        let target_vshard = replicated.vshard_id;
        match replicated.write {
            ReplicatedWrite::ArrayOp {
                ref array,
                ref op_bytes,
                ref provenance,
                ..
            } => {
                return apply_array_op(
                    state,
                    tracker,
                    AppliedPosition {
                        group_id,
                        log_index: entry.index,
                        applied_key,
                    },
                    array,
                    op_bytes,
                    provenance.as_deref(),
                )
                .await;
            }
            ReplicatedWrite::ArraySchema {
                ref array,
                ref snapshot_payload,
                schema_hlc_bytes,
            } => {
                return apply_array_schema(
                    state,
                    tracker,
                    AppliedPosition {
                        group_id,
                        log_index: entry.index,
                        applied_key,
                    },
                    crate::control::array_sync::raft_apply::ArraySchemaPayload {
                        array,
                        snapshot_payload,
                        schema_hlc_bytes,
                    },
                );
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
                            group_id,
                            index = entry.index,
                            error = %e,
                            "failed to decode CalvinReadResult payload"
                        );
                        tracker.complete(
                            group_id,
                            entry.index,
                            applied_key,
                            Err(crate::Error::Internal {
                                detail: format!("decode CalvinReadResult payload: {e}"),
                            }),
                        );
                        return false;
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
                        group_id,
                        index = entry.index,
                        error = %e,
                        "failed to forward CalvinReadResult to scheduler"
                    );
                }
                tracker.complete(group_id, entry.index, applied_key, Ok(vec![]));
                return true;
            }
            _ => {}
        }
    }

    let decoded = from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
    let (tenant_id, vshard_id, plan) = match decoded {
        Ok(Some(t)) => t,
        Ok(None) => {
            // Couldn't deserialize — might be a different format or corrupted.
            debug!(
                group_id,
                index = entry.index,
                "skipping non-ReplicatedEntry commit"
            );
            tracker.complete(group_id, entry.index, applied_key, Ok(vec![]));
            // Applied as a no-op: this index carries no engine state, so it is
            // a safe compaction boundary.
            return true;
        }
        Err(e) => {
            tracing::warn!(
                group_id,
                index = entry.index,
                error = %e,
                "failed to decode replicated entry (surrogate bind error)"
            );
            tracker.complete(
                group_id,
                entry.index,
                applied_key,
                Err(crate::Error::Internal {
                    detail: format!("decode replicated entry: {e}"),
                }),
            );
            return false;
        }
    };

    let request = build_request(
        state,
        tenant_id,
        vshard_id,
        plan,
        crate::event::EventSource::User,
    );

    let result = match dispatch_and_await(state, request).await {
        DispatchOutcome::Ok(payload) => Ok(payload),
        DispatchOutcome::Failed(reason) => {
            tracing::warn!(
                group_id,
                index = entry.index,
                reason = %reason,
                "applying committed write failed"
            );
            Err(crate::Error::Internal { detail: reason })
        }
    };

    let entry_applied_ok = result.is_ok();
    tracker.complete(group_id, entry.index, applied_key, result);
    entry_applied_ok
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use nodedb_raft::message::LogEntry;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::distributed_applier::ProposeTracker;
    use crate::control::state::SharedState;
    use crate::wal::WalManager;

    /// Build a `SharedState` with a recording compactor installed. Returns the
    /// state plus the shared log of `(group_id, applied_index)` compaction calls.
    fn test_state_with_recording_compactor() -> (Arc<SharedState>, Arc<Mutex<Vec<(u64, u64)>>>) {
        let dir = tempfile::tempdir().expect("tmpdir");
        // Leak the TempDir so it outlives the SharedState in the test.
        let wal_path = dir.path().join("apply_loop_test.wal");
        std::mem::forget(dir);
        let wal = Arc::new(WalManager::open_for_testing(&wal_path).expect("wal"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal);

        let recorded: Arc<Mutex<Vec<(u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::clone(&recorded);
        let compactor: Arc<crate::control::wal_replication::RaftCompactor> =
            Arc::new(move |group_id: u64, applied_index: u64| {
                rec.lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push((group_id, applied_index));
                Ok(true)
            });
        if state.raft_compactor.set(compactor).is_err() {
            panic!("raft_compactor already set");
        }
        (state, recorded)
    }

    fn empty_calvin_senders() -> Arc<
        Mutex<
            BTreeMap<
                u32,
                tokio::sync::mpsc::Sender<crate::control::cluster::calvin::ReadResultEvent>,
            >,
        >,
    > {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    /// A committed entry that applies as a no-op (non-`ReplicatedEntry` bytes)
    /// reaches the compaction tail — `entry.index` is a safe boundary.
    #[tokio::test]
    async fn successful_noop_entry_triggers_compaction() {
        let (state, recorded) = test_state_with_recording_compactor();
        let tracker = Arc::new(ProposeTracker::new());
        let senders = empty_calvin_senders();

        // `0xc1` is the msgpack "never used" byte: not a decodable
        // `ReplicatedEntry`, so `from_replicated_entry` returns `Ok(None)`.
        let entry = LogEntry {
            term: 1,
            index: 42,
            data: vec![0xc1],
        };

        let applied = apply_one_entry(&state, &tracker, &senders, 7, &entry).await;
        if applied {
            maybe_compact_log(&state, 7, entry.index);
        }

        assert!(applied, "no-op entry must report applied");
        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(7, 42)],
            "successful no-op entry must trigger compaction at its index"
        );
    }

    /// A failed entry (undecodable `CalvinReadResult` payload) must NOT reach
    /// the compaction tail — compacting past an unapplied index corrupts a
    /// lagging follower's snapshot.
    #[tokio::test]
    async fn failed_entry_does_not_trigger_compaction() {
        let (state, recorded) = test_state_with_recording_compactor();
        let tracker = Arc::new(ProposeTracker::new());
        let senders = empty_calvin_senders();

        // `values` is `0xc1`: an undecodable `Vec<(PassiveReadKeyId, Value)>`,
        // forcing the decode-error path which resolves the proposer with `Err`.
        let replicated = ReplicatedEntry::new(
            0,
            0,
            ReplicatedWrite::CalvinReadResult {
                epoch: 1,
                position: 0,
                passive_vshard: 0,
                tenant_id: 0,
                values: vec![0xc1],
            },
        );
        let entry = LogEntry {
            term: 1,
            index: 99,
            data: replicated.to_bytes(),
        };

        let applied = apply_one_entry(&state, &tracker, &senders, 7, &entry).await;
        if applied {
            maybe_compact_log(&state, 7, entry.index);
        }

        assert!(!applied, "failed entry must report not-applied");
        assert!(
            recorded.lock().unwrap().is_empty(),
            "failed entry must NOT trigger compaction"
        );
    }
}
