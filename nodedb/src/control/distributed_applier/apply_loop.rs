// SPDX-License-Identifier: BUSL-1.1

//! Background apply loop — reads committed Raft entries from the mpsc channel,
//! submits them through the shared Control-Plane write funnel (which appends
//! each entry's redo record on THIS replica before the enqueue), and resolves
//! propose waiters with the result.
//!
//! Each batch advances the group's durable applied floor (see
//! [`super::applied_index`]) to its highest contiguous successfully-applied
//! entry, so the next boot replays only above it and no entry is applied by
//! both WAL replay and Raft log replay.

use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::debug;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::array_sync::raft_apply::{
    AppliedPosition, ArrayCellTarget, apply_array_cell_write, apply_array_op, apply_array_schema,
};
use crate::control::cluster::calvin::ReadResultEvent;
use crate::control::server::dispatch_utils::{
    ChangeFeedOwner, SubmitWrite, WalDurability, WriteOrdering, submit_write,
};
use crate::control::state::SharedState;
use crate::control::wal_replication::{ReplicatedEntry, ReplicatedWrite, from_replicated_entry};
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::ArrayOp;

use super::applied_index::{AppliedPrefix, save_applied_index};
use super::applier::ApplyBatch;
use super::propose_tracker::{AppliedWrite, ProposeTracker};

fn committed_response_result(
    response: &crate::bridge::envelope::Response,
) -> crate::Result<AppliedWrite> {
    if response.status == Status::Ok {
        return Ok(AppliedWrite::from_response(response));
    }
    if let Some(error @ crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch { .. }) =
        response.error_code.as_deref()
    {
        return Err(crate::Error::DataPlane(error.clone()));
    }
    let reason = response
        .error_code
        .as_ref()
        .map(|code| format!("{code:?}"))
        .unwrap_or_else(|| "execution error".into());
    tracing::warn!(reason = %reason, "applying committed write failed");
    Err(crate::Error::Internal { detail: reason })
}

fn deterministic_crdt_fence_noop(result: &crate::Result<AppliedWrite>) -> bool {
    matches!(
        result,
        Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch { .. }
        ))
    )
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
        // The floor is saved ONCE per batch, after the loop — never per entry.
        // `save_applied_index` lands a redb transaction, and redb commits at
        // `Durability::Immediate`, so a per-entry save puts one synchronous
        // fsync per applied entry directly on the raft apply path. That stalls
        // the raft loop hard enough to delay heartbeats and keep elections from
        // stabilizing under a multi-node write load. One fsync per batch
        // amortizes the cost across every entry in it and keeps the critical
        // path free.
        //
        // `AppliedPrefix` computes WHICH index is safe to save: the highest
        // contiguous successfully-applied entry, stopping at the first failure
        // and never advancing past it. Every branch below must therefore report
        // its outcome — `record` for the ones whose success means a durable
        // redo record, `skip` for the ones that apply no durable state at all.
        let mut prefix = AppliedPrefix::new();
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

            // Database scope for the entry, read from the wire. `0` decodes to
            // `DatabaseId::DEFAULT` (the pre-`database_id` legacy shape). The
            // generic decode path (`from_replicated_entry`) returns only
            // `(tenant, vshard, plan, resolved_now_ms)`, so the scope is taken
            // from the entry itself — a WAL redo appended under the wrong
            // database scope replays into the wrong catalog namespace.
            let database_id = replicated_opt
                .as_ref()
                .map(|e| DatabaseId::new(e.database_id))
                .unwrap_or(DatabaseId::DEFAULT);

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
                            crate::control::array_sync::ArrayOpTarget {
                                tenant_id: TenantId::new(replicated.tenant_id),
                                database_id: DatabaseId::new(replicated.database_id),
                                array,
                            },
                            op_bytes,
                            provenance.as_deref(),
                        )
                        .await;
                        // Advance the durable prefix only when the op durably
                        // applied — same safe-watermark rule as the Data Plane
                        // write path below, and the same funnel: the op path
                        // submits through `submit_write`, so its redo is fsynced
                        // before it reports success. A failure breaks the
                        // prefix: the entry must stay replayable.
                        prefix.record(entry.index, applied_ok);
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
                                tenant_id: TenantId::new(replicated.tenant_id),
                                database_id: DatabaseId::new(replicated.database_id),
                                array,
                                snapshot_payload,
                                schema_hlc_bytes,
                            },
                        );
                        // Advance the durable prefix only when the schema
                        // snapshot durably imported.
                        //
                        // This is the one applied branch that mints no WAL redo
                        // record, and it needs none: its entire effect is two
                        // fsync-committed redb transactions — the schema
                        // registry's snapshot row and the array catalog's entry
                        // — both written before it reports success. The floor's
                        // invariant ("this entry's state survives a restart, so
                        // Raft need not redeliver it") is therefore already met
                        // by the registries themselves. The cell paths have no
                        // such durable store behind them: their state lives in
                        // Data-Plane memtables and exists on disk only as the
                        // redo record the funnel appends, which is why they must
                        // route through `submit_write`.
                        prefix.record(entry.index, applied_ok);
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
                                // Prefix-neutral, like the forward below: a read
                                // result mints no durable state either way, so
                                // there is nothing a re-delivery could restore
                                // and nothing later entries must wait behind.
                                prefix.skip();
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
                        tracker.complete(
                            batch.group_id,
                            entry.index,
                            applied_key,
                            Ok(AppliedWrite::unversioned(Vec::new())),
                        );
                        // A read result is forwarded to an in-memory Calvin
                        // scheduler and writes nothing durable, so it neither
                        // advances the prefix nor breaks it. Advancing on it
                        // would assert a redo record that does not exist;
                        // breaking on it would stall the floor behind an entry
                        // that a re-delivery could not usefully replay anyway —
                        // the epoch it belongs to does not survive a restart —
                        // and force every later write in the batch to be applied
                        // twice on the next boot.
                        prefix.skip();
                        continue;
                    }
                    _ => {}
                }
            }

            let decoded =
                from_replicated_entry(&entry.data, Some(state.surrogate_assigner.as_ref()));
            let (tenant_id, vshard_id, plan, resolved_now_ms) = match decoded {
                Ok(Some(t)) => t,
                Ok(None) => {
                    // Couldn't deserialize — might be a different format or corrupted.
                    debug!(
                        group_id = batch.group_id,
                        index = entry.index,
                        "skipping non-ReplicatedEntry commit"
                    );
                    tracker.complete(
                        batch.group_id,
                        entry.index,
                        applied_key,
                        Ok(AppliedWrite::unversioned(Vec::new())),
                    );
                    // Prefix-neutral. This is a pure shape check over
                    // `entry.data`, so a re-delivery on the next boot decodes to
                    // `None` again and skips again — stalling the floor behind
                    // it buys nothing and costs a double-apply of every later
                    // write in the batch. It applied no state, so it must not
                    // advance the floor either.
                    prefix.skip();
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
                    // Breaks the prefix, unlike the `Ok(None)` skip above: this
                    // IS a write, and it failed against live surrogate-assigner
                    // state rather than on its own bytes, so a re-delivery can
                    // legitimately succeed. Holding the floor below it is what
                    // keeps it replayable.
                    prefix.record(entry.index, false);
                    continue;
                }
            };

            // Raft-native array cell writes (`ArrayCellPut` / `ArrayCellDelete`)
            // decode to `PhysicalPlan::Array(Put | Delete)`. A follower must
            // OPEN the array on the Data Plane before applying, so these route
            // through the array-open bootstrap first — and then through the same
            // write funnel as the generic branch below, which is what gives them
            // a redo record and the fsync the applied floor asserts. No other
            // `ReplicatedWrite` variant decodes to a `PhysicalPlan::Array`, so
            // this match is exact.
            if matches!(
                plan,
                PhysicalPlan::Array(ArrayOp::Put { .. } | ArrayOp::Delete { .. })
            ) {
                let applied_ok = apply_array_cell_write(
                    &state,
                    &tracker,
                    AppliedPosition {
                        group_id: batch.group_id,
                        log_index: entry.index,
                        applied_key,
                    },
                    ArrayCellTarget {
                        tenant_id,
                        database_id,
                        vshard: vshard_id,
                        resolved_now_ms,
                    },
                    plan,
                )
                .await;
                prefix.record(entry.index, applied_ok);
                continue;
            }

            let submitted = submit_write(
                &state,
                SubmitWrite {
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan,
                    trace_id: TraceId::generate(),
                    // Cluster mode has exactly ONE write-apply path — this loop;
                    // the proposing node does not execute locally before commit
                    // either. Tagging these `RaftFollower` would mean AFTER
                    // triggers, DML audit, and CRDT packaging never fire anywhere
                    // in cluster mode, so the committed write keeps the `User`
                    // source its proposer had.
                    event_source: crate::event::EventSource::User,
                    txn_id: None,
                    // Auth ran on the proposing node before the entry was
                    // proposed; the committed entry carries no session user.
                    user_id: None,
                    // The redo record is appended HERE, on this replica, from the
                    // committed plan — the leader's WAL LSN is deliberately not
                    // carried on the wire, and the memory-only engines have no
                    // other durability path. `now_override` pins a TTL-bearing KV
                    // write's `expire_at_ms` to the instant the proposing node
                    // resolved, so this replica's redo record and its live apply
                    // install the byte-identical value every other replica does.
                    durability: WalDurability::AppendHere {
                        now_override: resolved_now_ms,
                    },
                    // Raft committed this entry at a fixed log index; every
                    // replica applies it in that order. Re-entering the
                    // write-admission gate would re-decide an ordering that is
                    // already final.
                    ordering: WriteOrdering::AlreadyOrdered,
                    // This loop runs on EVERY replica, so it must not publish:
                    // the node that proposed this entry already published the
                    // write's change event once, after commit + apply. Emitting
                    // here would give each subscriber one copy per replica plus
                    // a NOTIFY fan-out from each. See [`ChangeFeedOwner`].
                    change_feed: ChangeFeedOwner::Unowned,
                },
            )
            .await
            .map(|outcome| outcome.response);

            // The funnel returns an error-status response as `Ok`; a committed
            // entry that failed to apply must surface to the propose waiter as a
            // failure, not as an empty success.
            let result = match submitted {
                // The response carries this replica's post-write
                // `coll_write_lsn` for the written collection, which the
                // proposer needs as its read-your-writes floor: the version is
                // minted here (the funnel's WAL append) and never travels on the
                // wire, so the propose waiter is the only place it can be
                // handed back.
                Ok(resp) if resp.status == Status::Ok => Ok(AppliedWrite::from_response(&resp)),
                Ok(resp) => committed_response_result(&resp),
                Err(e) => {
                    tracing::warn!(
                        group_id = batch.group_id,
                        index = entry.index,
                        error = %e,
                        "applying committed write failed"
                    );
                    Err(crate::Error::Internal {
                        detail: e.to_string(),
                    })
                }
            };

            let applied_ok = result.is_ok() || deterministic_crdt_fence_noop(&result);
            tracker.complete(batch.group_id, entry.index, applied_key, result);

            // Extend the batch's durable prefix. On success `submit_write`'s
            // durable-at-ack barrier has already fsynced this entry's redo,
            // which is exactly the fact the floor asserts — `entry.index` is
            // the data-plane applied watermark here, NOT raft's commit index.
            // On failure the engines did not persist this index, so it is
            // neither a safe compaction boundary nor a safe restart floor;
            // breaking the prefix is what keeps a genuinely failed apply
            // replayable rather than silently skipped.
            prefix.record(entry.index, applied_ok);
        }

        // One save + one compaction check per batch, against the contiguous
        // prefix. Compaction is deliberately driven by the same index the floor
        // was just saved at — never the batch's last delivered index — so it can
        // never discard an entry the next boot still has to replay. Compacting
        // on the raft commit index while the SPSC apply lags would likewise let
        // the `SnapshotBuilder` serialize incomplete engine state and corrupt a
        // lagging follower's snapshot.
        if let Some(applied_index) = prefix.floor() {
            record_durable_apply(&state, batch.group_id, applied_index);
        }
    }
}

/// Record entry `applied_index` of `group_id` as durably applied: persist the
/// group's durable applied floor, then fire the compaction trigger against it.
///
/// `applied_index` MUST be the highest CONTIGUOUS successfully-applied entry —
/// [`AppliedPrefix::floor`] — not merely some entry that happened to succeed.
/// Everything at and below it must have applied with its redo record already
/// WAL-fsync-durable, because that is the fact the floor asserts and the next
/// boot resumes Raft delivery above it on the strength of it.
///
/// Called once per apply batch: each call is a redb commit and therefore an
/// fsync, and one per entry is slow enough to stall the raft loop it runs on.
///
/// Order is load-bearing: the floor lands first because compaction is itself
/// gated on the floor (it may only discard entries the engines can no longer
/// need the log for). Compacting first would either be refused or, if the gate
/// used the delivery watermark, discard entries whose redo is not yet fsynced.
fn record_durable_apply(state: &Arc<SharedState>, group_id: u64, applied_index: u64) {
    save_applied_index(state, group_id, applied_index);
    maybe_compact_log(state, group_id, applied_index);
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
    use super::*;

    #[test]
    fn fenced_frontier_mismatch_completes_retry_and_advances_durable_prefix() {
        let result: crate::Result<AppliedWrite> = Err(crate::Error::DataPlane(
            crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch {
                expected: [1; 32],
                actual: [2; 32],
            },
        ));
        assert!(deterministic_crdt_fence_noop(&result));
        assert!(matches!(
            result,
            Err(crate::Error::DataPlane(
                crate::bridge::envelope::ErrorCode::CrdtFrontierMismatch { .. }
            ))
        ));

        let mut prefix = AppliedPrefix::new();
        prefix.record(17, deterministic_crdt_fence_noop(&result));
        assert_eq!(prefix.floor(), Some(17));
    }
}
