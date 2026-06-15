// SPDX-License-Identifier: BUSL-1.1

//! Single fan-out/gather primitive for coordinator-mediated data movement.
//!
//! `gather_all_cores` fans a child plan to every Data-Plane core in parallel
//! using `join_all`, collects per-core payloads, and merges them into two
//! complementary views:
//!
//! - `raw`: concatenated per-core payloads (multiple msgpack arrays back-to-back).
//!   Consumed by the sync layer and legacy raw-scan paths.
//! - `merged_array`: a single msgpack array containing every row element
//!   from all cores.  Consumed by the response path and by `ProviderScan`
//!   embedding in join inputs.
//!
//! `finalize_aggregate` runs the Arrow SIMD post-processing pass for
//! `Gather{as_aggregate: true}` plans.

use futures::future::join_all;
use std::time::{Duration, Instant};

use nodedb_query::msgpack_scan;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::arrow_convert;
use crate::control::gateway::core::QueryContext;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};

/// Outcomes of a full fan-out/gather cycle across all Data-Plane cores.
pub struct GatherOutcome {
    /// Concatenated per-core payloads (multiple msgpack arrays back-to-back).
    /// Consumed by the sync layer and raw-scan paths.
    pub raw: Vec<u8>,
    /// Single merged msgpack array of all row elements.
    /// Consumed by the pgwire/native response path and `ProviderScan` embedding.
    pub merged_array: Vec<u8>,
    /// Maximum watermark LSN seen across all responding cores.
    pub watermark_lsn: Lsn,
}

/// Fan `plan` to every Data-Plane core in parallel and gather the results.
///
/// All per-core sends are issued before any response is awaited (`join_all`).
/// `NotFound` errors from individual cores are treated as "no rows" (the
/// collection shard simply has no matching data on that core).  Any other
/// error status from a core is noted, but only surfaces as an error if no
/// rows were gathered at all — partial results from healthy cores are returned
/// as-is.
pub async fn gather_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<GatherOutcome> {
    // Track broadcast calls for observability (shared counter with broadcast.rs).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let deadline_secs = state.tuning.network.default_deadline_secs;

    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    // Issue all per-core sends and collect receiver channels before awaiting.
    // This ensures every core has the request in flight before we block on any
    // of them, matching true parallelism semantics.
    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = state.next_request_id();
        let vshard_id = VShardId::new(core_id as u32);
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan: plan.clone(),
            deadline: Instant::now() + Duration::from_secs(deadline_secs),
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
        };

        let rx = state.tracker.register(request_id);
        state
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)?;
        receivers.push((core_id, rx));
    }

    // Await all responses in parallel using join_all.
    let deadline = Duration::from_secs(deadline_secs);
    let response_futures = receivers.into_iter().map(|(core_id, mut rx)| async move {
        tokio::time::timeout(deadline, async move { rx.recv().await.ok_or(()) })
            .await
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("gather timeout on core {core_id}"),
            })?
            .map_err(|_| crate::Error::Dispatch {
                detail: format!("gather channel closed on core {core_id}"),
            })
    });

    let results: Vec<crate::Result<Response>> = join_all(response_futures).await;

    let mut raw = Vec::new();
    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    let mut had_error = false;
    let mut error_msg = String::new();

    for result in results {
        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                had_error = true;
                error_msg = e.to_string();
                continue;
            }
        };

        if resp.status == Status::Error {
            if let Some(ref ec) = resp.error_code {
                match ec {
                    crate::bridge::envelope::ErrorCode::NotFound => continue,
                    _ => {
                        had_error = true;
                        error_msg = format!("{ec:?}");
                    }
                }
            }
            continue;
        }

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        if resp.payload.is_empty() {
            continue;
        }

        let payload_bytes: &[u8] = resp.payload.as_ref();
        raw.extend_from_slice(payload_bytes);
        all_elements.extend(extract_msgpack_elements(payload_bytes));
    }

    if had_error && all_elements.is_empty() && raw.is_empty() {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw,
        merged_array,
        watermark_lsn: max_lsn,
    })
}

/// Cluster-wide gather with routing awareness.
///
/// # Single-node mode
///
/// If `state.gateway` is `None` this degenerates to [`gather_all_cores`] with
/// unchanged behaviour.
///
/// # Cluster mode — single-vShard-homed sources (document, kv, columnar,
/// timeseries, spatial, vector, text)
///
/// Standard collections are *single-vShard-homed*: all rows for a collection
/// live on exactly one vShard determined by `vshard_for_collection(database_id,
/// &name)`.  The data-plane scan is **not** vshard-scoped, so broadcasting the
/// plan to every vShard via `Exchange{Gather}` causes the owning node to return
/// the full collection once per route that lands on it — 1 024× duplication.
///
/// For these sources the bare plan is routed through the gateway's normal
/// `route_plan` `other` arm, which sends it directly to the single owning
/// vShard (local or remote) and returns exactly the right rows.
///
/// # Cluster mode — cluster-partitioned sources (graph traversal, array)
///
/// Graph traversal ops and Array ops distribute data across vShards by node-id
/// or tile-id.  Cross-node gather for these sources requires a dedicated
/// scatter-gather path that does not yet exist.  To avoid producing wrong
/// results we fall back to the local `gather_all_cores` path.
///
/// TRACKED DEBT: cross-node gather for genuinely vShard-partitioned sources
/// (graph traversal / array) needs its own broadcast + vshard-scoped path.
/// The Exchange{Gather} broadcast approach is NOT correct for single-vShard-
/// homed collections and must not be reinstated for them.
pub async fn gather_all_vshards(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<GatherOutcome> {
    let Some(gateway) = state.gateway.as_ref() else {
        // Single-node: delegate to the local fan-out path unchanged.
        return gather_all_cores(state, tenant_id, database_id, plan, trace_id).await;
    };

    if nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan) {
        // Graph node-id / array tile partitioning: cross-node gather via this
        // primitive is NOT yet correct (these engines have dedicated scatter-
        // gather paths). Fall back to the prior local fan to avoid introducing
        // wrong results.
        // TRACKED DEBT: cross-node gather for genuinely vShard-partitioned
        // sources (graph traversal / array) needs its own broadcast +
        // vshard-scoped path. Do not replace this fallback with Exchange{Gather}
        // broadcasting — that path is only correct for single-vShard-homed
        // collections.
        return gather_all_cores(state, tenant_id, database_id, plan, trace_id).await;
    }

    // Single-vShard-homed source (document/kv/columnar/ts/spatial/vector/text):
    // the whole collection lives on ONE vShard. Route the BARE plan through the
    // gateway so route_plan's `other` arm sends it to that single owning vShard
    // (local or remote). Do NOT wrap in Exchange{Gather} — broadcasting would
    // duplicate rows because the data-plane scan is not vshard-scoped.
    let ctx = QueryContext {
        tenant_id,
        trace_id,
        database_id,
    };

    // `Box::pin` breaks an async-fn recursion cycle: the gateway dispatches the
    // plan through `dispatch_to_data_plane`, which re-enters
    // `resolve_exchange_in_plan` → `resolve_exchange` → here. The cycle
    // terminates at runtime (the plan is Exchange-free, so the re-entrant
    // resolve is a no-op), but the future must be heap-indirected so its size
    // is finite.
    let payloads: Vec<Vec<u8>> =
        Box::pin(gateway.execute(&ctx, plan))
            .await
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("cross-node gather via gateway: {e}"),
            })?;

    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut raw = Vec::new();
    for payload in &payloads {
        raw.extend_from_slice(payload);
        all_elements.extend(extract_msgpack_elements(payload));
    }

    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw,
        merged_array,
        // KNOWN LIMITATION: cross-node gather does not yet thread per-shard
        // watermark LSNs back through the gateway response, so Strong-consistency
        // LSN fencing degrades to pass-through on this path.  This is consistent
        // with existing gateway behavior (gateway.execute returns no LSN metadata).
        // Tracked as a follow-up: propagate watermark_lsn through GatewayResponse.
        watermark_lsn: Lsn::ZERO,
    })
}

/// Build the final aggregate payload for `Gather{as_aggregate: true}` plans.
///
/// Runs Arrow SIMD post-processing on the merged msgpack rows.  Returns the
/// merged array unchanged — the Arrow pass validates the merge and logs schema
/// information for observability; the payload itself is already in its final
/// form after the per-core partial-aggregate merge.
pub fn finalize_aggregate(merged_array: &[u8]) -> Vec<u8> {
    if let Some(batch) = arrow_convert::msgpack_rows_to_record_batch(merged_array) {
        tracing::trace!(
            rows = batch.num_rows(),
            columns = batch.num_columns(),
            "arrow aggregate post-processing: merged {} rows",
            batch.num_rows(),
        );
    }
    merged_array.to_vec()
}

/// Build a synthetic successful Response from a gathered merged-array payload.
pub(super) fn outcome_to_response(merged_array: Vec<u8>, watermark_lsn: Lsn) -> Response {
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(merged_array),
        watermark_lsn,
        error_code: None,
    }
}

/// Extract individual msgpack elements from a msgpack array payload.
///
/// If the payload is not a valid msgpack array, it is returned as a single
/// element with a warning logged.
fn extract_msgpack_elements(payload: &[u8]) -> Vec<Vec<u8>> {
    if payload.is_empty() {
        return Vec::new();
    }

    let Some((count, mut pos)) = msgpack_scan::array_header(payload, 0) else {
        tracing::warn!(
            payload_len = payload.len(),
            "gather: payload is not a msgpack array; treating as single element"
        );
        return vec![payload.to_vec()];
    };

    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        if pos >= payload.len() {
            break;
        }
        let start = pos;
        match msgpack_scan::skip_value(payload, pos) {
            Some(next) => {
                rows.push(payload[start..next].to_vec());
                pos = next;
            }
            None => {
                tracing::warn!(
                    pos,
                    payload_len = payload.len(),
                    "gather: could not skip msgpack element; stopping early"
                );
                break;
            }
        }
    }
    rows
}

/// Encode a list of pre-extracted msgpack elements into a single msgpack array.
fn encode_msgpack_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let total_data: usize = rows.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    let n = rows.len();
    if n < 16 {
        out.push(0x90 | n as u8);
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }

    for row in rows {
        out.extend_from_slice(row);
    }
    out
}
