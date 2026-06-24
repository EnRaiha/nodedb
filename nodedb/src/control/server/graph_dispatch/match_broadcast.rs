// SPDX-License-Identifier: BUSL-1.1

//! MATCH-specific cross-core broadcast that unwraps the DP→CP `{rows, frontier}`
//! envelope.
//!
//! The Data Plane MATCH handlers (`execute_graph_match` /
//! `execute_graph_match_continuation`) encode each core's result as a 2-field
//! msgpack map:
//!
//! ```text
//! { "rows": <rows msgpack array>, "frontier": <frontier msgpack array> }
//! ```
//!
//! The generic `gather_all_cores` / `broadcast_to_all_cores` primitives treat
//! the whole payload as a BARE msgpack array of row elements, which would
//! mis-merge this map. This module mirrors `gather_all_cores`'s per-core SPSC
//! fan-out (eager dispatch → `join_all`, NotFound-tolerant) but, for each core,
//! it DECODES the envelope and:
//!
//! - merges the per-core `rows` subfields into a single bare msgpack array
//!   (the SAME shape `match_payload_to_response` already expects), and
//! - UNIONs every core's `frontier` entries into one `Vec<UnresolvedExpansion>`
//!   for cross-shard continuation dispatch (consumed in B2).
//!
//! On a fully-local CSR every core's frontier is empty, so the returned rows
//! payload is byte-identical to the prior bare-array gather and single-node
//! client behaviour is unchanged.
//!
//! This is MATCH-only: the generic gather primitives are left untouched for all
//! other plan types.

use std::time::Duration;

use futures::future::join_all;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::server::exchange::gather::eager_dispatch_to_all_cores;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::data::executor::handlers::graph_match::{
    MATCH_ENVELOPE_FRONTIER_KEY, MATCH_ENVELOPE_ROWS_KEY,
};
use crate::engine::graph::pattern::executor::UnresolvedExpansion;
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_query::msgpack_scan::reader::{map_header, read_str_advance, skip_value};

/// Result of a MATCH cross-core broadcast after envelope unwrapping.
pub struct MatchBroadcastOutcome {
    /// Merged binding rows as a single BARE msgpack array — the exact shape
    /// `match_payload_to_response` decodes (byte-identical to the prior
    /// bare-array gather for single-node / empty-frontier results).
    pub rows_payload: Payload,
    /// Union of every core's cross-shard frontier entries. Empty on a
    /// fully-local CSR. Consumed by B2 cross-shard continuation dispatch.
    pub frontier: Vec<UnresolvedExpansion>,
    /// `true` if any core returned a partial (truncated) result.
    pub partial: bool,
}

/// Locate a top-level map value by key in a msgpack map payload.
///
/// Returns the raw msgpack bytes of the value (a complete, self-contained
/// msgpack value) for the first matching key, or `None` if the payload is not
/// a map or the key is absent.
fn map_value_raw<'a>(payload: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let (count, mut pos) = map_header(payload, 0)?;
    for _ in 0..count {
        let k = read_str_advance(payload, &mut pos)?;
        let val_start = pos;
        let val_end = skip_value(payload, pos)?;
        if k == key {
            return Some(&payload[val_start..val_end]);
        }
        pos = val_end;
    }
    None
}

/// Decode one core's `{rows, frontier}` envelope into its row elements and
/// frontier entries.
///
/// Malformed bytes (not a map, missing keys, undecodable frontier) surface as a
/// typed [`crate::Error`] rather than a panic.
fn decode_match_envelope(
    payload: &[u8],
) -> crate::Result<(Vec<Vec<u8>>, Vec<UnresolvedExpansion>)> {
    let rows_bytes =
        map_value_raw(payload, MATCH_ENVELOPE_ROWS_KEY).ok_or_else(|| crate::Error::Codec {
            detail: "match envelope: missing or malformed 'rows' field".into(),
        })?;
    let frontier_bytes =
        map_value_raw(payload, MATCH_ENVELOPE_FRONTIER_KEY).ok_or_else(|| crate::Error::Codec {
            detail: "match envelope: missing or malformed 'frontier' field".into(),
        })?;

    let row_elements = extract_msgpack_elements(rows_bytes);
    let frontier: Vec<UnresolvedExpansion> =
        zerompk::from_msgpack(frontier_bytes).map_err(|e| crate::Error::Codec {
            detail: format!("match envelope: invalid frontier: {e}"),
        })?;
    Ok((row_elements, frontier))
}

/// Unwrap a SINGLE Data-Plane MATCH `{rows, frontier}` envelope payload into a
/// bare rows msgpack array plus its frontier entries.
///
/// Used by surfaces that dispatch a MATCH plan to one shard rather than
/// fanning out to all cores (e.g. the native protocol's direct-op path), so
/// they can recover the same bare rows array shape every MATCH consumer
/// expected before the envelope existed. On a single-node / empty-frontier
/// MATCH the returned rows payload is byte-identical to the prior bare-array
/// response.
///
/// Malformed bytes surface a typed [`crate::Error`], never a panic. An empty
/// payload (e.g. a successful op with no result) passes through unchanged.
pub fn unwrap_match_envelope(
    payload: &Payload,
) -> crate::Result<(Payload, Vec<UnresolvedExpansion>)> {
    if payload.is_empty() {
        return Ok((payload.clone(), Vec::new()));
    }
    let (row_elements, frontier) = decode_match_envelope(payload.as_ref())?;
    let merged_rows = encode_msgpack_array(&row_elements);
    Ok((Payload::from_vec(merged_rows), frontier))
}

/// Fan a MATCH plan to every Data-Plane core, unwrap each core's
/// `{rows, frontier}` envelope, and merge the results.
///
/// Mirrors `exchange::gather::gather_all_cores`'s eager per-core dispatch +
/// `join_all` collection and its NotFound-tolerant / partial-result error
/// handling, but unwraps the MATCH envelope per core instead of treating the
/// payload as a bare row array.
pub async fn broadcast_match_to_all_cores(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<MatchBroadcastOutcome> {
    // Shared broadcast call counter (parity with the generic gather path).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let deadline_secs = state.tuning.network.default_deadline_secs;

    // Eager dispatch: register a tracker receiver and dispatch to each core
    // BEFORE awaiting any response, matching gather_all_cores' true-parallelism
    // prologue.
    let receivers =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, |_| plan.clone())?;

    // Await all cores in parallel, draining the full bounded response per core
    // (a core's result may stream as several Partial frames before its terminal
    // frame).
    let deadline = Duration::from_secs(deadline_secs);
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let response_futures = receivers.into_iter().map(|(core_id, mut rx)| async move {
        match tokio::time::timeout(
            deadline,
            crate::control::server::dispatch_utils::collect_bounded_response(
                &mut rx,
                max_result_bytes,
            ),
        )
        .await
        .map_err(|_| crate::Error::Dispatch {
            detail: format!("match gather timeout on core {core_id}"),
        })? {
            Ok(resp) => Ok(resp),
            Err(crate::control::server::dispatch_utils::DispatchCollectError::OverBudget {
                bytes,
            }) => Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "match gather on core {core_id} exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            }),
            Err(crate::control::server::dispatch_utils::DispatchCollectError::ChannelClosed) => {
                Err(crate::Error::Dispatch {
                    detail: format!("match gather channel closed on core {core_id}"),
                })
            }
        }
    });

    let results: Vec<crate::Result<Response>> = join_all(response_futures).await;

    let mut all_row_elements: Vec<Vec<u8>> = Vec::new();
    let mut frontier: Vec<UnresolvedExpansion> = Vec::new();
    let mut partial = false;
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

        if resp.partial {
            partial = true;
        }

        if resp.payload.is_empty() {
            continue;
        }

        let (mut rows, mut core_frontier) = decode_match_envelope(resp.payload.as_ref())?;
        all_row_elements.append(&mut rows);
        frontier.append(&mut core_frontier);
    }

    if had_error && all_row_elements.is_empty() {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    let merged_rows = encode_msgpack_array(&all_row_elements);

    Ok(MatchBroadcastOutcome {
        rows_payload: Payload::from_vec(merged_rows),
        frontier,
        partial,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::handlers::graph_match::encode_match_envelope;
    use crate::engine::graph::pattern::executor::{BindingRow, UnresolvedExpansion};

    fn row(pairs: &[(&str, &str)]) -> BindingRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Round-trip the `{rows, frontier}` envelope: encode in the handler shape,
    /// decode in the broadcast unwrap path, assert rows AND frontier survive.
    #[test]
    fn envelope_round_trips_rows_and_frontier() {
        let rows = vec![row(&[("a", "alice"), ("b", "bob")]), row(&[("a", "carol")])];
        let frontier = vec![UnresolvedExpansion {
            binding_var: "b".into(),
            node_name: "bob".into(),
            triple_idx: 1,
            partial_row: row(&[("a", "alice"), ("b", "bob")]),
        }];

        let payload = encode_match_envelope(&rows, &frontier).unwrap();
        let (row_elements, decoded_frontier) = decode_match_envelope(&payload).unwrap();

        // Rows preserved: 2 elements. Merging them back into a bare array
        // reproduces the exact `rows` map values embedded in the envelope —
        // compare against the SAME bytes the envelope carries (a second
        // independent `rows_to_msgpack` call could differ only in HashMap key
        // order, so we reconstruct the expected bare array from the envelope's
        // own `rows` field rather than re-serializing).
        assert_eq!(row_elements.len(), 2);
        let merged = encode_msgpack_array(&row_elements);
        let envelope_rows = map_value_raw(&payload, MATCH_ENVELOPE_ROWS_KEY).unwrap();
        assert_eq!(
            merged, envelope_rows,
            "merged rows must equal the envelope's bare rows array byte-for-byte"
        );
        // And each decoded row's bindings are intact regardless of key order.
        let decoded_json = nodedb_types::json_from_msgpack(&merged).unwrap();
        let arr = decoded_json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["a"], "alice");
        assert_eq!(arr[0]["b"], "bob");
        assert_eq!(arr[1]["a"], "carol");

        // Frontier preserved.
        assert_eq!(decoded_frontier.len(), 1);
        assert_eq!(decoded_frontier[0].node_name, "bob");
        assert_eq!(decoded_frontier[0].binding_var, "b");
        assert_eq!(decoded_frontier[0].triple_idx, 1);
        assert_eq!(
            decoded_frontier[0].partial_row.get("a").map(String::as_str),
            Some("alice")
        );
    }

    /// Empty rows + empty frontier (the empty-partition / single-node case):
    /// the merged rows payload is an empty bare msgpack array and the frontier
    /// is empty.
    #[test]
    fn envelope_round_trips_empty() {
        let payload = encode_match_envelope(&[], &[]).unwrap();
        let (row_elements, decoded_frontier) = decode_match_envelope(&payload).unwrap();
        assert!(row_elements.is_empty());
        assert!(decoded_frontier.is_empty());
        let merged = encode_msgpack_array(&row_elements);
        let expected = crate::engine::graph::pattern::executor::rows_to_msgpack(&[]).unwrap();
        assert_eq!(merged, expected);
    }

    /// Malformed bytes (not a map) surface a typed error, never a panic.
    #[test]
    fn malformed_envelope_is_typed_error() {
        // A bare msgpack array (the OLD pre-envelope shape) is not a map.
        let bogus = crate::engine::graph::pattern::executor::rows_to_msgpack(&[row(&[("a", "x")])])
            .unwrap();
        let err = decode_match_envelope(&bogus);
        assert!(
            err.is_err(),
            "bare array must not decode as an envelope map"
        );
    }
}
