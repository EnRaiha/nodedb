// SPDX-License-Identifier: BUSL-1.1

//! Clone CoW read-path interception for the pgwire handler.
//!
//! Called from `execute_planned_sql_inner` after planning and before dispatch.
//! For `Shadowed` / `Materializing` clones this produces an augmented task
//! list (target + source) and then merges the source response with tombstone
//! filtering applied.
//!
//! Non-cloned databases and fully `Materialized` clones return `None` —
//! zero overhead for the common path.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::clone::resolver::{
    CloneReadParams, ResolveOutcome, filter_tombstoned_rows, resolve_read,
};
use crate::control::server::pgwire::handler::plan::{PlanKind, payload_to_response};
use crate::control::server::response_shape::kv::apply_kv_wrap;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Intercept read tasks for cloned collections.
    ///
    /// Returns `Some(responses)` when clone resolution handled the dispatch
    /// completely — the caller should return that directly.
    ///
    /// Returns `None` when the tasks do not target a cloned collection —
    /// the caller should continue with normal dispatch.
    pub(super) async fn maybe_dispatch_clone_reads(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Option<Vec<Response>>> {
        // Compute query LSN and wall-ms for the resolver.
        //
        // If the first task carries a `system_as_of_ms` (i.e. the query was
        // written with `FOR SYSTEM_TIME AS OF <ms>`), derive query_lsn from
        // that wall-clock time so the clone predation check works correctly.
        // Otherwise fall back to the current WAL LSN (normal reads).
        let (query_lsn, query_ms) =
            if let Some(as_of_ms) = extract_system_as_of_ms(tasks.first().map(|t| &t.plan)) {
                let lsn = self.state.ms_to_lsn(as_of_ms);
                (lsn, Some(as_of_ms))
            } else {
                let lsn = self.state.wal.next_lsn();
                let ms = self.state.ms_to_lsn_inverse(lsn);
                (lsn, ms)
            };

        let params = CloneReadParams {
            query_lsn,
            query_ms,
        };

        let outcome = resolve_read(&self.state, tasks, tenant_id, &params).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        match outcome {
            None => Ok(None),

            Some(ResolveOutcome::Passthrough(_tasks)) => {
                // Fully materialized — let the normal dispatch path handle it.
                Ok(None)
            }

            Some(ResolveOutcome::PreDatesClone(note)) => {
                // Query time predates the clone's creation — return empty.
                tracing::debug!(
                    message = note.message,
                    query_lsn = %note.query_lsn,
                    clone_created_at = %note.clone_created_at,
                    "clone read predates clone creation — returning empty result"
                );
                let empty: Vec<u8> =
                    nodedb_types::json_to_msgpack(&serde_json::json!([])).unwrap_or_default();
                let shaped = payload_to_response(&empty, PlanKind::MultiRow);
                if let Some(notice) = shaped.notice {
                    self.sessions.push_notice(addr, notice);
                }
                Ok(Some(vec![shaped.response]))
            }

            Some(ResolveOutcome::Augmented {
                tasks,
                source_start_idx,
                origin: _,
                target_collection_key,
                note,
            }) => {
                if let Some(note) = note {
                    tracing::debug!(
                        message = note.message,
                        "clone read: T_lsn < clone_created_at (note attached)"
                    );
                }

                // Split tasks into target and source halves.
                let (target_tasks, source_tasks) = tasks.split_at(source_start_idx);

                // Dispatch target tasks (these are the primary tasks).
                let mut responses = Vec::with_capacity(target_tasks.len());
                for task in target_tasks {
                    let resp = self
                        .dispatch_task(task.clone(), None, None)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;
                    responses.push(resp);
                }

                // Load tombstones for source row filtering.
                let tombstoned = {
                    let catalog_arc = self.state.credentials.catalog();
                    match catalog_arc.as_ref() {
                        Some(catalog) => catalog
                            .list_clone_tombstones(&target_collection_key)
                            .map_err(|e| {
                                let (severity, code, message) = error_to_sqlstate(&e);
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    severity.to_owned(),
                                    code.to_owned(),
                                    message,
                                )))
                            })?,
                        None => std::collections::HashSet::new(),
                    }
                };

                // Load KV tombstones for KV-engine key-based filtering.
                let kv_tombstoned = {
                    let catalog_arc = self.state.credentials.catalog();
                    match catalog_arc.as_ref() {
                        Some(catalog) => catalog
                            .list_kv_clone_tombstones(&target_collection_key)
                            .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?,
                        None => std::collections::HashSet::new(),
                    }
                };

                // Dispatch source tasks, filter tombstoned rows, merge into target responses.
                //
                // For a single-level clone there is one source task per target task.
                // For a multi-level clone chain the resolver emits one source task per
                // ancestor level per original target task, so source_tasks.len() may be
                // a multiple of target_tasks.len().  All source tasks that correspond to
                // the same original query task (index = source_idx % target_tasks.len())
                // must be merged into the same target response slot.
                let target_count = target_tasks.len().max(1);
                for (source_idx, source_task) in source_tasks.iter().enumerate() {
                    let response_idx = source_idx % target_count;
                    let source_resp = self
                        .dispatch_task(source_task.clone(), None, None)
                        .await
                        .map_err(|e| {
                            let (severity, code, message) = error_to_sqlstate(&e);
                            PgWireError::UserError(Box::new(ErrorInfo::new(
                                severity.to_owned(),
                                code.to_owned(),
                                message,
                            )))
                        })?;

                    // For KvOp::Get: inject the primary key field into the raw map response
                    // so that projection and column-name assertions work correctly.
                    let normalized_payload =
                        apply_kv_wrap(&source_task.plan, source_resp.payload.as_ref());

                    // KvOp::Get responses arrive as a single msgpack map (not an array).
                    // Normalize to a 1-element array so tombstone filters and merge work
                    // uniformly across scan and point-get shapes.
                    let normalized_payload = wrap_single_map_as_array(normalized_payload);

                    // Apply surrogate tombstone filter (document engine rows).
                    // `filter_tombstoned_rows` returns `None` only when its input
                    // is not a well-formed msgpack array. Post-normalization
                    // (`wrap_single_map_as_array`) the input is guaranteed to be
                    // an empty slice or a valid array, so `None` here signals
                    // upstream corruption — log loudly and pass through unchanged
                    // rather than masking with `unwrap_or`.
                    let source_payload = match filter_tombstoned_rows(
                        &normalized_payload,
                        &tombstoned,
                    ) {
                        Some(p) => p,
                        None => {
                            tracing::warn!(
                                payload_len = normalized_payload.len(),
                                "clone read: filter_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                            );
                            normalized_payload
                        }
                    };

                    // Apply KV key tombstone filter (KV engine rows).
                    let source_payload = if !kv_tombstoned.is_empty() {
                        match filter_kv_tombstoned_rows(&source_payload, &kv_tombstoned) {
                            Some(p) => p,
                            None => {
                                tracing::warn!(
                                    payload_len = source_payload.len(),
                                    "clone read: filter_kv_tombstoned_rows received non-array msgpack payload after normalization — passing through unfiltered"
                                );
                                source_payload
                            }
                        }
                    } else {
                        source_payload
                    };

                    // Merge source rows into the corresponding target response.
                    // `response_idx` maps multi-level ancestor tasks back to the
                    // original query-task slot they serve.
                    if response_idx < responses.len() {
                        // Normalize target payload to array shape for uniform merge.
                        let target_payload = wrap_single_map_as_array(
                            responses[response_idx].payload.as_ref().to_vec(),
                        );
                        let merged = merge_msgpack_arrays(&target_payload, &source_payload)
                            .map_err(|e| {
                                let (severity, code, message) = error_to_sqlstate(&e);
                                PgWireError::UserError(Box::new(ErrorInfo::new(
                                    severity.to_owned(),
                                    code.to_owned(),
                                    message,
                                )))
                            })?;
                        responses[response_idx] = crate::bridge::envelope::Response {
                            payload: merged.into(),
                            ..responses[response_idx].clone()
                        };
                    } else {
                        // More source tasks than target tasks — append standalone.
                        responses.push(crate::bridge::envelope::Response {
                            payload: source_payload.into(),
                            ..source_resp
                        });
                    }
                }

                // Convert raw Response objects to pgwire Responses.
                let mut pg_responses = Vec::with_capacity(responses.len());
                for resp in responses {
                    let shaped = payload_to_response(resp.payload.as_ref(), PlanKind::MultiRow);
                    if let Some(notice) = shaped.notice {
                        self.sessions.push_notice(addr, notice);
                    }
                    pg_responses.push(shaped.response);
                }

                Ok(Some(pg_responses))
            }
        }
    }
}

/// If `payload` is a msgpack map (a single row from a point-get), wrap it as a
/// 1-element msgpack array so that tombstone filters and `merge_msgpack_arrays`
/// operate on a uniform array shape.  If `payload` is already an array (from a
/// scan) or is empty, return it unchanged.
fn wrap_single_map_as_array(payload: Vec<u8>) -> Vec<u8> {
    use nodedb_query::msgpack_scan;
    if payload.is_empty() {
        return payload;
    }
    // Already an array — leave as-is.
    if msgpack_scan::array_header(&payload, 0).is_some() {
        return payload;
    }
    // Single map row: wrap as fixarray(1) + map bytes.
    let mut buf = Vec::with_capacity(1 + payload.len());
    buf.push(0x91); // fixarray with 1 element
    buf.extend_from_slice(&payload);
    buf
}

/// Merge two msgpack arrays into one by concatenating their elements.
///
/// If either slice is empty, returns the other unchanged. Both inputs MUST
/// have been passed through `wrap_single_map_as_array` first; if either
/// non-empty input is not a valid msgpack array header an error is returned
/// — silently re-encoding a bogus header on top of the concatenated bytes
/// would corrupt downstream parsers in a way no caller could detect.
fn merge_msgpack_arrays(a: &[u8], b: &[u8]) -> crate::Result<Vec<u8>> {
    use nodedb_query::msgpack_scan;

    if a.is_empty() {
        return Ok(b.to_vec());
    }
    if b.is_empty() {
        return Ok(a.to_vec());
    }

    let (count_a, body_a_start) = msgpack_scan::array_header(a, 0).ok_or_else(|| {
        crate::Error::Storage {
            engine: "clone_merge".into(),
            detail: format!(
                "merge_msgpack_arrays: left input is not a msgpack array (len={}, first_byte=0x{:02x})",
                a.len(),
                a.first().copied().unwrap_or(0)
            ),
        }
    })?;
    let (count_b, body_b_start) = msgpack_scan::array_header(b, 0).ok_or_else(|| {
        crate::Error::Storage {
            engine: "clone_merge".into(),
            detail: format!(
                "merge_msgpack_arrays: right input is not a msgpack array (len={}, first_byte=0x{:02x})",
                b.len(),
                b.first().copied().unwrap_or(0)
            ),
        }
    })?;
    let total = count_a + count_b;
    let body_a = &a[body_a_start..];
    let body_b = &b[body_b_start..];

    let mut buf = Vec::with_capacity(5 + body_a.len() + body_b.len());

    // Write array header for `total`.
    if total <= 15 {
        buf.push(0x90 | (total as u8));
    } else if total <= 0xFFFF {
        buf.push(0xdc);
        buf.push((total >> 8) as u8);
        buf.push(total as u8);
    } else {
        buf.push(0xdd);
        buf.push((total >> 24) as u8);
        buf.push((total >> 16) as u8);
        buf.push((total >> 8) as u8);
        buf.push(total as u8);
    }

    buf.extend_from_slice(body_a);
    buf.extend_from_slice(body_b);
    Ok(buf)
}

/// Filter a msgpack array of KV rows, removing any whose `"key"` field is in
/// `tombstoned`.
///
/// KV scan responses are msgpack arrays of maps.  Each row map may have a `"key"`
/// field (injected by `apply_kv_wrap` for point-get responses, or
/// already present for typed KV scans).  Rows whose `"key"` value is in the
/// tombstoned set are excluded from the result.
///
/// Returns `None` when the input is not a well-formed msgpack array (caller
/// falls back to the original slice unchanged).  Returns `Some(bytes)` — which
/// may be a shorter array or the original bytes when nothing was filtered.
fn filter_kv_tombstoned_rows(
    payload: &[u8],
    tombstoned: &std::collections::HashSet<String>,
) -> Option<Vec<u8>> {
    use nodedb_query::msgpack_scan;

    if tombstoned.is_empty() || payload.is_empty() {
        return Some(payload.to_vec());
    }

    // Callers (clone_dispatch read path) MUST pass a payload that has been
    // through `wrap_single_map_as_array`, so the input is guaranteed to be
    // a valid msgpack array. Non-array input here means the upstream
    // normalization or the Data Plane response shape changed — return None
    // so the caller logs and degrades safely instead of silently producing
    // wrong tombstone behaviour for a single-map shape we no longer expect.
    let (count, body_start) = msgpack_scan::array_header(payload, 0)?;

    // Walk elements, collecting start/end byte offsets for rows to keep.
    let mut kept_ranges: Vec<(usize, usize)> = Vec::with_capacity(count);
    let mut pos = body_start;
    for _ in 0..count {
        let row_start = pos;
        // Advance `pos` by one msgpack value (the row map).
        pos = msgpack_scan::skip_value(payload, pos)?;
        let row_bytes = &payload[row_start..pos];

        // Extract the "key" field from this row map. A KV row without a
        // "key" field is a protocol contract violation (every KV scan/point-get
        // response is expected to carry a key after `apply_kv_wrap`
        // normalization). Log a warn and treat as not-tombstoned so we err on
        // the side of returning the row to the user — silent drop would be
        // worse than silent include.
        let extracted_key = msgpack_scan::extract_field(row_bytes, 0, "key")
            .and_then(|(start, _)| msgpack_scan::read_str(row_bytes, start));
        let is_tombstoned = match extracted_key {
            Some(k) => tombstoned.contains(k),
            None => {
                tracing::warn!(
                    row_len = row_bytes.len(),
                    "clone read: KV row in source response has no `key` field; including unfiltered (protocol contract violation upstream)"
                );
                false
            }
        };

        if !is_tombstoned {
            kept_ranges.push((row_start, pos));
        }
    }

    if kept_ranges.len() == count {
        // Nothing was filtered.
        return Some(payload.to_vec());
    }

    let kept = kept_ranges.len();
    let mut buf = Vec::with_capacity(payload.len());
    if kept <= 15 {
        buf.push(0x90 | (kept as u8));
    } else if kept <= 0xFFFF {
        buf.push(0xdc);
        buf.push((kept >> 8) as u8);
        buf.push(kept as u8);
    } else {
        buf.push(0xdd);
        buf.push((kept >> 24) as u8);
        buf.push((kept >> 16) as u8);
        buf.push((kept >> 8) as u8);
        buf.push(kept as u8);
    }
    for (start, end) in kept_ranges {
        buf.extend_from_slice(&payload[start..end]);
    }
    Some(buf)
}

/// Extract the `system_as_of_ms` value from a physical plan, if present.
///
/// Used to derive the clone predation query LSN when the SQL query carries
/// `FOR SYSTEM_TIME AS OF <ms>`.  Returns `None` for plan types that do not
/// carry a temporal qualifier (KV, DDL, writes, etc.).
fn extract_system_as_of_ms(
    plan: Option<&nodedb_physical::physical_plan::PhysicalPlan>,
) -> Option<i64> {
    use nodedb_physical::physical_plan::PhysicalPlan;
    // Exhaustive match — adding a new top-level engine MUST require an
    // explicit decision here about how `FOR SYSTEM_TIME AS OF` is plumbed
    // (or that it is intentionally unsupported on that engine). A
    // catch-all `_ =>` would silently let new temporal-capable plan
    // variants be ignored by the clone predation check.
    match plan? {
        PhysicalPlan::Document(op) => extract_doc_as_of(op),
        PhysicalPlan::Columnar(op) => extract_columnar_as_of(op),
        PhysicalPlan::Timeseries(op) => extract_timeseries_as_of(op),
        // Index-only / overlay engines (Vector, Text, Spatial, Graph) and
        // engines that do not currently carry a `system_as_of_ms` qualifier
        // on their plan variants (Kv, Crdt, Query, Meta, Array,
        // ClusterArray) are explicitly None. Bitemporal queries against
        // these go through composition with a data-bearing collection;
        // when that changes, add a branch here rather than relaxing this
        // match.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_) => None,
    }
}

fn extract_doc_as_of(op: &nodedb_physical::physical_plan::DocumentOp) -> Option<i64> {
    use nodedb_physical::physical_plan::DocumentOp;
    match op {
        DocumentOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_columnar_as_of(op: &nodedb_physical::physical_plan::ColumnarOp) -> Option<i64> {
    use nodedb_physical::physical_plan::ColumnarOp;
    match op {
        ColumnarOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}

fn extract_timeseries_as_of(op: &nodedb_physical::physical_plan::TimeseriesOp) -> Option<i64> {
    use nodedb_physical::physical_plan::TimeseriesOp;
    match op {
        TimeseriesOp::Scan { system_time, .. } => system_time.as_of_ms(),
        _ => None,
    }
}
