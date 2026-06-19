// SPDX-License-Identifier: BUSL-1.1

//! `AS OF SYSTEM TIME NULL` (audit-log) scan handler. Emits every
//! system-time version of every matching document, ordered ascending by
//! system time, with the system-time column (`_ts_system`) projected into
//! each output row.

use tracing::debug;

use super::projection::apply_projection_msgpack;
use super::scan_params::VersionedScanParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Output column name carrying the system-time of each version.
const SYSTEM_TIME_COLUMN: &str = "_ts_system";

impl CoreLoop {
    pub(in crate::data::executor) fn execute_document_scan_all_versions(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: VersionedScanParams<'_>,
    ) -> Response {
        let VersionedScanParams {
            collection,
            limit,
            offset,
            filters,
            projection,
            valid_at_ms,
        } = params;

        debug!(
            core = self.core_id,
            %collection,
            limit,
            offset,
            ?valid_at_ms,
            "document scan (all versions / audit log)"
        );

        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(f) => f,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("malformed scan filters: {e}"),
                        },
                    );
                }
            }
        };

        // Push the scan filters down into the engine so the `limit` truncation
        // counts only matching versions. Filtering the truncated result here
        // (as the old fetch-then-filter heuristic did) silently under-returned
        // when the filter was selective. The system-time column is injected
        // after the scan so it always appears in output regardless of filters.
        let predicate = |body: &[u8]| filter_predicates.iter().all(|f| f.matches_binary(body));
        // The engine returns the oldest `offset + limit` matching versions in
        // ascending system-time order; we then page within that window.
        let scan_limit = offset.saturating_add(limit);
        let rows = match self.sparse.versioned_scan_all(
            task.request.database_id.as_u64(),
            tid,
            collection,
            valid_at_ms,
            scan_limit,
            &predicate,
        ) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // A no-LIMIT audit-log scan shares the `usize::MAX` limit; bound its
        // materialized result by the scan memory budget and surface a
        // deterministic error rather than silently truncating.
        if limit == usize::MAX
            && crate::data::executor::handlers::scan_budget::version_bytes_exceeded(
                &rows,
                self.query_tuning.max_scan_result_bytes,
            )
        {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let sliced = rows.into_iter().skip(offset).take(limit);

        let mut out: Vec<(String, Vec<u8>)> = Vec::new();
        for (doc_id, sys_from_ms, body) in sliced {
            let projected = if projection.is_empty() {
                body
            } else {
                apply_projection_msgpack(&body, &[], projection)
            };
            let with_ts = match inject_system_time(&projected, sys_from_ms) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("inject _ts_system: {e}"),
                        },
                    );
                }
            };
            out.push((doc_id, with_ts));
        }

        self.send_document_rows_raw(task, &out, 1024)
    }
}

/// Decode the MessagePack document body, insert/overwrite the `_ts_system`
/// field with `sys_from_ms`, and re-encode. Non-object bodies are wrapped in
/// a fresh object carrying only the system-time column.
fn inject_system_time(body: &[u8], sys_from_ms: i64) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;
    let value =
        nodedb_types::value_from_msgpack(body).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        _ => std::collections::HashMap::new(),
    };
    obj.insert(SYSTEM_TIME_COLUMN.to_string(), Value::Integer(sys_from_ms));
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode document body with {SYSTEM_TIME_COLUMN}: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;

    fn obj(pairs: &[(&str, Value)]) -> Vec<u8> {
        let mut m = std::collections::HashMap::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v.clone());
        }
        nodedb_types::value_to_msgpack(&Value::Object(m)).expect("encode object body")
    }

    fn decode(bytes: &[u8]) -> std::collections::HashMap<String, Value> {
        match nodedb_types::value_from_msgpack(bytes).expect("decode") {
            Value::Object(m) => m,
            other => panic!("expected object, got {other:?}"),
        }
    }

    #[test]
    fn inject_adds_system_time_and_preserves_body_fields() {
        let body = obj(&[
            ("v", Value::Integer(1)),
            ("name", Value::String("alice".into())),
        ]);
        let out = inject_system_time(&body, 1_700_000_000_123).unwrap();
        let m = decode(&out);
        assert_eq!(m.get("v"), Some(&Value::Integer(1)));
        assert_eq!(m.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(
            m.get(SYSTEM_TIME_COLUMN),
            Some(&Value::Integer(1_700_000_000_123))
        );
    }

    #[test]
    fn inject_overwrites_any_preexisting_system_time_column() {
        // A document that happens to carry a `_ts_system` field of its own must
        // not shadow the version's true system time in the audit-log output.
        let body = obj(&[
            (SYSTEM_TIME_COLUMN, Value::Integer(-1)),
            ("v", Value::Integer(2)),
        ]);
        let out = inject_system_time(&body, 999).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(SYSTEM_TIME_COLUMN), Some(&Value::Integer(999)));
        assert_eq!(m.get("v"), Some(&Value::Integer(2)));
    }

    #[test]
    fn inject_wraps_non_object_body_in_fresh_object() {
        let body = nodedb_types::value_to_msgpack(&Value::Integer(42)).unwrap();
        let out = inject_system_time(&body, 7).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(SYSTEM_TIME_COLUMN), Some(&Value::Integer(7)));
        assert_eq!(
            m.len(),
            1,
            "non-object body yields a fresh object carrying only the system-time column"
        );
    }
}
