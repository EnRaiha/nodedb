// SPDX-License-Identifier: BUSL-1.1

//! `AS OF SYSTEM TIME NULL` (audit-log) scan handler. Emits every
//! system-time version of every matching document, ordered ascending by
//! system time, with the three synthetic user-facing temporal columns
//! (`_ts_system`, `_ts_valid_from`, `_ts_valid_until`) projected into each
//! output row from the version's real stored temporal coordinates. Uniform
//! across both Document engines and columnar/timeseries.

use tracing::debug;

use super::projection::apply_projection_msgpack;
use super::scan_params::VersionedScanParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::strict_format;
use crate::data::executor::task::ExecutionTask;
use nodedb_types::columnar::schema::{
    BITEMPORAL_RESERVED_COLUMNS, StrictSchema, TS_SYSTEM, TS_VALID_FROM, TS_VALID_UNTIL,
};

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

        // Strict collections store the row body as a Binary Tuple, not
        // msgpack; resolve the schema so it can be decoded before any
        // msgpack-shaped operation (projection, system-time injection) runs
        // on it. Mirrors the lookup in `scan.rs`'s current-version scan.
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let strict_schema: Option<StrictSchema> = self.doc_configs.get(&config_key).and_then(|c| {
            if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
                c.storage_mode
            {
                Some(schema.clone())
            } else {
                None
            }
        });

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
        for row in sliced {
            let msgpack_body = match &strict_schema {
                Some(schema) => match strict_audit_body(&row.body, schema) {
                    Ok(b) => b,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("decode strict audit-log body: {e}"),
                            },
                        );
                    }
                },
                None => row.body,
            };
            let projected = if projection.is_empty() {
                msgpack_body
            } else {
                apply_projection_msgpack(&msgpack_body, &[], projection)
            };
            let with_ts = match inject_temporal_columns(
                &projected,
                row.system_from_ms,
                row.valid_from_ms,
                row.valid_until_ms,
            ) {
                Ok(b) => b,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("inject temporal columns: {e}"),
                        },
                    );
                }
            };
            out.push((row.doc_id, with_ts));
        }

        self.send_document_rows_raw(task, &out, 1024)
    }
}

/// Decode a strict row's Binary Tuple `body` into MessagePack via the
/// collection's schema, then strip the reserved bitemporal bookkeeping
/// columns (`__system_from_ms`, `__valid_from_ms`, `__valid_until_ms`) so the
/// audit-log output shape stays identical to the schemaless path: user
/// columns plus the synthetic temporal triple (injected by the caller via
/// `inject_temporal_columns`). The reserved strict-tuple slots are stripped
/// here; the authoritative valid-time is taken from the row's stored envelope
/// (carried on `VersionedRow`), not from these slots, so both Document engines
/// surface identical temporal columns.
fn strict_audit_body(body: &[u8], schema: &StrictSchema) -> crate::Result<Vec<u8>> {
    use nodedb_types::Value;

    let msgpack = strict_format::binary_tuple_to_msgpack(body, schema).ok_or_else(|| {
        crate::Error::Serialization {
            format: "binary-tuple".into(),
            detail: "decode strict document body for audit-log scan".into(),
        }
    })?;
    let value =
        nodedb_types::value_from_msgpack(&msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("decode strict document body for audit-log scan: {e}"),
        })?;
    let mut obj = match value {
        Value::Object(map) => map,
        other => {
            return Err(crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("strict audit-log body decoded to non-object value: {other:?}"),
            });
        }
    };
    for reserved in BITEMPORAL_RESERVED_COLUMNS {
        obj.remove(reserved);
    }
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode stripped strict audit-log body: {e}"),
    })
}

/// Decode the MessagePack document body, insert/overwrite the three synthetic
/// user-facing audit temporal columns (`_ts_system`, `_ts_valid_from`,
/// `_ts_valid_until`) from the version's real stored temporal coordinates, and
/// re-encode. Valid-time is surfaced raw — `i64::MIN` / `i64::MAX` sentinels
/// mean "unbounded" (matching how columnar/timeseries emit their real Int64
/// temporal columns). Non-object bodies are wrapped in a fresh object carrying
/// only the temporal columns. The triple is uniform across both Document
/// engines and columnar/timeseries.
fn inject_temporal_columns(
    body: &[u8],
    system_from_ms: i64,
    valid_from_ms: i64,
    valid_until_ms: i64,
) -> crate::Result<Vec<u8>> {
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
    obj.insert(TS_SYSTEM.to_string(), Value::Integer(system_from_ms));
    obj.insert(TS_VALID_FROM.to_string(), Value::Integer(valid_from_ms));
    obj.insert(TS_VALID_UNTIL.to_string(), Value::Integer(valid_until_ms));
    nodedb_types::value_to_msgpack(&Value::Object(obj)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("re-encode document body with audit temporal columns: {e}"),
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
    fn inject_adds_temporal_columns_and_preserves_body_fields() {
        let body = obj(&[
            ("v", Value::Integer(1)),
            ("name", Value::String("alice".into())),
        ]);
        let out = inject_temporal_columns(&body, 1_700_000_000_123, 10, 20).unwrap();
        let m = decode(&out);
        assert_eq!(m.get("v"), Some(&Value::Integer(1)));
        assert_eq!(m.get("name"), Some(&Value::String("alice".into())));
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(1_700_000_000_123)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(10)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(20)));
    }

    #[test]
    fn inject_overwrites_any_preexisting_temporal_columns() {
        // A document that happens to carry temporal fields of its own must not
        // shadow the version's true temporal coordinates in the audit output.
        let body = obj(&[
            (TS_SYSTEM, Value::Integer(-1)),
            (TS_VALID_FROM, Value::Integer(-2)),
            (TS_VALID_UNTIL, Value::Integer(-3)),
            ("v", Value::Integer(2)),
        ]);
        let out = inject_temporal_columns(&body, 999, 111, 222).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(999)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(111)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(222)));
        assert_eq!(m.get("v"), Some(&Value::Integer(2)));
    }

    #[test]
    fn inject_surfaces_unbounded_valid_time_sentinels() {
        let body = obj(&[("v", Value::Integer(1))]);
        let out = inject_temporal_columns(&body, 5, i64::MIN, i64::MAX).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(i64::MIN)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(i64::MAX)));
    }

    #[test]
    fn inject_wraps_non_object_body_in_fresh_object() {
        let body = nodedb_types::value_to_msgpack(&Value::Integer(42)).unwrap();
        let out = inject_temporal_columns(&body, 7, 8, 9).unwrap();
        let m = decode(&out);
        assert_eq!(m.get(TS_SYSTEM), Some(&Value::Integer(7)));
        assert_eq!(m.get(TS_VALID_FROM), Some(&Value::Integer(8)));
        assert_eq!(m.get(TS_VALID_UNTIL), Some(&Value::Integer(9)));
        assert_eq!(
            m.len(),
            3,
            "non-object body yields a fresh object carrying only the temporal columns"
        );
    }
}
