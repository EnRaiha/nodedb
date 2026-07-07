// SPDX-License-Identifier: BUSL-1.1

//! `_system.audit_log` row producer.
//!
//! Materializes the durable audit log as msgpack rows. There is no SQL-level
//! pushdown here — the caller's row budget is enforced by an unconditional
//! materialize cap (`MATERIALIZE_LIMIT`) that bounds memory regardless of the
//! client query.
//!
//! Columns:
//! - `seq`          — monotonic sequence number (int8).
//! - `timestamp_us` — UTC microseconds since epoch (int8).
//! - `event`        — event discriminant name (text, e.g. "AuthSuccess").
//! - `tenant_id`    — tenant identifier, 0 if not applicable (int8).
//! - `source`       — source IP or node identifier (text).
//! - `detail`       — human-readable event detail (text).
//! - `prev_hash`    — SHA-256 hex of the previous chain entry (text).
//!
//! Permission required: `audit_log:read`, granted to `superuser` and
//! `monitor` roles. Access is enforced here before any data is read.

use std::collections::HashMap;

use nodedb_types::Value;

use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

use super::tables::collections::encode_row;

/// Upper bound on rows materialized. Independent of any client-supplied LIMIT —
/// the LIMIT is applied later by the data source consumer. This cap exists
/// only to bound memory.
const MATERIALIZE_LIMIT: usize = 100_000;

pub fn audit_log(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> crate::Result<Vec<Vec<u8>>> {
    if !identity.is_superuser && !identity.has_role(&Role::Monitor) {
        return Err(crate::Error::RejectedAuthz {
            tenant_id: identity.tenant_id,
            resource: "audit_log:read".to_string(),
        });
    }

    let mut rows: Vec<Vec<u8>> = Vec::new();

    // Merge catalog-persisted entries with the in-memory tail. The audit log
    // is flushed from memory to the catalog by a periodic background timer
    // (`SharedState::flush_audit_log`), so the in-memory log always holds the
    // most recent entries that have not yet been persisted. Reading only the
    // catalog would hide those entries from operators querying
    // `_system.audit_log` between flush ticks. Dedupe by `seq`.
    let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();

    {
        let catalog = state.credentials.catalog();
        let entries = catalog
            .load_audit_entries_ranged(1, u64::MAX, 0, u64::MAX, MATERIALIZE_LIMIT)
            .map_err(|e| crate::Error::Storage {
                engine: "catalog".to_string(),
                detail: e.to_string(),
            })?;
        for e in entries {
            if !seen.insert(e.seq) {
                continue;
            }
            let mut r: HashMap<String, Value> = HashMap::with_capacity(7);
            r.insert("seq".into(), Value::Integer(e.seq as i64));
            r.insert("timestamp_us".into(), Value::Integer(e.timestamp_us as i64));
            r.insert("event".into(), Value::String(e.event));
            r.insert(
                "tenant_id".into(),
                Value::Integer(e.tenant_id.unwrap_or(0) as i64),
            );
            r.insert("source".into(), Value::String(e.source));
            r.insert("detail".into(), Value::String(e.detail));
            r.insert(
                "prev_hash".into(),
                if e.prev_hash.is_empty() {
                    Value::Null
                } else {
                    Value::String(e.prev_hash)
                },
            );
            rows.push(encode_row(r)?);
        }
    }

    let log = match state.audit.lock() {
        Ok(l) => l,
        Err(p) => p.into_inner(),
    };
    let all = log.all();
    let skip = all.len().saturating_sub(MATERIALIZE_LIMIT);
    for entry in all.iter().skip(skip) {
        if !seen.insert(entry.seq) {
            continue;
        }
        if rows.len() >= MATERIALIZE_LIMIT {
            break;
        }
        let mut r: HashMap<String, Value> = HashMap::with_capacity(7);
        r.insert("seq".into(), Value::Integer(entry.seq as i64));
        r.insert(
            "timestamp_us".into(),
            Value::Integer(entry.timestamp_us as i64),
        );
        r.insert("event".into(), Value::String(format!("{:?}", entry.event)));
        r.insert(
            "tenant_id".into(),
            Value::Integer(entry.tenant_id.map_or(0i64, |t| t.as_u64() as i64)),
        );
        r.insert("source".into(), Value::String(entry.source.clone()));
        r.insert("detail".into(), Value::String(entry.detail.clone()));
        r.insert(
            "prev_hash".into(),
            if entry.prev_hash.is_empty() {
                Value::Null
            } else {
                Value::String(entry.prev_hash.clone())
            },
        );
        rows.push(encode_row(r)?);
    }
    Ok(rows)
}
