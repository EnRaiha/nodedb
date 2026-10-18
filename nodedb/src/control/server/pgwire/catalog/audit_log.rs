// SPDX-License-Identifier: BUSL-1.1

//! `_system.audit_log` row producer.
//!
//! Materializes the audit log as msgpack rows. There is no SQL-level
//! pushdown here — the caller's row budget is enforced by an unconditional
//! materialize cap (`MATERIALIZE_LIMIT`) that bounds memory regardless of the
//! client query. The cap keeps the newest entries.
//!
//! Rows come from [`SharedState::recent_audit_entries`].
//! That reader merges the in-memory buffer with the catalog.
//! A scan therefore returns every entry recorded before it started.
//!
//! Columns:
//! - `seq`          — monotonic sequence number (int8).
//! - `timestamp_us` — UTC microseconds since epoch (int8).
//! - `event`        — snake_case event name (text, e.g. "auth_success").
//!   It matches the `event` column of every `SHOW AUDIT` command.
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

    let entries = state.recent_audit_entries(MATERIALIZE_LIMIT, |_| true)?;

    let mut rows: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
    for e in entries {
        let mut r: HashMap<String, Value> = HashMap::with_capacity(7);
        r.insert("seq".into(), Value::Integer(e.seq as i64));
        r.insert("timestamp_us".into(), Value::Integer(e.timestamp_us as i64));
        r.insert(
            "event".into(),
            Value::String(e.event.snake_name().to_string()),
        );
        r.insert(
            "tenant_id".into(),
            Value::Integer(e.tenant_id.map_or(0i64, |t| t.as_u64() as i64)),
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
    Ok(rows)
}
