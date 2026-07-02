// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `SHOW CONSUMER GROUPS ON <stream>` and
//! `SHOW PARTITIONS ON <stream>` handlers.
//!
//! Ported from the pgwire `ddl::consumer_group::show` handlers. The token-based
//! syntax checks, the tenant scoping, the per-group offset counting, and the
//! per-partition buffer-scan statistics are preserved verbatim; only the result
//! construction changed from a pgwire `QueryResponse` to the protocol-neutral
//! [`DdlResult::Rows`].

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};

/// Handle `SHOW CONSUMER GROUPS ON <stream>`
pub fn show_consumer_groups(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // parts: ["SHOW", "CONSUMER", "GROUPS", "ON", "<stream>"]
    if parts.len() < 5 || !parts[3].eq_ignore_ascii_case("ON") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected SHOW CONSUMER GROUPS ON <stream>".to_string(),
        });
    }

    let stream_name = parts[4].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    let columns = vec![
        "group_name".to_string(),
        "stream".to_string(),
        "committed_partitions".to_string(),
        "owner".to_string(),
    ];

    let groups = state
        .group_registry
        .list_for_stream(tenant_id, &stream_name);

    let mut rows = Vec::with_capacity(groups.len());
    for g in &groups {
        let offsets = state
            .offset_store
            .get_all_offsets(tenant_id, &stream_name, &g.name);
        let committed_count = offsets.len();

        let mut row = Map::new();
        row.insert("group_name".to_string(), JsonValue::String(g.name.clone()));
        row.insert(
            "stream".to_string(),
            JsonValue::String(g.stream_name.clone()),
        );
        row.insert(
            "committed_partitions".to_string(),
            JsonValue::String(committed_count.to_string()),
        );
        row.insert("owner".to_string(), JsonValue::String(g.owner.clone()));
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}

/// Handle `SHOW PARTITIONS ON <stream>`
///
/// Lists all vShard partitions that have events in the stream's buffer,
/// with earliest/latest LSN for each partition.
pub fn show_partitions(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    // parts: ["SHOW", "PARTITIONS", "ON", "<stream>"]
    if parts.len() < 4 || !parts[2].eq_ignore_ascii_case("ON") {
        return Err(DdlError {
            sqlstate: "42601".to_string(),
            message: "expected SHOW PARTITIONS ON <stream>".to_string(),
        });
    }

    let stream_name = parts[3].to_lowercase();
    let tenant_id = identity.tenant_id.as_u64();

    // Get the stream's buffer from the CdcRouter.
    let buffer = state.cdc_router.get_buffer(tenant_id, &stream_name);

    let columns = vec![
        "partition_id".to_string(),
        "earliest_lsn".to_string(),
        "latest_lsn".to_string(),
        "event_count".to_string(),
    ];

    let mut rows = Vec::new();
    if let Some(buf) = buffer {
        // Scan the buffer and collect per-partition stats.
        let events = buf.read_from_lsn(0, usize::MAX);
        let mut partition_stats: std::collections::BTreeMap<u32, (u64, u64, usize)> =
            std::collections::BTreeMap::new();
        for e in &events {
            let entry = partition_stats
                .entry(e.partition)
                .or_insert((u64::MAX, 0, 0));
            entry.0 = entry.0.min(e.lsn);
            entry.1 = entry.1.max(e.lsn);
            entry.2 += 1;
        }
        for (pid, (earliest, latest, count)) in &partition_stats {
            let mut row = Map::new();
            row.insert(
                "partition_id".to_string(),
                JsonValue::String(pid.to_string()),
            );
            row.insert(
                "earliest_lsn".to_string(),
                JsonValue::String(earliest.to_string()),
            );
            row.insert(
                "latest_lsn".to_string(),
                JsonValue::String(latest.to_string()),
            );
            row.insert(
                "event_count".to_string(),
                JsonValue::String(count.to_string()),
            );
            rows.push(row);
        }
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
