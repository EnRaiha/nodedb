// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral administrative observability `SHOW` commands: server-wide
//! counters, per-engine query stats, and per-engine memory budgets.
//!
//! `SHOW STATS` and `SHOW SERVER STATS` expose the same underlying
//! `SystemMetrics` counters used by the Prometheus `/metrics` endpoint
//! and the OTLP exporter — without forcing administrators to leave the
//! session for a side-channel HTTP probe.
//!
//! `SHOW METRICS` is a `(key, value)` projection of the same source,
//! suitable for grep-and-pipe inspection.
//!
//! `SHOW MEMORY` reports per-engine memory budgets and current
//! utilisation from `nodedb_mem::MemoryGovernor`.
//!
//! Ported from the pgwire `ddl::observability` handlers. The metric source
//! reads, ordering, and the tenant-admin gate are preserved verbatim; only the
//! result construction changed from pgwire `Response` / `QueryResponse` to the
//! protocol-neutral `DdlResult` over `ShapedRows`.

use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::result::{DdlError, DdlResult};
use super::auth_support::require_tenant_admin;

/// Render an `(name, value)` schema and emit one row per `(name, value)`
/// pair. Both columns are TEXT — the consumer interprets numbers.
fn key_value_result(rows_in: Vec<(String, String)>) -> Result<Vec<DdlResult>, DdlError> {
    let mut rows = Vec::with_capacity(rows_in.len());
    for (k, v) in rows_in {
        let mut row = Map::new();
        row.insert("name".to_string(), JsonValue::String(k));
        row.insert("value".to_string(), JsonValue::String(v));
        rows.push(row);
    }
    Ok(vec![DdlResult::Rows(ShapedRows::text_rows(
        vec!["name".to_string(), "value".to_string()],
        rows,
    ))])
}

/// Build the canonical `(name, value)` rows for `SHOW STATS` and
/// `SHOW SERVER STATS`. Both commands use the same source; the
/// distinction is purely a UX synonym.
fn server_stats_rows(state: &SharedState) -> Vec<(String, String)> {
    use std::sync::atomic::Ordering;

    let mut rows: Vec<(String, String)> = Vec::new();

    rows.push(("version".into(), crate::version::VERSION.to_string()));

    if let Some(sys) = state.system_metrics.as_ref() {
        rows.push((
            "queries_total".into(),
            sys.queries_total.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "query_errors".into(),
            sys.query_errors.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "slow_queries_total".into(),
            sys.slow_queries_total.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "active_connections".into(),
            sys.active_connections.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "pgwire_connections".into(),
            sys.pgwire_connections.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "http_connections".into(),
            sys.http_connections.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "native_connections".into(),
            sys.native_connections.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "auth_failures".into(),
            sys.auth_failures.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "auth_successes".into(),
            sys.auth_successes.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "wal_fsync_count".into(),
            sys.wal_fsync_count.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "wal_segment_count".into(),
            sys.wal_segment_count.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "wal_segment_bytes".into(),
            sys.wal_segment_bytes.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "raft_apply_lag".into(),
            sys.raft_apply_lag.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "compaction_debt".into(),
            sys.compaction_debt.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "compaction_cycles".into(),
            sys.compaction_cycles.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_vector".into(),
            sys.queries_vector.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_graph".into(),
            sys.queries_graph.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "graph_edges_written_total".into(),
            sys.graph_edges_written.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "graph_edges_deleted_total".into(),
            sys.graph_edges_deleted.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_document".into(),
            sys.queries_document.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_columnar".into(),
            sys.queries_columnar.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_kv".into(),
            sys.queries_kv.load(Ordering::Relaxed).to_string(),
        ));
        rows.push((
            "queries_fts".into(),
            sys.queries_fts.load(Ordering::Relaxed).to_string(),
        ));
    }

    rows
}

/// SHOW STATS / SHOW SERVER STATS — server-wide counters as
/// `(name, value)` rows.
///
/// Restricted to tenant_admin or superuser; the same authorisation
/// envelope as `SHOW USERS`. Numbers are emitted as their decimal
/// string form so the column type stays uniform.
pub fn show_server_stats(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show stats")?;
    key_value_result(server_stats_rows(state))
}

/// SHOW METRICS — `(name, value)` projection of the same source as
/// SHOW STATS, with histogram percentiles appended so latency-style
/// metrics are visible from the SQL surface.
pub fn show_metrics(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show metrics")?;
    let mut rows = server_stats_rows(state);

    if let Some(sys) = state.system_metrics.as_ref() {
        rows.push((
            "wal_fsync_p50_us".into(),
            sys.wal_fsync_seconds.percentile(50.0).to_string(),
        ));
        rows.push((
            "wal_fsync_p99_us".into(),
            sys.wal_fsync_seconds.percentile(99.0).to_string(),
        ));
        rows.push((
            "query_latency_p50_us".into(),
            sys.query_latency.percentile(50.0).to_string(),
        ));
        rows.push((
            "query_latency_p99_us".into(),
            sys.query_latency.percentile(99.0).to_string(),
        ));
    }

    key_value_result(rows)
}

/// SHOW MEMORY — per-engine memory budget and utilisation.
///
/// Columns: `engine`, `allocated_bytes`, `limit_bytes`, `peak_bytes`,
/// `rejections`, `utilization_percent`. One row per engine in
/// `nodedb_mem::EngineId`.
pub fn show_memory(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_tenant_admin(identity, "show memory")?;

    let columns = vec![
        "engine".to_string(),
        "allocated_bytes".to_string(),
        "limit_bytes".to_string(),
        "peak_bytes".to_string(),
        "rejections".to_string(),
        "utilization_percent".to_string(),
    ];
    let column_types = vec![
        DdlColType::Text,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
        DdlColType::Int8,
    ];

    let mut rows = Vec::new();
    for snap in state.governor.snapshot() {
        let mut row = Map::new();
        row.insert(
            "engine".to_string(),
            JsonValue::String(format!("{:?}", snap.engine)),
        );
        row.insert(
            "allocated_bytes".to_string(),
            JsonValue::String((snap.allocated as i64).to_string()),
        );
        row.insert(
            "limit_bytes".to_string(),
            JsonValue::String((snap.limit as i64).to_string()),
        );
        row.insert(
            "peak_bytes".to_string(),
            JsonValue::String((snap.peak as i64).to_string()),
        );
        row.insert(
            "rejections".to_string(),
            JsonValue::String((snap.rejections as i64).to_string()),
        );
        row.insert(
            "utilization_percent".to_string(),
            JsonValue::String((snap.utilization_percent as i64).to_string()),
        );
        rows.push(row);
    }

    Ok(vec![DdlResult::Rows(ShapedRows::from_json_rows(
        columns,
        column_types,
        rows,
    ))])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::metrics::SystemMetrics;
    use std::sync::Arc;

    /// Read one `(name, value)` row from a rendered row set.
    fn row_value(rows: &[(String, String)], name: &str) -> Option<String> {
        rows.iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone())
    }

    /// The graph write counters render on the Prometheus surface as counters
    /// with their own help text, next to the live-edge gauge they must not be
    /// confused with.
    #[test]
    fn the_graph_write_counters_render_on_the_prometheus_surface() {
        let metrics = SystemMetrics::new();
        metrics.record_graph_edge_written();
        metrics.record_graph_edge_deleted();
        let output = metrics.to_prometheus();

        assert!(
            output.contains("# TYPE nodedb_graph_edges_written_total counter"),
            "the write counter renders as a counter: {output}"
        );
        assert!(
            output.contains("nodedb_graph_edges_written_total 1"),
            "the write counter carries its value: {output}"
        );
        assert!(
            output.contains("# TYPE nodedb_graph_edges_deleted_total counter"),
            "the delete counter renders as a counter: {output}"
        );
        assert!(
            output.contains("nodedb_graph_edges_deleted_total 1"),
            "the delete counter carries its value: {output}"
        );
    }

    /// The row builder reads the counters off `SystemMetrics`, the same
    /// instance the Data Plane records through, so `SHOW STATS` cannot drift
    /// from `/metrics`. Rows carry the value as decimal text like every other
    /// counter in the set.
    #[test]
    fn the_stats_rows_read_the_graph_write_counters_from_metrics() {
        let directory = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(
            crate::wal::WalManager::open_for_testing(&directory.path().join("obs-stats.wal"))
                .expect("open WAL"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let metrics = state
            .system_metrics
            .as_ref()
            .expect("system metrics are wired")
            .clone();
        metrics.record_graph_edge_written();
        metrics.record_graph_edge_written();
        metrics.record_graph_edge_deleted();

        let rows = server_stats_rows(&state);

        assert_eq!(
            row_value(&rows, "graph_edges_written_total").as_deref(),
            Some("2"),
        );
        assert_eq!(
            row_value(&rows, "graph_edges_deleted_total").as_deref(),
            Some("1"),
        );
    }
}
