// SPDX-License-Identifier: BUSL-1.1

//! Prometheus serialization: WAL, replication, bridge, compaction, auth, connections, queries.

use std::fmt::Write;
use std::sync::atomic::Ordering;

use crate::control::metrics::system::SystemMetrics;

impl SystemMetrics {
    /// Serialize core infrastructure metrics as Prometheus text format.
    ///
    /// Called by [`to_prometheus()`](Self::to_prometheus) — not directly by handlers.
    pub(in crate::control::metrics) fn prometheus_core(&self, out: &mut String) {
        // ── WAL ──
        self.wal_fsync_seconds.write_prometheus(
            out,
            "nodedb_wal_fsync_seconds",
            "WAL fsync latency distribution in seconds",
        );
        counter(
            out,
            "nodedb_wal_fsync_total",
            "WAL fsync count",
            self.wal_fsync_count.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_wal_segment_count",
            "WAL segment files",
            self.wal_segment_count.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_wal_segment_bytes",
            "WAL total bytes",
            self.wal_segment_bytes.load(Ordering::Relaxed),
        );

        // ── Replication ──
        gauge(
            out,
            "nodedb_raft_apply_lag",
            "Raft apply lag entries",
            self.raft_apply_lag.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_raft_commit_index",
            "Raft commit index",
            self.raft_commit_index.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_raft_applied_index",
            "Raft applied index",
            self.raft_applied_index.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_raft_leader_term",
            "Raft leader term",
            self.raft_leader_term.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_raft_snapshot_total",
            "Raft snapshots taken",
            self.raft_snapshot_count.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_vshard_migrations_active",
            "Active vShard migrations",
            self.vshard_migrations_active.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_cluster_insecure_transports",
            "Insecure cluster transports created; alert whenever this is greater than zero",
            nodedb_cluster::insecure_transport_count(),
        );

        // ── Bridge ──
        gauge_f64(
            out,
            "nodedb_bridge_utilization_ratio",
            "SPSC bridge utilization as a ratio (0.0–1.0)",
            self.bridge_utilization.load(Ordering::Relaxed) as f64 / 100.0,
        );

        // ── Compaction ──
        gauge(
            out,
            "nodedb_compaction_debt",
            "Pending L1 segments for compaction",
            self.compaction_debt.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_compaction_cycles_total",
            "Compaction cycles completed",
            self.compaction_cycles.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_compaction_bytes_total",
            "Total bytes written by compaction (use rate() for throughput)",
            self.compaction_bytes_total.load(Ordering::Relaxed),
        );

        // ── Auth ──
        counter(
            out,
            "nodedb_auth_failures_total",
            "Failed auth attempts",
            self.auth_failures.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_auth_successes_total",
            "Successful auth attempts",
            self.auth_successes.load(Ordering::Relaxed),
        );

        // ── Connections ──
        gauge(
            out,
            "nodedb_active_connections",
            "Active client connections",
            self.active_connections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_pgwire_connections",
            "Active pgwire connections",
            self.pgwire_connections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_http_connections",
            "Active HTTP connections",
            self.http_connections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_native_connections",
            "Active native protocol connections",
            self.native_connections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_websocket_connections",
            "Active WebSocket connections",
            self.websocket_connections.load(Ordering::Relaxed),
        );
        gauge(
            out,
            "nodedb_ilp_connections",
            "Active ILP connections",
            self.ilp_connections.load(Ordering::Relaxed),
        );

        // ── Queries ──
        counter(
            out,
            "nodedb_queries_total",
            "Total queries executed",
            self.queries_total.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_query_errors_total",
            "Query errors",
            self.query_errors.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_slow_queries_total",
            "Queries exceeding 100ms",
            self.slow_queries_total.load(Ordering::Relaxed),
        );
        self.query_planning_seconds.write_prometheus(
            out,
            "nodedb_query_planning_seconds",
            "Query planning latency distribution in seconds",
        );
        self.query_execution_seconds.write_prometheus(
            out,
            "nodedb_query_execution_seconds",
            "Query execution latency distribution in seconds",
        );
        self.query_latency.write_prometheus(
            out,
            "nodedb_query_latency_seconds",
            "Query latency distribution",
        );

        // ── Per-engine queries ──
        counter(
            out,
            "nodedb_queries_vector_total",
            "Vector engine queries",
            self.queries_vector.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_queries_graph_total",
            "Graph engine queries",
            self.queries_graph.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_queries_document_total",
            "Document engine queries",
            self.queries_document.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_queries_columnar_total",
            "Columnar engine queries",
            self.queries_columnar.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_queries_kv_total",
            "KV engine queries",
            self.queries_kv.load(Ordering::Relaxed),
        );
        counter(
            out,
            "nodedb_queries_fts_total",
            "FTS engine queries",
            self.queries_fts.load(Ordering::Relaxed),
        );
    }
}

pub(in crate::control::metrics) fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = write!(
        out,
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}\n"
    );
}

pub(in crate::control::metrics) fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = write!(
        out,
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
    );
}

pub(in crate::control::metrics) fn gauge_f64(out: &mut String, name: &str, help: &str, value: f64) {
    let _ = write!(
        out,
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value:.2}\n"
    );
}
