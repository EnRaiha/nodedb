// SPDX-License-Identifier: BUSL-1.1

//! This node's built-in counters as PromQL series.
//!
//! The counters are node state, not stored data. Each one is a single
//! sample at the current time. They are visible to the same principals as
//! `/metrics`: superusers and holders of the Monitor role.

use std::sync::atomic::Ordering;

use crate::control::promql::types::{Labels, Sample, Series};
use crate::control::security::identity::{AuthenticatedIdentity, Role};
use crate::control::state::SharedState;

/// Name, type and help text of every node counter, in emission order.
pub(crate) const NODE_METRICS: &[(&str, &str, &str)] = &[
    ("nodedb_queries_total", "counter", "Total queries executed"),
    ("nodedb_query_errors_total", "counter", "Query errors"),
    (
        "nodedb_active_connections",
        "gauge",
        "Active client connections",
    ),
    (
        "nodedb_wal_fsync_mean_seconds",
        "gauge",
        "Mean WAL fsync latency in seconds",
    ),
    ("nodedb_raft_apply_lag", "gauge", "Raft apply lag entries"),
    (
        "nodedb_bridge_utilization_ratio",
        "gauge",
        "SPSC bridge utilization (0..1 ratio)",
    ),
    (
        "nodedb_compaction_debt",
        "gauge",
        "Pending L1 segments for compaction",
    ),
    (
        "nodedb_vector_searches_total",
        "counter",
        "Vector search operations",
    ),
    (
        "nodedb_graph_traversals_total",
        "counter",
        "Graph traversal operations",
    ),
    (
        "nodedb_fts_searches_total",
        "counter",
        "Full-text search operations",
    ),
    ("nodedb_kv_gets_total", "counter", "KV GET operations"),
    ("nodedb_kv_memory_bytes", "gauge", "KV engine memory usage"),
    (
        "nodedb_pgwire_connections",
        "gauge",
        "Active pgwire connections",
    ),
    (
        "nodedb_http_connections",
        "gauge",
        "Active HTTP connections",
    ),
    (
        "nodedb_websocket_connections",
        "gauge",
        "Active WebSocket connections",
    ),
    (
        "nodedb_slow_queries_total",
        "counter",
        "Queries exceeding 100ms",
    ),
    (
        "nodedb_storage_l0_bytes",
        "gauge",
        "L0 (hot/RAM) storage bytes",
    ),
    (
        "nodedb_storage_l1_bytes",
        "gauge",
        "L1 (warm/NVMe) storage bytes",
    ),
];

/// Whether `identity` can see the node counters.
pub(crate) fn visible_to(identity: &AuthenticatedIdentity) -> bool {
    identity.is_superuser || identity.has_role(&Role::Monitor)
}

/// The node counters as one-sample series stamped `now_ms`. Empty when the
/// node keeps no system metrics.
pub(crate) fn node_series(state: &SharedState, now_ms: i64) -> Vec<Series> {
    let Some(sys) = state.system_metrics.as_ref() else {
        return Vec::new();
    };
    let fsync_count = sys.wal_fsync_seconds.count();
    let fsync_mean_seconds = if fsync_count > 0 {
        sys.wal_fsync_seconds.sum_us() as f64 / fsync_count as f64 / 1_000_000.0
    } else {
        0.0
    };
    let values: [f64; 18] = [
        sys.queries_total.load(Ordering::Relaxed) as f64,
        sys.query_errors.load(Ordering::Relaxed) as f64,
        sys.active_connections.load(Ordering::Relaxed) as f64,
        fsync_mean_seconds,
        sys.raft_apply_lag.load(Ordering::Relaxed) as f64,
        sys.bridge_utilization.load(Ordering::Relaxed) as f64 / 100.0,
        sys.compaction_debt.load(Ordering::Relaxed) as f64,
        sys.vector_searches.load(Ordering::Relaxed) as f64,
        sys.graph_traversals.load(Ordering::Relaxed) as f64,
        sys.fts_searches.load(Ordering::Relaxed) as f64,
        sys.kv_gets_total.load(Ordering::Relaxed) as f64,
        sys.kv_memory_bytes.load(Ordering::Relaxed) as f64,
        sys.pgwire_connections.load(Ordering::Relaxed) as f64,
        sys.http_connections.load(Ordering::Relaxed) as f64,
        sys.websocket_connections.load(Ordering::Relaxed) as f64,
        sys.slow_queries_total.load(Ordering::Relaxed) as f64,
        sys.storage_l0_bytes.load(Ordering::Relaxed) as f64,
        sys.storage_l1_bytes.load(Ordering::Relaxed) as f64,
    ];
    NODE_METRICS
        .iter()
        .zip(values)
        .map(|((name, _, _), value)| {
            let mut labels = Labels::new();
            labels.insert("__name__".to_owned(), (*name).to_owned());
            Series {
                labels,
                samples: vec![Sample {
                    timestamp_ms: now_ms,
                    value,
                }],
            }
        })
        .collect()
}
