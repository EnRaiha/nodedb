// SPDX-License-Identifier: BUSL-1.1

//! GET `/obsv/api/v1/metadata` — metric metadata for Grafana metric browser.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::control::promql;
use crate::control::server::http::admission::{admit, identity_database};
use crate::control::server::http::auth::{AppState, ResolvedIdentity};
use crate::control::server::http::peer::PeerAddr;

pub async fn metadata(
    identity: ResolvedIdentity,
    peer: PeerAddr,
    State(state): State<AppState>,
) -> Response {
    let rate_limit_headers = match admit(
        &state,
        &identity.0,
        identity_database(&identity.0),
        peer.as_str(),
        "promql_metadata",
    ) {
        Ok(headers) => headers,
        Err(error) => return error.into_response(),
    };

    let mut out = String::from(r#"{"status":"success","data":{"#);
    let mut first = true;

    if state.shared.system_metrics.is_some() {
        let metrics_meta: &[(&str, &str, &str)] = &[
            ("nodedb_queries_total", "counter", "Total queries executed"),
            ("nodedb_query_errors_total", "counter", "Query errors"),
            (
                "nodedb_active_connections",
                "gauge",
                "Active client connections",
            ),
            (
                "nodedb_wal_fsync_seconds",
                "histogram",
                "WAL fsync latency distribution in seconds",
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

        for (name, metric_type, help) in metrics_meta {
            if !first {
                out.push(',');
            }
            first = false;
            out.push('"');
            out.push_str(name);
            out.push_str(r#"":[{"type":""#);
            out.push_str(metric_type);
            out.push_str(r#"","help":""#);
            promql::types::json_escape(&mut out, help);
            out.push_str(r#"","unit":""}]"#);
        }
    }

    out.push_str("}}");
    (
        StatusCode::OK,
        rate_limit_headers,
        [("content-type", "application/json")],
        out,
    )
        .into_response()
}
