// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for the distributed-shuffle OCC cross-shard read-validation
//! integration tests (shuffle-JOIN and shuffle-AGGREGATE in-transaction suites).
//!
//! These small pgwire / sequencer-introspection helpers are identical across the
//! shuffle-join and shuffle-aggregate OCC suites, so they live here to avoid
//! duplication. Each suite keeps its own unique setup helpers (collection layout,
//! forced-plan session overrides, SQL builders) local.

use std::sync::atomic::Ordering;

use tokio_postgres::SimpleQueryMessage;

use crate::cluster_harness::TestClusterNode;

/// Observed sequencer-group leader id from the node's local Raft status, or `0`
/// if no leader is known yet.
pub fn sequencer_leader(node: &TestClusterNode) -> u64 {
    let Some(status_fn) = node.shared.raft_status_fn.get() else {
        return 0;
    };
    status_fn()
        .into_iter()
        .find(|g| g.group_id == nodedb_cluster::calvin::SEQUENCER_GROUP_ID)
        .map(|g| g.leader_id)
        .unwrap_or(0)
}

/// Count of transactions the single-node sequencer has admitted to an epoch, or
/// `0` if the sequencer metrics handle is not installed yet. Used to prove a
/// COMMIT (or its abort) went through the multi-participant Calvin barrier rather
/// than a single-shard fast path.
pub fn admitted_total(node: &TestClusterNode) -> u64 {
    node.shared
        .sequencer_metrics
        .get()
        .map(|m| m.admitted_total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Extract the SQLSTATE code from a `tokio_postgres` error, or `None`.
pub fn pg_sqlstate(e: &tokio_postgres::Error) -> Option<String> {
    e.as_db_error().map(|db| db.code().code().to_string())
}

/// Human-readable `sqlstate: message` rendering for assertion failure context.
pub fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Count `Row` messages in a simple-query result set.
pub fn count_rows(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

/// `true` if any returned row's `id` column equals `id`.
pub fn has_id(msgs: &[SimpleQueryMessage], id: &str) -> bool {
    msgs.iter().any(|m| match m {
        SimpleQueryMessage::Row(r) => r.get("id") == Some(id),
        _ => false,
    })
}

/// Open an additional pgwire connection to the SAME single node.
pub async fn open_client(
    node: &TestClusterNode,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=nodedb",
        node.pg_addr.port()
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("open extra pgwire connection to the single node");
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    (client, handle)
}
