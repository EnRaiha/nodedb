// SPDX-License-Identifier: BUSL-1.1
//! Cross-node PK WRITE correctness (UPDATE / DELETE) from a non-member
//! coordinator — and the surrogate-map pollution regression that follows.
//!
//! ## The bug this guards against
//!
//! A PK point op resolves pk → surrogate on the QUERY COORDINATOR's local
//! catalog. The surrogate↔PK map (`surrogate_pk{,_rev}_v3`) is SHARDED to the
//! collection's data-group members. `document_strict` collections are
//! single-vShard-homed, so when the coordinator is NOT a member of that group,
//! resolution misses → the coordinator ships `Surrogate::ZERO` to the owner.
//!
//! Unlike point READS (which route through the owner's `exec_receiver`, fixed
//! separately), point WRITES (UPDATE / DELETE by PK) route via Raft
//! propose → apply. On apply, the owner's `decode.rs` `PointUpdate` /
//! `PointDelete` arms resolved the carried surrogate via `assigner.bind(...)`,
//! which is FIRST-WINS:
//!
//!   - For an EXISTING row a binding already exists, so `bind` returns the
//!     existing (correct) surrogate and the ZERO is harmlessly discarded.
//!   - For a NON-EXISTENT pk (UPDATE/DELETE of a key that was never inserted,
//!     or an out-of-order apply) no binding exists, so `bind` PUTS
//!     `pk → ZERO` into the catalog. That pollutes the map: a LATER INSERT of
//!     that same pk finds the existing ZERO binding (first-wins) and stores the
//!     row under surrogate ZERO → silent data corruption / wrong lookups.
//!
//! ## The fix being verified
//!
//! The `PointUpdate` / `PointDelete` apply arms now re-resolve a ZERO carried
//! surrogate READ-ONLY via `assigner.lookup(...)` and NEVER bind ZERO. A
//! non-ZERO (authoritative, member-coordinator) carried value is still bound
//! first-wins. A missing pk stays ZERO (a correct no-op on a non-existent row)
//! and leaves the catalog untouched, so a subsequent INSERT of that pk gets a
//! freshly allocated surrogate and resolves correctly.
//!
//! ## Test shape
//!
//!  1. Spawn a 3-node cluster, create a `document_strict` collection with a PK,
//!     insert a few rows via one node, and converge.
//!  2. UPDATE-existing from a non-owner node → read back the new value.
//!  3. DELETE-existing from a non-owner node → assert the row is gone.
//!  4. POLLUTION (the core regression): DELETE a ghost (non-existent) pk from a
//!     non-owner node, THEN INSERT that same pk, converge, and assert the
//!     inserted value resolves from a non-owner coordinator. Without the fix
//!     the ghost-DELETE binds `ghost → ZERO`, the INSERT lands under ZERO, and
//!     the read resolves wrong/empty.
//!
//! Each mutating statement is issued from EVERY node so that at least one is a
//! genuine non-member coordinator (the harness does not expose routing to pick
//! a definite non-member, so iterating covers it).

mod common;
use common::cluster_harness::TestCluster;

use std::time::{Duration, Instant};

const ROW_COUNT: u32 = 5;

/// Format a `tokio_postgres` error as `sqlstate: message` (or plain text).
fn pg_detail(e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!("{e}")
    }
}

/// Is this error a transient cluster catch-up condition that warrants a retry
/// (catalog/descriptor lag), as opposed to a genuine empty/wrong result?
///
/// We retry ONLY on:
///   - "table not found" / "collection not found" (sqlstate 42601): the catalog
///     has not yet propagated to this coordinator.
///   - "schema changed during execution" / "please retry": a descriptor version
///     race that resolves on the next attempt.
///
/// We never retry a successful-but-wrong result — that is the bug, and the
/// caller asserts on it directly.
fn is_transient(e: &tokio_postgres::Error) -> bool {
    if let Some(db) = e.as_db_error() {
        let code = db.code().code();
        let msg = db.message();
        code == "42601"
            || msg.contains("table not found")
            || msg.contains("collection not found")
            || msg.contains("schema changed during execution")
            || msg.contains("please retry")
    } else {
        false
    }
}

/// Run `SELECT payload FROM <coll> WHERE id = <pk>` on `client`, returning the
/// single `payload` value. Retries only transient catch-up errors until
/// `timeout`; a query that SUCCEEDS but returns no row is returned as `None`.
async fn point_get_payload(
    client: &tokio_postgres::Client,
    coll: &str,
    pk: &str,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT payload FROM {coll} WHERE id = '{pk}'"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg {
                        return r.get(0).map(|s| s.to_string());
                    }
                }
                // Query succeeded with zero data rows — this is one of the
                // failure modes the test catches, so do NOT retry it.
                return None;
            }
            Err(ref e) => {
                if is_transient(e) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                panic!("point-get `{pk}` on {coll} failed: {}", pg_detail(e));
            }
        }
    }
}

/// Run `SELECT COUNT(*) FROM <coll>`, retrying only transient catch-up errors.
async fn count_rows(client: &tokio_postgres::Client, coll: &str, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        match client
            .simple_query(&format!("SELECT COUNT(*) FROM {coll}"))
            .await
        {
            Ok(rows) => {
                for msg in rows {
                    if let tokio_postgres::SimpleQueryMessage::Row(r) = msg
                        && let Some(s) = r.get(0)
                    {
                        return s.parse::<usize>().expect("COUNT(*) parse");
                    }
                }
                panic!("COUNT(*) returned no rows for {coll}");
            }
            Err(ref e) => {
                if is_transient(e) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                panic!("COUNT(*) on {coll} failed: {}", pg_detail(e));
            }
        }
    }
}

/// Execute a mutating statement, retrying only transient catch-up errors.
async fn exec_dml(
    client: &tokio_postgres::Client,
    sql: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match client.simple_query(sql).await {
            Ok(_) => return Ok(()),
            Err(ref e) => {
                if is_transient(e) && Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                return Err(pg_detail(e));
            }
        }
    }
}

/// UPDATE / DELETE by PK from a non-member coordinator must apply correctly,
/// and a ghost UPDATE/DELETE (non-existent pk) must NOT pollute the
/// surrogate↔PK map and corrupt a subsequent INSERT of that pk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_pk_write_resolves_and_does_not_pollute() {
    let cluster = TestCluster::spawn_three()
        .await
        .expect("spawn 3-node cluster");

    cluster
        .exec_ddl_on_any_leader(
            "CREATE COLLECTION xn_pk_w \
             (id TEXT PRIMARY KEY, payload TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION xn_pk_w");

    // Insert the rows through a single node. The collection is single-homed, so
    // exactly one vShard owner holds the surrogate↔PK binding for all keys; the
    // other two nodes are non-members for these keys.
    for i in 0..ROW_COUNT {
        cluster.nodes[0]
            .client
            .simple_query(&format!(
                "INSERT INTO xn_pk_w (id, payload) VALUES ('row-{i}', 'payload-{i}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("insert row-{i}: {}", pg_detail(&e)));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    let last = cluster.nodes.len() - 1;

    // --- UPDATE-existing from a non-owner coordinator ---------------------
    // Row-0 was inserted via node 0; mutate it from the last node, which is a
    // non-member for this key (it shipped Surrogate::ZERO before the fix, and
    // even with the fix relies on read-only re-resolution of the existing
    // binding rather than binding ZERO).
    exec_dml(
        &cluster.nodes[last].client,
        "UPDATE xn_pk_w SET payload = 'updated-0' WHERE id = 'row-0'",
        Duration::from_secs(10),
    )
    .await
    .expect("cross-node UPDATE of row-0");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got =
            point_get_payload(&node.client, "xn_pk_w", "row-0", Duration::from_secs(10)).await;
        assert_eq!(
            got.as_deref(),
            Some("updated-0"),
            "node {idx}: row-0 after cross-node UPDATE returned {got:?}, expected updated-0"
        );
    }

    // --- DELETE-existing from a non-owner coordinator ---------------------
    exec_dml(
        &cluster.nodes[last].client,
        "DELETE FROM xn_pk_w WHERE id = 'row-1'",
        Duration::from_secs(10),
    )
    .await
    .expect("cross-node DELETE of row-1");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got =
            point_get_payload(&node.client, "xn_pk_w", "row-1", Duration::from_secs(10)).await;
        assert_eq!(
            got, None,
            "node {idx}: row-1 after cross-node DELETE returned {got:?}, expected gone"
        );
        let count = count_rows(&node.client, "xn_pk_w", Duration::from_secs(10)).await;
        assert_eq!(
            count,
            (ROW_COUNT - 1) as usize,
            "node {idx}: COUNT(*) after one DELETE = {count}, expected {}",
            ROW_COUNT - 1
        );
    }

    // --- POLLUTION regression (the core assertion) ------------------------
    // DELETE a key that was NEVER inserted, from EVERY node so at least one is
    // a non-member coordinator that ships Surrogate::ZERO. Without the fix, the
    // owner's apply binds `ghost → ZERO`. This is a correct no-op on the row
    // itself either way.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        exec_dml(
            &node.client,
            "DELETE FROM xn_pk_w WHERE id = 'ghost'",
            Duration::from_secs(10),
        )
        .await
        .unwrap_or_else(|e| panic!("node {idx}: ghost DELETE: {e}"));
    }

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    // Now INSERT the ghost key. With pollution, the existing ZERO binding wins
    // (first-wins) and the row lands under surrogate ZERO → the read below
    // resolves wrong/empty. With the fix, no binding was ever written, so the
    // INSERT allocates a fresh surrogate and the read resolves correctly.
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO xn_pk_w (id, payload) VALUES ('ghost', 'ghost-val')")
        .await
        .unwrap_or_else(|e| panic!("insert ghost: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got =
            point_get_payload(&node.client, "xn_pk_w", "ghost", Duration::from_secs(10)).await;
        assert_eq!(
            got.as_deref(),
            Some("ghost-val"),
            "node {idx}: ghost after no-op DELETE + INSERT returned {got:?}, expected ghost-val \
             (a polluted `ghost → ZERO` binding from the cross-node DELETE would corrupt this)"
        );
    }

    cluster.shutdown().await;
}
