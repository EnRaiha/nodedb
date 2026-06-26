// SPDX-License-Identifier: BUSL-1.1
//! Cross-node PK WRITE correctness (UPDATE / DELETE) from a non-leader
//! coordinator under RF=3 — and the surrogate-map pollution regression that the
//! apply path must never reintroduce.
//!
//! ## Multi-replica reality (RF=3)
//!
//! This 3-node test cluster runs replication factor 3: EVERY node is a voter of
//! every data group and locally binds surrogates for every committed write.
//! There is NO "non-member" coordinator that has to ship `Surrogate::ZERO` for
//! these keys — every node resolves pk → surrogate from its own local catalog.
//! A point WRITE (UPDATE / DELETE by PK) issued anywhere routes via Raft
//! propose → apply and lands on all three replicas.
//!
//! ## The invariants this guards
//!
//!  (a) CROSS-NODE CONVERGENCE: a PK UPDATE / DELETE issued from a coordinator
//!      that is NOT the group leader still resolves correctly and converges on
//!      EVERY replica. Because applies are async, a read can race a follower's
//!      apply, so every write is followed by a full-apply convergence barrier
//!      before any read-back.
//!
//!  (b) NO GHOST / PHANTOM POLLUTION: a PK that is only ever DELETEd or only
//!      ever READ (never INSERTed) must NEVER acquire a surrogate binding. On
//!      apply, the `decode.rs` `bind_or_lookup` path re-resolves a carried
//!      ZERO surrogate READ-ONLY and NEVER binds ZERO, so a missing pk stays
//!      unbound. A subsequent INSERT of that pk therefore allocates a FRESH
//!      surrogate and resolves correctly — no `pk → ZERO` phantom corrupts it.
//!
//! ## Test shape
//!
//!  1. Spawn a 3-node RF=3 cluster, create a `document_strict` collection with
//!     a PK, insert a few rows via one node, and converge.
//!  2. UPDATE-existing from a NON-LEADER node → converge → read back the new
//!     value on every node, and assert all three members agree.
//!  3. DELETE-existing from a NON-LEADER node → converge → assert the row is
//!     gone on every node, and that all three members agree.
//!  4. GHOST/PHANTOM (the anti-pollution regression):
//!       - DELETE a key that was NEVER inserted, from every node (each delete
//!         resolves to an unbound key → ZERO; apply must NOT bind it), then
//!         INSERT that key and assert it reads back as its real value on every
//!         node. A phantom `ghost → ZERO` binding would corrupt this read.
//!       - Assert a never-written, never-deleted pk returns no row on every
//!         node — proof that merely reading/deleting an absent key created no
//!         spurious binding.
//!
//! The mutating UPDATE / DELETE in (2) and (3) are issued from a non-leader
//! member; the ghost DELETE in (4) is issued from every node so the
//! anti-pollution path is exercised from all coordinators.

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

/// Read `pk` back on EVERY node and assert it equals `expected` (or is gone for
/// `None`), returning the agreed value. Also asserts all members AGREE — the
/// genuinely-new RF=3 invariant: every replica returns the same post-write
/// state, strictly stronger than a single-owner check.
async fn assert_all_members_agree(
    cluster: &TestCluster,
    coll: &str,
    pk: &str,
    expected: Option<&str>,
    label: &str,
) {
    let mut seen: Vec<Option<String>> = Vec::with_capacity(cluster.nodes.len());
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let got = point_get_payload(&node.client, coll, pk, Duration::from_secs(10)).await;
        assert_eq!(
            got.as_deref(),
            expected,
            "{label}: node {idx} for pk '{pk}' returned {got:?}, expected {expected:?}"
        );
        seen.push(got);
    }
    // Cross-member consistency: all replicas must report identical state.
    let first = &seen[0];
    for (idx, got) in seen.iter().enumerate() {
        assert_eq!(
            got, first,
            "{label}: members disagree on pk '{pk}' — node {idx} = {got:?}, node 0 = {first:?}"
        );
    }
}

/// Cross-node PK UPDATE / DELETE from a non-leader coordinator under RF=3 must
/// converge on every replica, all members must agree on the result, and a key
/// that is only ever deleted/read (never inserted) must NEVER acquire a phantom
/// surrogate binding that would corrupt a later INSERT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_node_pk_write_converges_and_does_not_pollute() {
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

    // RF=3: all three nodes are voters of the data group and locally bind a
    // surrogate for each committed write. Insert the rows through node 0.
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

    // Issue mutations from a DIFFERENT node than the one that inserted — a
    // cross-node, non-leader coordinator. Its write resolves pk → surrogate
    // locally (RF=3 member) and routes via Raft propose → apply to all replicas.
    let coord = cluster.nodes.len() - 1;

    // --- UPDATE-existing from a non-leader coordinator --------------------
    exec_dml(
        &cluster.nodes[coord].client,
        "UPDATE xn_pk_w SET payload = 'updated-0' WHERE id = 'row-0'",
        Duration::from_secs(10),
    )
    .await
    .expect("cross-node UPDATE of row-0");

    // Applies are async — barrier before reading so reads don't race followers.
    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    assert_all_members_agree(
        &cluster,
        "xn_pk_w",
        "row-0",
        Some("updated-0"),
        "cross-node UPDATE",
    )
    .await;

    // --- DELETE-existing from a non-leader coordinator --------------------
    exec_dml(
        &cluster.nodes[coord].client,
        "DELETE FROM xn_pk_w WHERE id = 'row-1'",
        Duration::from_secs(10),
    )
    .await
    .expect("cross-node DELETE of row-1");

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    assert_all_members_agree(&cluster, "xn_pk_w", "row-1", None, "cross-node DELETE").await;

    // The deleted row must be gone everywhere, and the count consistent.
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let count = count_rows(&node.client, "xn_pk_w", Duration::from_secs(10)).await;
        assert_eq!(
            count,
            (ROW_COUNT - 1) as usize,
            "node {idx}: COUNT(*) after one DELETE = {count}, expected {}",
            ROW_COUNT - 1
        );
    }

    // --- ANTI-POLLUTION (ghost) regression — load-bearing ----------------
    // DELETE a key that was NEVER inserted, from EVERY node. Each delete
    // resolves to an unbound key (ZERO carry); apply must re-resolve READ-ONLY
    // and NEVER bind `ghost → ZERO`. The delete is a correct no-op either way,
    // but a phantom ZERO binding here would corrupt the INSERT below.
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

    // INSERT the ghost key. With pollution, a phantom `ghost → ZERO` binding
    // wins (first-wins) and the row lands under surrogate ZERO → the read
    // resolves wrong/empty. With the correct apply path no binding was ever
    // written, so the INSERT allocates a fresh surrogate and resolves on all
    // replicas. All members must agree.
    cluster.nodes[0]
        .client
        .simple_query("INSERT INTO xn_pk_w (id, payload) VALUES ('ghost', 'ghost-val')")
        .await
        .unwrap_or_else(|e| panic!("insert ghost: {}", pg_detail(&e)));

    cluster
        .wait_for_full_apply_convergence(Duration::from_secs(15))
        .await;

    assert_all_members_agree(
        &cluster,
        "xn_pk_w",
        "ghost",
        Some("ghost-val"),
        "ghost no-op DELETE + INSERT (a phantom `ghost → ZERO` binding would corrupt this)",
    )
    .await;

    // --- PHANTOM read/never-touched key — strengthened anti-pollution -----
    // 'phantom' is never inserted and never deleted; merely reading an absent
    // key must NOT create any binding. It must return no row on every member,
    // and the members must agree it is absent.
    assert_all_members_agree(
        &cluster,
        "xn_pk_w",
        "phantom",
        None,
        "never-written/never-deleted key must have no spurious binding",
    )
    .await;

    cluster.shutdown().await;
}
