// SPDX-License-Identifier: BUSL-1.1

//! Row-level security over graph-edge and vector-primary writes.
//!
//! Two write surfaces store a real row image outside the document engine, and
//! a `FOR WRITE` policy has to reach both or it governs only part of what the
//! caller can persist:
//!
//! - `GRAPH INSERT EDGE` carries its `PROPERTIES` clause as the edge's row
//!   body, and `GRAPH DELETE EDGE` removes a body that already exists. Neither
//!   is accompanied by a document write, so nothing else would decide them.
//! - A `primary='vector'` collection stores the row's non-vector columns in the
//!   vector upsert itself; there is no document sidecar write to gate.
//!
//! What these tests pin:
//!
//! - An edge whose properties violate the policy is rejected; a conforming one
//!   applies.
//! - A delete is decided against the edge's STORED properties, and a refused
//!   delete leaves the edge exactly where it was.
//! - A vector-primary insert is decided against its payload image.
//! - A vector-primary write that omits the governed column is denied. The
//!   payload image is what the statement wrote, so the predicate has nothing to
//!   test and fails closed. That is the safe direction, and pinning it makes it
//!   a stated behavior rather than a surprise an operator meets in production —
//!   only the `payload_indexes` subset is queryable afterwards, so it is easy
//!   to assume the other columns never reach the write at all.
//! - A document write carrying `_from`/`_to` is decided ONCE, by the document
//!   policy. The mirrored edge it produces is reconciliation of that same
//!   already-decided row, so gating it a second time would refuse conforming
//!   inserts on the strength of their own mirror.
//! - Collections with no write policy are untouched.

mod common;

use common::pgwire_harness::TestServer;
use nodedb_types::id::{DatabaseId, VShardId};

const PASSWORD: &str = "graph-vector-write-rls-secret-42";

/// The least privilege that can run the statements under test, so a denial is
/// the policy's doing and not the RBAC layer's.
const ROLE: &str = "readwrite";

async fn create_user(server: &TestServer, user: &str) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    server
        .exec(&format!("GRANT ROLE {ROLE} TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant {ROLE} to {user}: {e}"));
}

/// Restrict writes on `collection` to rows the authenticated principal owns.
async fn write_policy(server: &TestServer, policy: &str, collection: &str) {
    server
        .exec(&format!(
            "CREATE RLS POLICY {policy} ON {collection} FOR WRITE \
             USING (owner = $auth.username)"
        ))
        .await
        .unwrap_or_else(|e| panic!("create write policy {policy}: {e}"));
}

/// Run `sql` as `user`, returning the rows' first column on success and the
/// server's error message on failure.
///
/// The message is read off the attached `DbError`, never off the
/// `tokio_postgres::Error` wrapper: that wrapper's `Display` is the fixed
/// string "db error", so asserting on it would make every refusal below
/// indistinguishable from every other failure.
async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map_err(|e| {
        e.as_db_error()
            .map(|db| db.message().to_string())
            .unwrap_or_else(|| e.to_string())
    });
    let rows = result.map(|messages| {
        messages
            .into_iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(0).unwrap_or("").to_string())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    });
    drop(client);
    handle.abort();
    rows
}

/// Assert a statement was refused BY THE POLICY rather than by some unrelated
/// failure that would make the test pass for the wrong reason.
fn assert_rls_denied(result: Result<Vec<String>, String>, what: &str) {
    match result {
        Ok(rows) => panic!("{what} must be refused, but it succeeded: {rows:?}"),
        Err(message) => assert!(
            message.contains("RLS"),
            "{what} must be refused by the RLS policy, got: {message}"
        ),
    }
}

/// The out-neighbors of `node` read back as the superuser, who holds no
/// restricting policy — so this is the true stored topology.
async fn neighbors(server: &TestServer, collection: &str, node: &str) -> Vec<String> {
    let rows = server
        .query_text(&format!(
            "GRAPH NEIGHBORS IN '{collection}' OF '{node}' DIRECTION out"
        ))
        .await
        .unwrap_or_else(|e| panic!("read neighbors of {node}: {e}"));
    let mut out = Vec::new();
    for row in rows {
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row).unwrap_or_default();
        for entry in parsed {
            if let Some(node) = entry.get("node").and_then(|value| value.as_str()) {
                out.push(node.to_string());
            }
        }
    }
    out
}

/// An edge insert with an explicit `PROPERTIES` object.
fn insert_edge(collection: &str, src: &str, dst: &str, owner: &str) -> String {
    format!(
        "GRAPH INSERT EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'knows' \
         PROPERTIES '{{\"owner\":\"{owner}\"}}'"
    )
}

fn delete_edge(collection: &str, src: &str, dst: &str) -> String {
    format!("GRAPH DELETE EDGE IN '{collection}' FROM '{src}' TO '{dst}' TYPE 'knows'")
}

/// The `PROPERTIES` clause is the edge's row image, so the policy decides it at
/// plan time: the conforming edge lands, the violating one does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_edge_insert_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "g_rls_ins_user";
    server
        .exec("CREATE COLLECTION g_rls_ins")
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_rls_ins_owner", "g_rls_ins").await;

    assert_rls_denied(
        run_as(&server, user, &insert_edge("g_rls_ins", "a", "b", "alice")).await,
        "an edge insert handing the properties to another owner",
    );
    assert!(
        neighbors(&server, "g_rls_ins", "a").await.is_empty(),
        "the refused edge must not exist"
    );

    run_as(&server, user, &insert_edge("g_rls_ins", "c", "d", user))
        .await
        .expect("an edge whose properties satisfy the policy must apply");
    assert_eq!(
        neighbors(&server, "g_rls_ins", "c").await,
        vec!["d".to_string()],
        "the conforming edge must be stored"
    );
}

/// An edge written with no `PROPERTIES` carries no field the predicate can
/// name, so it fails closed. "No field to test" must never mean "allow".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_edge_without_properties_is_rejected_under_a_write_policy() {
    let server = TestServer::start().await;
    let user = "g_rls_bare_user";
    server
        .exec("CREATE COLLECTION g_rls_bare")
        .await
        .expect("create edge collection");
    create_user(&server, user).await;
    write_policy(&server, "g_rls_bare_owner", "g_rls_bare").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "GRAPH INSERT EDGE IN 'g_rls_bare' FROM 'a' TO 'b' TYPE 'knows'",
        )
        .await,
        "an edge insert carrying no property object",
    );
    assert!(
        neighbors(&server, "g_rls_bare", "a").await.is_empty(),
        "the refused edge must not exist"
    );
}

/// A delete carries no image of its own, so it is decided in the Data Plane
/// against the edge's STORED properties — and a refused delete must leave that
/// edge exactly where it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_an_edge_the_policy_excludes_is_rejected() {
    let server = TestServer::start().await;
    let user = "g_rls_del_user";
    server
        .exec("CREATE COLLECTION g_rls_del")
        .await
        .expect("create edge collection");
    server
        .exec(&insert_edge("g_rls_del", "a", "b", "alice"))
        .await
        .expect("seed the edge owned by someone else");
    server
        .exec(&insert_edge("g_rls_del", "c", "d", user))
        .await
        .expect("seed the edge owned by the caller");
    create_user(&server, user).await;
    write_policy(&server, "g_rls_del_owner", "g_rls_del").await;

    assert_rls_denied(
        run_as(&server, user, &delete_edge("g_rls_del", "a", "b")).await,
        "deleting an edge outside the write policy",
    );
    assert_eq!(
        neighbors(&server, "g_rls_del", "a").await,
        vec!["b".to_string()],
        "the excluded edge must survive the refused delete"
    );

    run_as(&server, user, &delete_edge("g_rls_del", "c", "d"))
        .await
        .expect("deleting an owned edge must apply");
    assert!(
        neighbors(&server, "g_rls_del", "c").await.is_empty(),
        "the owned edge must be gone"
    );
}

/// A vector-primary collection stores the row in the upsert itself, so the
/// payload image is what the policy decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_violating_vector_primary_insert_is_rejected_and_a_conforming_one_succeeds() {
    let server = TestServer::start().await;
    let user = "v_rls_ins_user";
    server
        .exec(
            "CREATE COLLECTION v_rls_ins (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary collection");
    create_user(&server, user).await;
    write_policy(&server, "v_rls_ins_owner", "v_rls_ins").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO v_rls_ins (id, vec, owner) \
             VALUES ('r_theirs', ARRAY[1.0, 0.0, 0.0], 'alice')",
        )
        .await,
        "a vector-primary insert handing the row to another owner",
    );

    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO v_rls_ins (id, vec, owner) \
             VALUES ('r_mine', ARRAY[2.0, 0.0, 0.0], '{user}')"
        ),
    )
    .await
    .expect("a vector-primary insert whose payload satisfies the policy must apply");
}

/// The payload image is what the STATEMENT wrote, so an insert that omits the
/// governed column leaves the predicate nothing to test and fails closed.
/// Pinned because only the `payload_indexes` subset is queryable afterwards,
/// which makes it easy to assume the other columns never travel with the write
/// — they do, and their absence is what decides this.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_vector_write_omitting_the_governed_column_is_denied() {
    let server = TestServer::start().await;
    let user = "v_rls_omit_user";
    server
        .exec(
            "CREATE COLLECTION v_rls_omit \
                 (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary collection");
    create_user(&server, user).await;
    write_policy(&server, "v_rls_omit_owner", "v_rls_omit").await;

    assert_rls_denied(
        run_as(
            &server,
            user,
            "INSERT INTO v_rls_omit (id, vec) VALUES ('r1', ARRAY[1.0, 0.0, 0.0])",
        )
        .await,
        "a vector-primary insert that never supplies the governed column",
    );
}

/// A node id that hashes into the same vShard as `collection`.
///
/// A document write is homed on its collection's vShard and the mirrored edge
/// on `from_key(_from)`. When those two differ the statement is a cross-shard
/// transaction, which a single-node deployment refuses outright — before any
/// policy is consulted. Picking an endpoint inside the collection's own shard
/// keeps the statement single-shard, so what decides it is the write gate,
/// which is the thing under test.
fn node_in_collection_shard(collection: &str, prefix: &str) -> String {
    let home = VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection).as_u32();
    (0..100_000u32)
        .map(|n| format!("{prefix}{n}"))
        .find(|node| VShardId::from_key(node.as_bytes()).as_u32() == home)
        .expect("some node id must hash into the collection's vShard")
}

/// A document carrying `_from`/`_to` is mirrored as a graph edge. The document
/// write is decided by the policy; the mirror is reconciliation of that same
/// already-decided row and must not be decided again, or every conforming
/// insert would be refused on the strength of its own edge — whose property
/// object holds an edge weight and none of the columns a policy names.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_document_insert_carrying_from_and_to_is_gated_once() {
    let server = TestServer::start().await;
    let user = "d_rls_edge_user";
    server
        .exec("CREATE COLLECTION d_rls_edge")
        .await
        .expect("create document collection");
    create_user(&server, user).await;
    write_policy(&server, "d_rls_edge_owner", "d_rls_edge").await;

    let ok_src = node_in_collection_shard("d_rls_edge", "ok_src_");
    let bad_src = node_in_collection_shard("d_rls_edge", "bad_src_");

    run_as(
        &server,
        user,
        &format!(
            "INSERT INTO d_rls_edge (id, _from, _to, _type, owner) \
             VALUES ('e_ok', '{ok_src}', 'ok_dst', 'knows', '{user}')"
        ),
    )
    .await
    .expect("a conforming edge-bearing document must apply, mirror included");
    assert_eq!(
        neighbors(&server, "d_rls_edge", &ok_src).await,
        vec!["ok_dst".to_string()],
        "the mirrored edge must have been written, so the mirror was not gated a second time"
    );

    assert_rls_denied(
        run_as(
            &server,
            user,
            &format!(
                "INSERT INTO d_rls_edge (id, _from, _to, _type, owner) \
                 VALUES ('e_bad', '{bad_src}', 'bad_dst', 'knows', 'alice')"
            ),
        )
        .await,
        "an edge-bearing document whose row violates the policy",
    );
    assert!(
        neighbors(&server, "d_rls_edge", &bad_src).await.is_empty(),
        "a refused document write must leave no mirrored edge behind"
    );
}

/// Collections with no write policy pay nothing for the gate: every shape the
/// policed collections above refuse must still apply here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_without_a_write_policy_are_unaffected() {
    let server = TestServer::start().await;
    let user = "gv_rls_free_user";
    server
        .exec("CREATE COLLECTION gv_rls_free_edges")
        .await
        .expect("create edge collection");
    server
        .exec(
            "CREATE COLLECTION gv_rls_free_vec \
                 (id STRING PRIMARY KEY, vec VECTOR(3), owner STRING) \
             WITH (engine='vector', primary='vector', vector_field='vec', dim=3, \
                   payload_indexes=['owner'])",
        )
        .await
        .expect("create vector-primary collection");
    create_user(&server, user).await;

    run_as(
        &server,
        user,
        "GRAPH INSERT EDGE IN 'gv_rls_free_edges' FROM 'a' TO 'b' TYPE 'knows'",
    )
    .await
    .expect("an edge with no properties must apply with no write policy");
    run_as(
        &server,
        user,
        &insert_edge("gv_rls_free_edges", "c", "d", "alice"),
    )
    .await
    .expect("an edge owned by anyone must apply with no write policy");
    run_as(&server, user, &delete_edge("gv_rls_free_edges", "c", "d"))
        .await
        .expect("deleting any edge must apply with no write policy");
    run_as(
        &server,
        user,
        "INSERT INTO gv_rls_free_vec (id, vec, owner) \
         VALUES ('r1', ARRAY[1.0, 0.0, 0.0], 'alice')",
    )
    .await
    .expect("a vector-primary insert must apply with no write policy");

    assert_eq!(
        neighbors(&server, "gv_rls_free_edges", "a").await,
        vec!["b".to_string()],
        "the ungoverned edge must be stored"
    );
    assert!(
        neighbors(&server, "gv_rls_free_edges", "c")
            .await
            .is_empty(),
        "the ungoverned delete must have applied"
    );
}
