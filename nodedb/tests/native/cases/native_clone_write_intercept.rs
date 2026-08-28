// SPDX-License-Identifier: BUSL-1.1

//! Native-protocol UPDATE on a `Shadowed` clone must copy the source row up
//! before applying, exactly like pgwire — the clone CoW write-interception
//! hook now runs from the protocol-neutral `shared::clone_write` module at
//! native's own dispatch entry point too, not only on the pgwire path.
//!
//! Setup and verification run over pgwire; the write under test — an UPDATE
//! against a row that exists only in the clone's source — runs over native.
//!
//! The native protocol has no `USE DATABASE` statement: a session's database
//! is bound once, on its `OpCode::Auth` request (`TextFields.database`), and
//! re-authenticating on the same connection to change it is refused (see
//! `control/server/native/session/auth.rs` — "already authenticated;
//! reconnect to switch identity"). So the clone-scoped session below
//! authenticates explicitly with `database: Some("ncw_tgt")` on its first
//! request, rather than issuing SQL text the native router does not
//! recognize as a statement at all.

use nodedb_test_support::native_harness::{do_handshake, send_request, send_sql};
use nodedb_test_support::pgwire_harness::TestServer;

use nodedb_types::protocol::opcodes::ResponseStatus;
use nodedb_types::protocol::text_fields::TextFields;
use nodedb_types::protocol::{AuthMethod, HelloFrame, OpCode};
use nodedb_types::value::Value;

/// A native UPDATE against a row that lives only in a Shadowed clone's
/// source copies the row up into the clone before applying: the clone
/// reflects the new value, and the source row is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_update_on_shadowed_clone_copies_up_source_row() {
    let srv = TestServer::start().await;

    srv.exec("CREATE DATABASE ncw_src")
        .await
        .expect("create source database");
    srv.exec("USE DATABASE ncw_src")
        .await
        .expect("use source database");
    srv.exec("CREATE COLLECTION docs (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create source collection");
    srv.exec("INSERT INTO docs (id, v) VALUES ('a', 1)")
        .await
        .expect("seed source row");

    srv.exec("USE DATABASE default")
        .await
        .expect("use default database");
    srv.exec("CLONE DATABASE ncw_tgt FROM ncw_src")
        .await
        .expect("clone database (Shadowed by default)");

    // The write under test: a native UPDATE against `docs.a`, which was
    // never copied into `ncw_tgt` — it exists only in `ncw_src`. The
    // session authenticates straight into the clone database — native has
    // no mid-session database switch (see module docs) — using the same
    // trust identity the harness's `AuthMode::Trust` server provisions.
    let trust_username = srv
        .shared
        .credentials
        .configured_trust_superuser()
        .expect("read configured trust superuser")
        .expect("harness runs in AuthMode::Trust");

    let native_addr = format!("127.0.0.1:{}", srv.native_port)
        .parse()
        .expect("native addr");
    let (mut native_stream, _ack) = do_handshake(native_addr, &HelloFrame::current())
        .await
        .expect("native handshake");

    let auth = send_request(
        &mut native_stream,
        1,
        OpCode::Auth,
        TextFields {
            auth: Some(AuthMethod::Trust {
                username: trust_username,
            }),
            database: Some("ncw_tgt".into()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        auth.status,
        ResponseStatus::Ok,
        "native session must authenticate straight into the clone database: {auth:?}"
    );

    let update = send_sql(
        &mut native_stream,
        2,
        "UPDATE docs SET v = 99 WHERE id = 'a'",
    )
    .await;
    assert_eq!(
        update.status,
        ResponseStatus::Ok,
        "native UPDATE on a source-only clone row must succeed via copy-up: {update:?}"
    );
    assert_eq!(
        update.rows_affected,
        Some(1),
        "the copied-up row must be the one the UPDATE reports as affected: {update:?}"
    );

    // The clone must now read the new value — read over native, on the same
    // session, still scoped to `ncw_tgt`.
    let read_clone = send_sql(&mut native_stream, 3, "SELECT v FROM docs WHERE id = 'a'").await;
    assert_eq!(
        read_clone.status,
        ResponseStatus::Ok,
        "read from clone after update must succeed: {read_clone:?}"
    );
    let clone_rows = read_clone.rows.expect("clone rows present");
    assert_eq!(
        clone_rows.len(),
        1,
        "one row expected in clone: {clone_rows:?}"
    );
    assert_eq!(
        clone_rows[0][0],
        Value::Integer(99),
        "the clone must reflect the native UPDATE: {clone_rows:?}"
    );
    drop(native_stream);

    // The source row must be untouched — a bug here would mean the native
    // write skipped copy-up and mutated the source directly.
    srv.exec("USE DATABASE ncw_src")
        .await
        .expect("use source database for verification");
    let source_rows = srv
        .query_rows("SELECT v FROM docs WHERE id = 'a'")
        .await
        .expect("select source row");
    assert_eq!(
        source_rows,
        vec![vec!["1".to_string()]],
        "the clone's source must be unchanged by the native UPDATE on the clone: {source_rows:?}"
    );

    srv.graceful_shutdown().await;
}

/// `REFRESH MATERIALIZED VIEW` against a view whose target collection is a
/// `Shadowed` clone is refused at its `TRUNCATE` step (SQLSTATE `55006`),
/// before the synthesized per-row `INSERT`s ever run — `TRUNCATE` clears
/// only target-local rows with no tombstone, so silently letting it through
/// would leave the clone's stale source rows readable after a "successful"
/// refresh. Reads against the same view stay unaffected by the refusal.
/// `ALTER DATABASE ... MATERIALIZE` clears the refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_materialized_view_truncate_refused_on_shadowed_clone_target() {
    let srv = TestServer::start().await;

    srv.exec("CREATE DATABASE mvw_src")
        .await
        .expect("create source database");
    srv.exec("USE DATABASE mvw_src")
        .await
        .expect("use source database");
    srv.exec("CREATE COLLECTION mv_src_coll (id TEXT PRIMARY KEY, v INT)")
        .await
        .expect("create source collection");
    srv.exec("INSERT INTO mv_src_coll (id, v) VALUES ('a', 1)")
        .await
        .expect("seed source row");
    srv.exec("CREATE MATERIALIZED VIEW mv_view ON mv_src_coll AS SELECT id, v FROM mv_src_coll")
        .await
        .expect("create materialized view");
    srv.exec("REFRESH MATERIALIZED VIEW mv_view")
        .await
        .expect("initial refresh in the source database");

    srv.exec("USE DATABASE default")
        .await
        .expect("use default database");
    srv.exec("CLONE DATABASE mvw_tgt FROM mvw_src")
        .await
        .expect("clone database (Shadowed by default)");

    srv.exec("USE DATABASE mvw_tgt")
        .await
        .expect("use cloned database");

    // Reads must keep working against the Shadowed clone's view: delegated
    // through to the source row the initial refresh wrote.
    let rows = srv
        .query_rows("SELECT id, v FROM mv_view")
        .await
        .expect("read on shadowed clone view must still succeed");
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "1".to_string()]],
        "expected exactly the delegated source row: {rows:?}"
    );

    // The write is refused: SQLSTATE 55006 (CLONE_WRITE_REQUIRES_MATERIALIZE),
    // checked as a code, not a message substring. If the refusal were
    // removed, this REFRESH would report success — the old, broken
    // behaviour — so asserting the code is what actually falsifies the fix.
    let refresh_err = srv
        .client
        .simple_query("REFRESH MATERIALIZED VIEW mv_view")
        .await
        .expect_err("refresh against a Shadowed clone target must be refused");
    let sqlstate = refresh_err
        .as_db_error()
        .expect("refusal must be a structured database error")
        .code()
        .code();
    assert_eq!(
        sqlstate, "55006",
        "expected CLONE_WRITE_REQUIRES_MATERIALIZE, got: {refresh_err:?}"
    );

    // The refusal must not have truncated the target: the view still reads
    // the one delegated source row, unchanged.
    let rows = srv
        .query_rows("SELECT id, v FROM mv_view")
        .await
        .expect("read after refused refresh must still succeed");
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "1".to_string()]],
        "refused refresh must not have truncated the view: {rows:?}"
    );

    // Materialize the clone, then the same refresh must succeed.
    srv.exec("USE DATABASE default")
        .await
        .expect("use default database");
    srv.exec("ALTER DATABASE mvw_tgt MATERIALIZE")
        .await
        .expect("materialize the clone");
    srv.exec("USE DATABASE mvw_tgt")
        .await
        .expect("use materialized clone database");

    srv.exec("REFRESH MATERIALIZED VIEW mv_view")
        .await
        .expect("refresh after MATERIALIZE must succeed");
    let rows = srv
        .query_rows("SELECT id, v FROM mv_view")
        .await
        .expect("scan refreshed materialized view");
    assert_eq!(
        rows,
        vec![vec!["a".to_string(), "1".to_string()]],
        "materialized refresh must leave exactly the one row: {rows:?}"
    );

    srv.graceful_shutdown().await;
}
