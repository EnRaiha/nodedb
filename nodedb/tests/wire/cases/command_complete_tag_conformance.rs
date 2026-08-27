// SPDX-License-Identifier: BUSL-1.1

//! Pins two `CommandComplete` tag conformance fixes: the object-literal
//! `INSERT INTO t { ... }` path used to omit its affected-row count
//! entirely (a bare `INSERT` tag real `psql` cannot parse), and `TRUNCATE
//! TABLE` used to append a row count Postgres's tag never carries.
//!
//! `tokio_postgres::SimpleQueryMessage::CommandComplete` exposes only a
//! `u64`, derived by `tokio_postgres::query::extract_row_affected` as
//! `tag.rsplit(' ').next()` — the LAST whitespace-separated token, parsed as
//! an integer (falling back to `0` if that fails). This makes the two cases
//! below observable:
//!
//! - a bare tag (no trailing integer) always parses to `0` via the fallback;
//! - a tag with a trailing integer parses to that integer regardless of how
//!   many tokens precede it.
//!
//! It does NOT make the `INSERT` OID fix observable: `INSERT 1` (malformed,
//! oid omitted) and `INSERT 0 1` (correct) both end in `1`, so
//! `extract_row_affected` returns `1` either way. Distinguishing those two
//! requires reading the raw tag string, which `tokio_postgres`'s public API
//! never surfaces (only the derived count crosses the crate boundary) — so
//! that half of the fix has no assertion here that could fail against the
//! pre-fix code. The bare-tag and TRUNCATE-count regressions below are
//! within the driver's reach and are exercised directly.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// The row count carried by the first `CommandComplete` in `sql`'s response.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let messages = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"));
    messages
        .into_iter()
        .find_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .unwrap_or_else(|| panic!("statement reported no command tag: {sql}"))
}

/// Before the fix, `INSERT INTO t { ... }` (the object-literal insert path,
/// distinct from `INSERT ... VALUES`) returned `DdlResult::Status {
/// rows_affected: None, .. }`, which pgwire renders as a bare `INSERT` tag.
/// `extract_row_affected` falls back to `0` for a tag with no trailing
/// integer, so a real single-row insert misreported as touching 0 rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn object_literal_insert_reports_one_row_affected() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_conformance_obj_insert \
             (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));

    let count = affected(
        &server,
        "INSERT INTO tag_conformance_obj_insert { id: 'row1', v: 'hello' }",
    )
    .await;
    assert_eq!(
        count, 1,
        "object-literal insert must report 1 affected row, not fall back to 0 \
         via a bare CommandComplete tag"
    );
}

/// Before the fix, `TRUNCATE TABLE` rendered `TRUNCATE <rows-removed>` — a
/// count Postgres's `TRUNCATE TABLE` tag never carries. `extract_row_affected`
/// would then report the pre-truncate row count instead of falling back to
/// `0` for the count-less tag a real Postgres server sends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_table_reports_no_row_count() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION tag_conformance_truncate \
             (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')",
        )
        .await
        .unwrap_or_else(|e| panic!("create collection: {e}"));
    for id in ["a", "b", "c"] {
        server
            .exec(&format!(
                "INSERT INTO tag_conformance_truncate (id, v) VALUES ('{id}', 'x')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {id}: {e}"));
    }

    let count = affected(&server, "TRUNCATE TABLE tag_conformance_truncate").await;
    assert_eq!(
        count, 0,
        "TRUNCATE TABLE's tag carries no row count, so the driver's fallback \
         parse must read 0 — not the number of rows actually removed"
    );
}
