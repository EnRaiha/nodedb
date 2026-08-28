// SPDX-License-Identifier: BUSL-1.1

//! A `Shadowed` clone's `columnar` collection has no copy-on-write module
//! (`clone_write` only implements Document/KV point-op copy-up/tombstone).
//! Without a write-time refusal, an `UPDATE` against the clone would write
//! straight to target storage while the stale source row stays live —
//! `merge_msgpack_arrays` concatenates both on the next read with no dedup,
//! so the row silently doubles. This proves the fix instead: the write is
//! refused with `CLONE_WRITE_REQUIRES_MATERIALIZE` (SQLSTATE `55006`) while
//! reads keep working, and the write succeeds once `MATERIALIZE` runs.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn columnar_write_on_shadowed_clone_is_refused_then_succeeds_after_materialize() {
    let server = TestServer::start().await;

    server
        .exec("CREATE DATABASE ccwr_src")
        .await
        .expect("CREATE DATABASE ccwr_src");
    server
        .exec("USE DATABASE ccwr_src")
        .await
        .expect("USE ccwr_src");
    server
        .exec(
            "CREATE COLLECTION metrics \
             COLUMNS (id TEXT, reading TEXT) \
             WITH (engine='columnar')",
        )
        .await
        .expect("CREATE COLLECTION metrics");
    server
        .exec("INSERT INTO metrics (id, reading) VALUES ('m1', 'source-value')")
        .await
        .expect("INSERT m1");

    server
        .exec("USE DATABASE default")
        .await
        .expect("USE default");
    server
        .exec("CLONE DATABASE ccwr_clone FROM ccwr_src")
        .await
        .expect("CLONE DATABASE ccwr_clone");
    server
        .exec("USE DATABASE ccwr_clone")
        .await
        .expect("USE ccwr_clone");

    // Reads must keep working against the Shadowed clone: delegated through
    // to the source row, unaffected by the write-path refusal.
    let rows = server
        .query_named_rows("SELECT id, reading FROM metrics WHERE id = 'm1'")
        .await
        .expect("SELECT on shadowed columnar clone must still succeed");
    assert_eq!(rows.len(), 1, "expected exactly one delegated source row");
    assert_eq!(
        rows[0].get("reading").map(String::as_str),
        Some("source-value")
    );

    // The write is refused: SQLSTATE 55006 (CLONE_WRITE_REQUIRES_MATERIALIZE).
    // If the refusal were removed, this UPDATE would report success — the
    // Data Plane has nothing that rejects it — and the duplicate would only
    // surface on a later read, so asserting the write itself fails here is
    // what actually falsifies the fix.
    server
        .expect_error(
            "UPDATE metrics SET reading = 'clone-write' WHERE id = 'm1'",
            "55006",
        )
        .await;

    // The refusal must not have mutated target storage: still exactly the
    // one delegated source row, unchanged.
    let rows = server
        .query_named_rows("SELECT id, reading FROM metrics WHERE id = 'm1'")
        .await
        .expect("SELECT after refused write");
    assert_eq!(
        rows.len(),
        1,
        "refused write must not create a target-only row"
    );
    assert_eq!(
        rows[0].get("reading").map(String::as_str),
        Some("source-value")
    );

    // Materialize the clone, then the same UPDATE must succeed.
    server
        .exec("USE DATABASE default")
        .await
        .expect("USE default");
    server
        .exec("ALTER DATABASE ccwr_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE ccwr_clone MATERIALIZE");
    server
        .exec("USE DATABASE ccwr_clone")
        .await
        .expect("USE ccwr_clone");

    server
        .exec("UPDATE metrics SET reading = 'clone-write' WHERE id = 'm1'")
        .await
        .expect("UPDATE after MATERIALIZE must succeed");

    let rows = server
        .query_named_rows("SELECT id, reading FROM metrics WHERE id = 'm1'")
        .await
        .expect("SELECT after materialized write");
    assert_eq!(
        rows.len(),
        1,
        "materialized write must leave exactly one row, no duplicate: {rows:?}"
    );
    assert_eq!(
        rows[0].get("reading").map(String::as_str),
        Some("clone-write")
    );
}
