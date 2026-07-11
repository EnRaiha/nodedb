// SPDX-License-Identifier: BUSL-1.1

//! All-or-nothing atomicity of a `MERGE` whose UPDATE and INSERT arms both fire.
//!
//! Autocommit `MERGE` is Control-Plane-orchestrated: the matched UPDATE and the
//! NOT-MATCHED INSERT arms are applied under ONE redb write transaction, so a
//! constraint violation on the insert must roll back the update too — nothing
//! lands.
//!
//! The MERGE below UPDATEs an existing row (`a`.n → 100) AND INSERTs a new row
//! (`c`) whose `code` collides with a pre-existing UNIQUE value (`b`.code =
//! 'Y'). The insert's UNIQUE violation must abort the whole statement, leaving
//! BOTH the update and the insert un-applied.
//!
//! Fails pre-fix (and against any naive split): the raw per-row insert path
//! applied the matched UPDATE in its own commit before the insert ran, so
//! `a`.n would already be 100 when the insert failed — the update would survive
//! the abort.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_update_plus_failing_insert_is_all_or_nothing() {
    let server = TestServer::start().await;

    // Target with a UNIQUE secondary index on `code`.
    server
        .exec("CREATE COLLECTION matomic_target")
        .await
        .unwrap();
    server
        .exec("CREATE UNIQUE INDEX idx_matomic_code ON matomic_target (code)")
        .await
        .unwrap();

    // Pre-existing rows: `a` (updated by the MERGE) and `b` (holds code = 'Y',
    // the value the insert will collide with).
    server
        .exec("INSERT INTO matomic_target (id, code, n) VALUES ('a', 'A', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO matomic_target (id, code, n) VALUES ('b', 'Y', 2)")
        .await
        .unwrap();

    // Source: `a` matches (drives the UPDATE), `c` does not match (drives the
    // INSERT whose code = 'Y' collides with the pre-existing `b`).
    server
        .exec("CREATE COLLECTION matomic_source")
        .await
        .unwrap();
    server
        .exec("INSERT INTO matomic_source (id, n) VALUES ('a', 100)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO matomic_source (id, code) VALUES ('c', 'Y')")
        .await
        .unwrap();

    // The MERGE must fail on the insert's UNIQUE violation.
    let result = server
        .exec(
            "MERGE INTO matomic_target t \
             USING matomic_source s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET n = s.n \
             WHEN NOT MATCHED THEN INSERT (id, code) VALUES (s.id, s.code)",
        )
        .await;
    assert!(
        result.is_err(),
        "a MERGE whose INSERT arm violates a UNIQUE constraint must error, not \
         silently partially apply"
    );

    // The matched UPDATE was rolled back: `a`.n is still 1, not 100.
    let n_a = server
        .query_text("SELECT n FROM matomic_target WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        n_a,
        vec!["1".to_string()],
        "the matched UPDATE must be rolled back with the failed INSERT; got \
         a.n = {n_a:?} (pre-fix: the update committed before the insert failed)"
    );

    // The NOT-MATCHED INSERT did not land: only `a` and `b` remain.
    let ids = server
        .query_text("SELECT id FROM matomic_target ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        ids,
        vec!["a".to_string(), "b".to_string()],
        "a violating MERGE must leave the target unchanged; got {ids:?}"
    );
}
