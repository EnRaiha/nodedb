// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for the CRDT conflict-policy SQL DDL surface:
//!
//! - `ALTER COLLECTION <name> SET ON CONFLICT <policy> FOR <kind>`
//! - `SHOW CONFLICT POLICY ON <name>`

use crate::harness::TestServer;

/// ALTER … LAST_WRITER_WINS FOR UNIQUE succeeds and SHOW reflects the change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_last_writer_wins_and_show() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test1 (id INT)")
        .await
        .expect("create collection");

    server
        .exec("ALTER COLLECTION cp_test1 SET ON CONFLICT LAST_WRITER_WINS FOR UNIQUE")
        .await
        .expect("alter set on conflict");

    let rows = server
        .query_text("SHOW CONFLICT POLICY ON cp_test1")
        .await
        .expect("show conflict policy");

    assert_eq!(rows.len(), 1, "expected one row");
    let policy_json = &rows[0];
    assert!(
        policy_json.contains("LastWriterWins"),
        "unique field must be LastWriterWins, got: {policy_json}"
    );
}

/// Two ALTERs targeting different constraint kinds must both be reflected
/// after the second call — read-modify-write must not clobber unrelated fields.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_alters_preserve_both_fields() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test2 (id INT)")
        .await
        .expect("create collection");

    server
        .exec("ALTER COLLECTION cp_test2 SET ON CONFLICT LAST_WRITER_WINS FOR UNIQUE")
        .await
        .expect("first alter");

    server
        .exec("ALTER COLLECTION cp_test2 SET ON CONFLICT ESCALATE_TO_DLQ FOR CHECK")
        .await
        .expect("second alter");

    let rows = server
        .query_text("SHOW CONFLICT POLICY ON cp_test2")
        .await
        .expect("show conflict policy");

    assert_eq!(rows.len(), 1, "expected one row");
    let policy_json = &rows[0];
    assert!(
        policy_json.contains("LastWriterWins"),
        "unique must still be LastWriterWins after second ALTER, got: {policy_json}"
    );
    assert!(
        policy_json.contains("EscalateToDlq"),
        "check must be EscalateToDlq, got: {policy_json}"
    );
}

/// SHOW on a collection with no explicit policy must still return the ephemeral default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn show_returns_default_when_no_policy_set() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test3 (id INT)")
        .await
        .expect("create collection");

    let rows = server
        .query_text("SHOW CONFLICT POLICY ON cp_test3")
        .await
        .expect("show conflict policy on fresh collection");

    assert_eq!(rows.len(), 1, "expected one row even for default policy");
    let policy_json = &rows[0];
    // Ephemeral default has RenameSuffix for unique and CascadeDefer for foreign_key.
    assert!(
        policy_json.contains("RenameSuffix"),
        "default unique policy must be RenameSuffix, got: {policy_json}"
    );
}

/// Unknown policy keyword must produce a typed parse error (SQLSTATE 42601).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_policy_keyword_is_parse_error() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test4 (id INT)")
        .await
        .expect("create collection");

    server
        .expect_error(
            "ALTER COLLECTION cp_test4 SET ON CONFLICT BOGUS_POLICY FOR UNIQUE",
            "unknown conflict policy keyword",
        )
        .await;
}

/// Unknown constraint kind must produce a typed parse error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_constraint_kind_is_parse_error() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test5 (id INT)")
        .await
        .expect("create collection");

    server
        .expect_error(
            "ALTER COLLECTION cp_test5 SET ON CONFLICT LAST_WRITER_WINS FOR BAD_KIND",
            "unknown constraint kind",
        )
        .await;
}

/// CUSTOM policy keyword must be rejected with a message pointing to the native protocol.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_policy_rejected_with_hint() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test6 (id INT)")
        .await
        .expect("create collection");

    server
        .expect_error(
            "ALTER COLLECTION cp_test6 SET ON CONFLICT CUSTOM FOR UNIQUE",
            "native NodeDB protocol",
        )
        .await;
}

/// CASCADE_DEFER policy round-trips correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_defer_roundtrip() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cp_test7 (id INT)")
        .await
        .expect("create collection");

    server
        .exec("ALTER COLLECTION cp_test7 SET ON CONFLICT CASCADE_DEFER FOR FOREIGN_KEY")
        .await
        .expect("alter");

    let rows = server
        .query_text("SHOW CONFLICT POLICY ON cp_test7")
        .await
        .expect("show");

    let policy_json = &rows[0];
    assert!(
        policy_json.contains("CascadeDefer"),
        "foreign_key must be CascadeDefer, got: {policy_json}"
    );
}
