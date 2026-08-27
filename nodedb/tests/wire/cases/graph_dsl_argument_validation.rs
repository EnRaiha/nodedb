// SPDX-License-Identifier: BUSL-1.1

//! The Graph DSL must reject arguments outside its own vocabulary.
//!
//! Two contracts, both about telling the caller what is wrong:
//!
//! 1. A clause value the DSL does not recognise is refused, not quietly
//!    replaced by the default. Defaulting an *omitted* clause is correct and
//!    documented; defaulting a *misspelled* one returns a confidently wrong
//!    answer to a question the caller did not ask.
//! 2. A required clause that is missing is reported as a missing clause. The
//!    variant parsers return `Option`, so a malformed graph statement is
//!    indistinguishable from input that was never a graph statement, and the
//!    caller is told `GRAPH` is not an SQL statement at all.
//!
//! Sibling numeric-range coverage lives in `graph_dsl_handlers.rs`
//! (`graph_traverse_rejects_absurd_depth` and friends), which rejects values
//! that parse but are out of range. These cover values that never parsed.

use crate::harness::TestServer;

/// One directed edge `a -> b`. Traversing from `b` reaches `a` only on an
/// inbound walk, which is what makes the direction argument observable
/// rather than merely accepted.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!("CREATE COLLECTION {collection}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "GRAPH INSERT EDGE IN '{collection}' FROM 'a' TO 'b' TYPE 'knows'"
        ))
        .await
        .unwrap();
}

// ── 1. Unrecognised clause values are refused ────────────────────────

/// `INBOUND` is a plausible misspelling of `IN` and is not in the documented
/// vocabulary (`in`, `out`, `both`). It must be refused, not read as `out`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_rejects_unrecognised_direction_word() {
    let server = TestServer::start().await;
    seed(&server, "gdir_word").await;

    server
        .expect_error(
            "GRAPH TRAVERSE IN 'gdir_word' FROM 'b' DEPTH 1 DIRECTION INBOUND",
            "INBOUND",
        )
        .await;
}

/// An arbitrary token must be refused rather than silently traversing `out`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_rejects_arbitrary_direction_token() {
    let server = TestServer::start().await;
    seed(&server, "gdir_arb").await;

    server
        .expect_error(
            "GRAPH TRAVERSE IN 'gdir_arb' FROM 'b' DEPTH 1 DIRECTION BANANA",
            "BANANA",
        )
        .await;
}

/// `GRAPH NEIGHBORS` reads the same direction helper and must refuse the
/// same way — the flaw is in the shared helper, not in one variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbors_rejects_unrecognised_direction() {
    let server = TestServer::start().await;
    seed(&server, "gdir_neigh").await;

    server
        .expect_error(
            "GRAPH NEIGHBORS IN 'gdir_neigh' OF 'b' DIRECTION BANANA",
            "BANANA",
        )
        .await;
}

/// A non-numeric `DEPTH` must be refused rather than falling back to the
/// default of 2 — same shape as the direction default, different helper.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_rejects_non_numeric_depth() {
    let server = TestServer::start().await;
    seed(&server, "gdepth_word").await;

    server
        .expect_error(
            "GRAPH TRAVERSE IN 'gdepth_word' FROM 'b' DEPTH banana",
            "banana",
        )
        .await;
}

/// A non-numeric `MAX_DEPTH` must be refused rather than defaulting to 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_rejects_non_numeric_max_depth() {
    let server = TestServer::start().await;
    seed(&server, "gmaxdepth_word").await;

    server
        .expect_error(
            "GRAPH PATH IN 'gmaxdepth_word' FROM 'a' TO 'b' MAX_DEPTH banana",
            "banana",
        )
        .await;
}

// ── 2. Missing required clauses name the missing clause ──────────────

/// Omitting `IN <collection>` must say so. The variant parser returns `None`,
/// the input falls through to the general SQL parser, and the caller is told
/// `GRAPH` is not an SQL statement — which points away from the real cause
/// and is the first error anyone upgrading past a required-`IN` release hits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_without_in_clause_names_the_missing_clause() {
    let server = TestServer::start().await;
    seed(&server, "gmiss_trav").await;

    let error = server
        .query_text("GRAPH TRAVERSE FROM 'b' DEPTH 1")
        .await
        .expect_err("a GRAPH TRAVERSE with no IN clause must fail");

    assert!(
        !error.contains("an SQL statement"),
        "the error must name the missing clause, not claim GRAPH is not SQL: {error}"
    );
    assert!(
        error.contains("IN"),
        "the error must name the missing IN clause: {error}"
    );
}

/// Same contract for `GRAPH NEIGHBORS`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn neighbors_without_in_clause_names_the_missing_clause() {
    let server = TestServer::start().await;
    seed(&server, "gmiss_neigh").await;

    let error = server
        .query_text("GRAPH NEIGHBORS OF 'b'")
        .await
        .expect_err("a GRAPH NEIGHBORS with no IN clause must fail");

    assert!(
        !error.contains("an SQL statement"),
        "the error must name the missing clause: {error}"
    );
}

/// Same contract for `GRAPH PATH`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn path_without_in_clause_names_the_missing_clause() {
    let server = TestServer::start().await;
    seed(&server, "gmiss_path").await;

    let error = server
        .query_text("GRAPH PATH FROM 'a' TO 'b'")
        .await
        .expect_err("a GRAPH PATH with no IN clause must fail");

    assert!(
        !error.contains("an SQL statement"),
        "the error must name the missing clause: {error}"
    );
}

// ── Guards: the valid vocabulary keeps working ───────────────────────

/// `DIRECTION IN` reaches the inbound neighbour. Without this the refusal
/// tests above could pass on a build that refused every direction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_with_valid_inbound_direction_reaches_source() {
    let server = TestServer::start().await;
    seed(&server, "gdir_ok").await;

    let rows = server
        .query_text_joined("GRAPH TRAVERSE IN 'gdir_ok' FROM 'b' DEPTH 1 DIRECTION IN")
        .await
        .expect("DIRECTION IN is documented vocabulary and must succeed");

    assert!(
        rows.iter().any(|r| r.contains('a')),
        "an inbound walk from 'b' must reach 'a': {rows:?}"
    );
}

/// An omitted `DIRECTION` still defaults to `out`. Refusing unrecognised
/// values must not turn the documented default into an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_without_direction_still_defaults() {
    let server = TestServer::start().await;
    seed(&server, "gdir_default").await;

    server
        .query_text("GRAPH TRAVERSE IN 'gdir_default' FROM 'a' DEPTH 1")
        .await
        .expect("an omitted DIRECTION must keep defaulting, not error");
}

/// An omitted `DEPTH` still defaults. Same guard for the numeric helper.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn traverse_without_depth_still_defaults() {
    let server = TestServer::start().await;
    seed(&server, "gdepth_default").await;

    server
        .query_text("GRAPH TRAVERSE IN 'gdepth_default' FROM 'a'")
        .await
        .expect("an omitted DEPTH must keep defaulting, not error");
}
