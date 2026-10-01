// SPDX-License-Identifier: BUSL-1.1

//! `HAVING` must filter a timeseries aggregate, or refuse it — never vanish.
//!
//! The timeseries engine rule lowers every aggregate to a native
//! `SqlPlan::TimeseriesScan`. That plan carries `group_by`, `aggregates`,
//! `filters`, `gap_fill`, `limit` and `sort_keys` — but no `having` slot, and
//! the rule never reads `params.having`. Every other engine rule forwards it
//! (`columnar.rs:148`, `document_schemaless.rs:153`, `document_strict.rs:153`,
//! `kv.rs:128`, `spatial.rs:140` all write `having: p.having`).
//!
//! So a `HAVING` over a timeseries aggregate is silently dropped: the planner
//! builds the predicate (`engine_rules/params.rs:146`), the rule discards it,
//! and the query answers every group the `WHERE` clause left. The user gets
//! rows they explicitly filtered out, with no error, which is worse than a
//! refusal: a refusal tells them the engine cannot do it.
//!
//! The engine refuses the clause rather than filter it, because the native
//! `TimeseriesScan` has no slot for a predicate over the aggregate result. So
//! these tests assert the refusal — a typed error naming the clause and the
//! collection — together with the two controls that make the refusal meaningful:
//! the same aggregate without `HAVING` still answers, and `WHERE`, which filters
//! input rows rather than group results, still filters. What must never happen is
//! the predicate disappearing while the statement reports success.
//!
//! `having_matrix_all_engines.rs` carries the same clause across every engine and
//! accepts either outcome, because the engines that can express the predicate do
//! filter it.

use crate::harness::TestServer;

/// Seed three hosts with distinguishable counts, so a `HAVING` that fires and
/// one that is dropped produce different row sets.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} (ts TIMESTAMP TIME_KEY, host TEXT, value FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");

    let mut rows = Vec::new();
    // host_a: 3 events, host_b: 2, host_c: 1.
    for (host, count) in [("host_a", 3), ("host_b", 2), ("host_c", 1)] {
        for i in 0..count {
            rows.push(format!("('2020-03-05 1{i}:00:00', '{host}', 1.0)"));
        }
    }
    server
        .exec(&format!(
            "INSERT INTO {collection} (ts, host, value) VALUES {}",
            rows.join(", ")
        ))
        .await
        .expect("seed the events");
}

/// Without `HAVING`, every host groups. This is the control: it proves the
/// grouping itself works, so a failure above it is the `HAVING` clause and not
/// the aggregate.
#[tokio::test]
async fn group_by_without_having_returns_every_group() {
    let server = TestServer::start().await;
    seed(&server, "ts_no_having").await;

    let rows = server
        .query_named_rows("SELECT host, COUNT(*) FROM ts_no_having GROUP BY host")
        .await
        .expect("GROUP BY without HAVING must answer");

    assert_eq!(
        rows.len(),
        3,
        "all three hosts group when nothing filters them: {rows:?}"
    );
}

/// A `HAVING` over a timeseries aggregate is refused with a typed error naming
/// the collection, because the native `TimeseriesScan` cannot express a
/// predicate over the aggregate result.
///
/// The defect this pins is the silent alternative: before the fix the predicate
/// reached `plan_aggregate`, no slot carried it, and the statement answered
/// every group the `WHERE` clause left — rows the user had explicitly filtered
/// out, with no error. A refusal is the honest outcome; silently widening the
/// result set is not.
#[tokio::test]
async fn having_is_refused_not_silently_dropped() {
    let server = TestServer::start().await;
    seed(&server, "ts_having").await;

    let error = server
        .query_named_rows("SELECT host, COUNT(*) FROM ts_having GROUP BY host HAVING COUNT(*) > 1")
        .await
        .expect_err("HAVING over a timeseries aggregate must refuse, not answer");

    let message = error.to_string();
    assert!(
        message.contains("HAVING") && message.contains("ts_having"),
        "the refusal must name the clause and the collection, got: {message}"
    );
    assert!(
        message.contains("WHERE"),
        "the refusal must name the alternative that works, got: {message}"
    );
}

/// The guard is narrow: a `HAVING`-less aggregate still answers. Without this,
/// a refusal that swallowed every timeseries aggregate would pass the test
/// above, and the suite would not notice.
#[tokio::test]
async fn an_aggregate_without_having_still_answers() {
    let server = TestServer::start().await;
    seed(&server, "ts_still_ok").await;

    let rows = server
        .query_named_rows("SELECT host, COUNT(*) FROM ts_still_ok GROUP BY host")
        .await
        .expect("an aggregate without HAVING must still answer");
    assert_eq!(rows.len(), 3, "all three hosts group: {rows:?}");
}

/// The same predicate written as a `WHERE` filter does work, which is what makes
/// the silent drop so hard to notice: the two look alike and only one applies.
#[tokio::test]
async fn where_equivalent_still_filters() {
    let server = TestServer::start().await;
    seed(&server, "ts_where").await;

    let rows = server
        .query_named_rows(
            "SELECT host, COUNT(*) FROM ts_where WHERE host <> 'host_c' GROUP BY host",
        )
        .await
        .expect("WHERE must filter");

    let hosts: Vec<String> = rows.iter().filter_map(|r| r.get("host").cloned()).collect();
    assert_eq!(
        hosts.len(),
        2,
        "the WHERE equivalent excludes host_c: {rows:?}"
    );
}

/// The other shape that dropped `HAVING`: no `GROUP BY`, and a projection whose
/// only aggregate lives inside the predicate.
///
/// `has_aggregation` decided the statement was not an aggregate one, so the
/// planner took the non-aggregate path and never read the clause. The predicate
/// then vanished and every row came back.
#[tokio::test]
async fn having_without_group_by_is_refused_not_dropped() {
    let server = TestServer::start().await;
    seed(&server, "ts_bare_having").await;

    let error = server
        .query_named_rows("SELECT host FROM ts_bare_having HAVING COUNT(*) > 1")
        .await
        .expect_err("HAVING without GROUP BY must refuse on a timeseries collection");

    let message = error.to_string();
    assert!(
        message.contains("HAVING") && message.contains("ts_bare_having"),
        "the refusal must name the clause and the collection, got: {message}"
    );
}
