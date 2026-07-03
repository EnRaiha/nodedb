// SPDX-License-Identifier: BUSL-1.1

//! A `CREATE MATERIALIZED VIEW ... STREAMING` created over SQL must wire the
//! Event-Plane incremental aggregation: the DDL handler has to register a
//! streaming MV definition into `mv_registry` so that writes to the source
//! collection — fanned out through the MV's source change stream — drive an
//! O(1)-per-event incremental aggregate update.
//!
//! Read surface: streaming-MV aggregate state lives in the in-memory
//! `mv_registry` (per-group partial aggregate `MvState`), not in a batch
//! target collection like `REFRESH MATERIALIZED VIEW`. The harness exposes
//! `TestServer::shared`, so this test observes the aggregation directly on the
//! registry the Event Plane maintains. The default harness connection runs as
//! tenant id 1.

mod common;

use std::time::Duration;

use common::pgwire_harness::TestServer;

/// The default harness superuser (`nodedb`) is provisioned under tenant id 1.
const TENANT_ID: u64 = 1;

/// Streaming materialized view fed by a change stream must incrementally
/// aggregate source writes: two `active` orders and one `pending` order must
/// surface as per-group COUNT/SUM in the MV's live aggregate state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_mv_incrementally_aggregates_source_writes() {
    let server = TestServer::start().await;

    // Base collection whose writes feed the change stream.
    server.exec("CREATE COLLECTION smv_orders").await.unwrap();

    // Change stream over the base collection. Streaming MVs source from a
    // change stream (registered in `stream_registry`), not from the base
    // collection directly. Registered via catalog post-apply on CREATE.
    server
        .exec("CREATE CHANGE STREAM smv_order_changes ON smv_orders")
        .await
        .unwrap();

    // Streaming MV: aggregate per `status` with COUNT(*) and SUM(amount),
    // sourced from the change stream via the FROM clause. The `ON smv_orders`
    // clause names the lineage collection so the handler's source-existence
    // check passes; `STREAMING` selects incremental refresh mode.
    server
        .exec(
            "CREATE MATERIALIZED VIEW smv_order_stats ON smv_orders STREAMING AS \
             SELECT status, COUNT(*) AS cnt, SUM(amount) AS total \
             FROM smv_order_changes GROUP BY status",
        )
        .await
        .unwrap();

    // Writes to the base collection produce WriteEvents that the Event Plane
    // fans out to the change stream and, from there, into every streaming MV
    // sourced from that stream.
    server
        .exec("INSERT INTO smv_orders { id: 'o1', status: 'active', amount: 10 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o2', status: 'active', amount: 20 }")
        .await
        .unwrap();
    server
        .exec("INSERT INTO smv_orders { id: 'o3', status: 'pending', amount: 5 }")
        .await
        .unwrap();

    // The Event Plane consumes WriteEvents asynchronously. Poll the registry
    // until the MV state materializes both group keys (or time out). This is a
    // deterministic convergence poll, not a blind fixed sleep.
    let mut results: Vec<(String, Vec<(String, f64)>)> = Vec::new();
    for _ in 0..80 {
        if let Some(state) = server
            .shared
            .mv_registry
            .get_state(TENANT_ID, "smv_order_stats")
        {
            results = state.read_results();
            if results.len() >= 2 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A correctly-wired streaming MV registers its definition on CREATE and
    // then incrementally aggregates the three source writes into two groups.
    // Today the neutral DDL handler never registers a `StreamingMvDef`, so the
    // registry has no state for the view and this assertion fails.
    assert_eq!(
        results.len(),
        2,
        "streaming MV must aggregate source writes into two groups (active, pending); got {results:?}"
    );

    let active = results
        .iter()
        .find(|(k, _)| k == "active")
        .expect("`active` group must be present in streaming MV state");
    // Aggregate order matches the SELECT list: index 0 = COUNT(*), 1 = SUM(amount).
    assert!(
        (active.1[0].1 - 2.0).abs() < f64::EPSILON,
        "active COUNT(*) must be 2; got {active:?}"
    );
    assert!(
        (active.1[1].1 - 30.0).abs() < f64::EPSILON,
        "active SUM(amount) must be 10 + 20 = 30; got {active:?}"
    );

    let pending = results
        .iter()
        .find(|(k, _)| k == "pending")
        .expect("`pending` group must be present in streaming MV state");
    assert!(
        (pending.1[0].1 - 1.0).abs() < f64::EPSILON,
        "pending COUNT(*) must be 1; got {pending:?}"
    );
    assert!(
        (pending.1[1].1 - 5.0).abs() < f64::EPSILON,
        "pending SUM(amount) must be 5; got {pending:?}"
    );
}
