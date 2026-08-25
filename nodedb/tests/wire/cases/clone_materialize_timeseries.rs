// SPDX-License-Identifier: BUSL-1.1

//! `ALTER DATABASE clone MATERIALIZE` for the Timeseries engine.
//!
//! Seeds 20 rows into a timeseries source, clones, materializes, and verifies
//! all rows are readable in the clone. The ingest measurement-name validator
//! now permits `/` so db-qualified collection names (`{db_id}/{name}`) work.

use crate::harness::TestServer;

#[tokio::test(flavor = "multi_thread")]
async fn timeseries_clone_materialize_copies_all_source_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE ts_mat_src")
        .await
        .expect("CREATE DATABASE ts_mat_src");
    client
        .simple_query("USE DATABASE ts_mat_src")
        .await
        .expect("USE ts_mat_src");
    client
        .simple_query(
            "CREATE COLLECTION metrics \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, sensor TEXT, value FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION metrics");

    for i in 0..20u32 {
        let ts = (i as u64) * 1000 + 1000;
        client
            .simple_query(&format!(
                "INSERT INTO metrics (id, ts, sensor, value) \
                 VALUES ('m{i}', {ts}, 'cpu', {i}.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT m{i}: {e}"));
    }

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE ts_mat_clone FROM ts_mat_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("ALTER DATABASE ts_mat_clone MATERIALIZE")
        .await
        .expect("ALTER DATABASE ts_mat_clone MATERIALIZE");

    client
        .simple_query("USE DATABASE ts_mat_clone")
        .await
        .expect("USE ts_mat_clone");

    let rows = client
        .simple_query("SELECT id FROM metrics")
        .await
        .expect("SELECT id FROM metrics in clone");

    let count = rows
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        count, 20,
        "all 20 source rows must be readable in the materialized timeseries clone"
    );
}
