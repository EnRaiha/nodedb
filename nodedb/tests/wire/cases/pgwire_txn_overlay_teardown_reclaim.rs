// SPDX-License-Identifier: BUSL-1.1

//! An abandoned pgwire transaction's Data-Plane staging overlay must be
//! reclaimed (not merely invisible) when its connection tears down without
//! COMMIT/ROLLBACK. Proven via the `nodedb_active_txn_overlays` Prometheus
//! gauge on `/metrics`, which must return to baseline after teardown.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

/// Fetch a single Prometheus gauge value from `/metrics` by exact name.
async fn fetch_metric(http_port: u16, name: &str) -> u64 {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("connect to /metrics");
    let req = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.expect("write metrics request");
    let mut body = String::new();
    stream
        .read_to_string(&mut body)
        .await
        .expect("read metrics response");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix(name)
            && rest.starts_with(' ')
            && let Ok(v) = rest.trim().parse::<u64>()
        {
            return v;
        }
    }
    panic!("metric {name} not found in /metrics response: {body}");
}

/// Poll the gauge until `pred` is satisfied or `deadline` elapses. Returns
/// the last observed value so a timeout panics with the actual value.
async fn poll_gauge(http_port: u16, deadline: Duration, pred: impl Fn(u64) -> bool) -> u64 {
    let start = Instant::now();
    loop {
        let value = fetch_metric(http_port, "nodedb_active_txn_overlays").await;
        if pred(value) || start.elapsed() >= deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_abandoned_txn_overlay_reclaimed_on_teardown() {
    let server = TestServer::start().await;

    let baseline = fetch_metric(server.http_port, "nodedb_active_txn_overlays").await;
    assert_eq!(baseline, 0, "gauge must start at zero: {baseline}");

    // Open a SEPARATE connection the test OWNS (not `server.client`, which the
    // harness owns) so we can close it mid-transaction. tokio-postgres runs the
    // socket in a spawned task; we keep its JoinHandle to abort it later.
    let conn_str = format!(
        "host=127.0.0.1 port={} user=nodedb dbname=default",
        server.pg_port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("owned pgwire connection must connect");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "CREATE COLLECTION pg_txn_overlay_teardown (id STRING PRIMARY KEY, n INT) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE must succeed");

    client
        .batch_execute("BEGIN")
        .await
        .expect("BEGIN must succeed");
    client
        .batch_execute("INSERT INTO pg_txn_overlay_teardown (id, n) VALUES ('a', 1)")
        .await
        .expect("in-tx INSERT must succeed");

    // The staged write must have materialized the overlay, bumping the gauge
    // above baseline -- confirms there is something to reclaim.
    let after_stage = poll_gauge(server.http_port, Duration::from_secs(5), |v| v > baseline).await;
    assert!(
        after_stage > baseline,
        "staged write must raise active_txn_overlays above baseline {baseline}, got {after_stage}"
    );

    // Abruptly abandon the connection -- no COMMIT/ROLLBACK. Dropping the
    // Client resolves the Connection future; aborting its task guarantees the
    // socket is dropped so the server sees EOF and runs `on_connection_end`.
    drop(client);
    conn_handle.abort();
    let _ = conn_handle.await;

    // The abandoned transaction's overlay must be reclaimed on teardown,
    // bringing the gauge back down to baseline.
    let after_teardown =
        poll_gauge(server.http_port, Duration::from_secs(5), |v| v == baseline).await;
    assert_eq!(
        after_teardown, baseline,
        "abandoned txn overlay must be reclaimed on connection teardown, \
         active_txn_overlays still {after_teardown} (baseline {baseline})"
    );

    // Belt-and-suspenders: the harness-owned autocommit connection must not see
    // the staged row -- it was never committed.
    let rows = server
        .query_text("SELECT n FROM pg_txn_overlay_teardown WHERE id = 'a'")
        .await
        .expect("post-teardown SELECT must succeed");
    assert!(
        rows.is_empty(),
        "the never-committed staged row must not be visible: {rows:?}"
    );
}
