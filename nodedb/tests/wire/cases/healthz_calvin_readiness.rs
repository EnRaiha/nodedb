// SPDX-License-Identifier: BUSL-1.1

//! `/healthz` must not report ready before a cross-shard write can be
//! sequenced. The real binary defaults `single_node_calvin = true`
//! (`config/server/section.rs`), so a standalone server always runs a Calvin
//! sequencer, and `submit_calvin_routed`
//! (`control/planner/calvin/submit.rs`) refuses a submit with "no sequencer
//! leader elected yet" until the sequencer group elects one. A client that
//! waits for `/healthz` and then writes must never see that refusal.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

/// Fetch the raw `/healthz` response (status line + headers + body).
async fn fetch_healthz(http_port: u16) -> String {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("connect to /healthz");
    let req = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.expect("write healthz request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read healthz response");
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn healthz_ready_implies_cross_shard_write_is_not_refused() {
    // `start()` returns only after `/healthz` answered 200.
    let server = TestServer::start().await;

    let response = fetch_healthz(server.http_port).await;
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "/healthz must still be 200 for a served node: {response}"
    );
    assert!(
        !response.contains("sequencer_leader_pending")
            && !response.contains("sequencer_epoch_seed_pending"),
        "a 200 /healthz must never carry a sequencer-pending reason: {response}"
    );

    server
        .exec(
            "CREATE COLLECTION healthz_calvin_probe \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v FLOAT) \
             WITH (engine='timeseries')",
        )
        .await
        .expect("CREATE COLLECTION healthz_calvin_probe");

    // The very first write after readiness, with no retry: the readiness
    // signal itself has to carry the guarantee.
    let err = match server
        .exec("INSERT INTO healthz_calvin_probe (id, ts, v) VALUES ('probe', 0, 0.0)")
        .await
    {
        Ok(()) => return,
        Err(e) => e,
    };
    assert!(
        !err.contains("no sequencer leader elected yet"),
        "/healthz reported ready but the first cross-shard write was refused for want of a \
         sequencer leader: {err}"
    );
    panic!("cross-shard write after /healthz failed for an unrelated reason: {err}");
}
