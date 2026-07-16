// SPDX-License-Identifier: BUSL-1.1

//! Real process-kill WAL-durability regression.
//!
//! A write acknowledged by the server (an `INSERT` that returned) must
//! survive a `kill -9`, because the write path acks the client only after
//! the WAL append is persisted. Reopening the same data directory on a
//! fresh process replays the WAL and must restore the row.

mod crash_harness;

use crash_harness::CrashHarness;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
async fn committed_write_survives_kill_9() {
    let mut h = CrashHarness::new();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    {
        let (client, connection) =
            tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
                .await
                .expect("connect before crash");
        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(
                "CREATE COLLECTION crash_kv (id TEXT PRIMARY KEY, v INT) WITH (engine='document_strict')",
            )
            .await
            .expect("create collection");
        client
            .batch_execute("INSERT INTO crash_kv (id, v) VALUES ('a', 42)")
            .await
            .expect("insert acknowledged");

        drop(client);
        let _ = conn_handle.await;
    }

    // Hard crash: no graceful shutdown, no extra flush.
    h.kill_9();

    // Fresh process, same data directory -> WAL replay on boot.
    h.reopen();

    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .expect("connect after recovery");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });

    // Use `simple_query` so the value comes back as text regardless of the
    // column's reported type OID — this is a durability smoke test, not a
    // wire-type-decoding test, so we only care that the value survived.
    let messages = client
        .simple_query("SELECT v FROM crash_kv WHERE id = 'a'")
        .await
        .expect("select after recovery");

    let recovered: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                Some(row.get("v").unwrap_or_default().to_string())
            }
            _ => None,
        })
        .collect();

    assert_eq!(
        recovered,
        vec!["42".to_string()],
        "committed write did not survive kill -9 + WAL replay (got {recovered:?})"
    );

    drop(client);
    let _ = conn_handle.await;

    // `h` drops here: kills any surviving process and removes the temp
    // data directory.
}
