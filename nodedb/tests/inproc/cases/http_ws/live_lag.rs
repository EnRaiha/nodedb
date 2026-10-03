// SPDX-License-Identifier: BUSL-1.1

//! A LIVE SELECT flooded past its buffers never skips an event silently.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb::config::auth::AuthMode;
use tokio_tungstenite::tungstenite::Message;

use crate::cases::http_support::{create_orders, start_http};
use crate::cases::http_ws_support::{connect_ws, next_ws_json};

/// A LIVE SELECT flooded past its buffers must never silently skip events: the
/// client either receives every change in order, or is told to reset.
///
/// This deliberately does NOT assert that a reset always happens. Whether the
/// broadcast channel actually overflows depends on how much the WS socket and
/// the forwarder absorb before backpressure reaches it, and socket buffer sizes
/// are a property of the machine, not of this code — on an idle host the
/// subscriber keeps up and delivering all 4608 events is the correct outcome.
/// Asserting the reset unconditionally made this fail on fast machines while
/// the server was behaving perfectly. The invariant that always holds, and the
/// one a client depends on, is the absence of a silent gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_live_lag_never_silently_skips_events() {
    let srv = start_http(AuthMode::Trust).await;
    create_orders(&srv).await;
    let mut ws = connect_ws(&srv).await;
    ws.send(Message::Text(
        serde_json::json!({
            "id": 1,
            "method": "live",
            "params": {"sql": "LIVE SELECT * FROM orders"}
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("open LIVE SELECT subscription");
    let ack = next_ws_json(&mut ws, "LIVE SELECT acknowledgement").await;
    let subscription_id = ack["result"]["subscription_id"]
        .as_u64()
        .expect("LIVE SELECT acknowledgement must include a subscription id");
    tokio::task::yield_now().await;

    // Do not read while overflowing both the 256-message WS forwarder and
    // the 4096-entry broadcast channel. Large, bounded notifications ensure
    // the socket sender applies backpressure before the broadcast receiver
    // observes its lag. Each transaction commits one replicated entry whose
    // rows publish together. The zero-padded sequence makes key order and
    // insertion order agree, so the order check holds whichever of the two
    // the commit publishes in.
    let document_suffix = "x".repeat(2_048);
    const PUBLISHED: u64 = 4_096 + 512;
    const ROWS_PER_COMMIT: u64 = 512;
    for batch in 0..PUBLISHED / ROWS_PER_COMMIT {
        srv.node.exec("BEGIN").await.expect("begin");
        for sequence in batch * ROWS_PER_COMMIT..(batch + 1) * ROWS_PER_COMMIT {
            srv.node
                .exec(&format!(
                    "INSERT INTO orders {{ id: 'lagged-order-{sequence:05}-{document_suffix}' }}"
                ))
                .await
                .unwrap_or_else(|error| panic!("insert lagged order {sequence}: {error}"));
        }
        srv.node.exec("COMMIT").await.expect("commit");
    }

    // Drain until either a reset arrives or every published event has been
    // seen. Bounded by total elapsed time, not by a per-read timeout or a
    // message budget: how much arrives before backpressure reaches the
    // broadcast channel varies with machine load, and a stall mid-stream is
    // only a failure if the whole drain overruns.
    let mut delivered: Vec<u64> = Vec::with_capacity(PUBLISHED as usize);
    let mut reset: Option<serde_json::Value> = None;
    tokio::time::timeout(Duration::from_secs(30), async {
        while reset.is_none() && (delivered.len() as u64) < PUBLISHED {
            let Some(message) = ws.next().await else {
                panic!(
                    "WS stream ended after {} notifications with neither a reset nor the \
                     full event set",
                    delivered.len()
                );
            };
            let message = match message {
                Ok(Message::Text(text)) => serde_json::from_str::<serde_json::Value>(&text)
                    .unwrap_or_else(|e| panic!("invalid JSON in live notification: {e}")),
                Ok(other) => panic!("expected Text frame in live stream, got {other:?}"),
                Err(e) => panic!("WS error after {} notifications: {e}", delivered.len()),
            };
            if message["method"] == "reset_required" {
                reset = Some(message);
                continue;
            }
            let document_id = message["params"]["document_id"]
                .as_str()
                .unwrap_or_else(|| panic!("live notification without document_id: {message}"));
            let sequence: u64 = document_id
                .trim_start_matches("lagged-order-")
                .split('-')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| panic!("unexpected document_id shape: {document_id}"));
            delivered.push(sequence);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "LIVE SELECT stalled: {} of {PUBLISHED} events delivered, no reset",
            delivered.len()
        )
    });

    match reset {
        // Events were dropped, and the client was told so — the whole point of
        // the reset. Which events were lost is not asserted: that is exactly
        // the information the reset exists to say is unavailable.
        Some(reset) => {
            assert_eq!(reset["params"]["subscription_id"], subscription_id);
            assert_eq!(reset["params"]["reason"], "change stream lagged");
        }
        // No reset, so the stream claims to be complete — hold it to that.
        // A gap here is the silent-loss bug: the client would have no way to
        // know it missed a change.
        None => {
            let expected: Vec<u64> = (0..PUBLISHED).collect();
            assert_eq!(
                delivered, expected,
                "LIVE SELECT delivered no reset_required, so every published event must have \
                 arrived exactly once and in publication order"
            );
        }
    }
}
