// SPDX-License-Identifier: BUSL-1.1

//! Cross-runtime integration test.
//!
//! Proves the bridge works across real Tokio ↔ "TPC" thread boundaries:
//!
//! - Thread 1: Tokio runtime using `AsyncControlHandle` (async push/pop via eventfd).
//! - Thread 2: Simulated TPC core using raw epoll on eventfd (sync push/pop).
//!
//! This is the critical validation that the eventfd-based waking works across
//! different event loops.
//!
//! Requires the `tokio` feature: `cargo test -p nodedb-bridge --features tokio`

#![cfg(all(feature = "tokio", target_os = "linux"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use nodedb_bridge::async_bridge::BridgeChannel;

const MESSAGE_COUNT: u64 = 10_000;

/// Simulated TPC event loop using raw epoll + eventfd.
///
/// This is what Glommio/monoio would do internally — poll an fd, wake up,
/// process work, signal the other side.
fn tpc_event_loop(
    mut data: nodedb_bridge::async_bridge::DataHandle<u64, u64>,
    done: Arc<AtomicBool>,
) {
    let epoll_fd = unsafe { libc::epoll_create1(0) };
    assert!(epoll_fd >= 0, "epoll_create1 failed");

    // Register the request-available eventfd with epoll (edge-triggered).
    let req_fd = data.request_wake_fd();
    let mut event = libc::epoll_event {
        events: (libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: req_fd as u64,
    };
    let ret = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, req_fd, &mut event) };
    assert_eq!(ret, 0, "epoll_ctl failed");

    let mut processed = 0u64;
    let mut events = [libc::epoll_event { events: 0, u64: 0 }; 8];

    while processed < MESSAGE_COUNT {
        // epoll_wait with short timeout so we don't hang on edge-triggered misses.
        let nfds = unsafe { libc::epoll_wait(epoll_fd, events.as_mut_ptr(), 8, 10) };

        if nfds < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            panic!("epoll_wait failed: {err}");
        }

        // Consume the eventfd signal to re-arm edge trigger.
        if nfds > 0 {
            let _ = data.req_wake.consumer_wake.try_read();
        }

        // Drain all available requests.
        let mut batch = Vec::new();
        data.drain_requests(&mut batch, 512);

        for req in batch {
            loop {
                match data.try_send_response(req * 2) {
                    Ok(()) => break,
                    Err(nodedb_bridge::BridgeError::Full { .. }) => {
                        thread::yield_now();
                    }
                    Err(e) => panic!("TPC send error: {e}"),
                }
            }
            processed += 1;
        }
    }

    done.store(true, Ordering::Release);
    unsafe { libc::close(epoll_fd) };
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_to_tpc_via_eventfd() {
    let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(1024, 1024).unwrap();
    let control = bridge.control;
    let data = bridge.data;

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = Arc::clone(&done);

    // Spawn the TPC thread (raw epoll, no Tokio).
    let tpc_handle = thread::spawn(move || {
        tpc_event_loop(data, done_clone);
    });

    // Tokio side: wrap in AsyncControlHandle.
    let mut async_ctrl = nodedb_bridge::tokio_fd::AsyncControlHandle::new(control).unwrap();

    // Interleave sends and receives to avoid filling both queues.
    let mut sent = 0u64;
    let mut responses = Vec::with_capacity(MESSAGE_COUNT as usize);

    while responses.len() < MESSAGE_COUNT as usize {
        // Send a batch.
        while sent < MESSAGE_COUNT {
            match async_ctrl.inner.try_send_request(sent + 1) {
                Ok(()) => sent += 1,
                Err(nodedb_bridge::BridgeError::Full { .. }) => break,
                Err(e) => panic!("send error: {e}"),
            }
        }

        // Try to drain responses without blocking.
        loop {
            match async_ctrl.try_recv_response() {
                Ok(rsp) => responses.push(rsp),
                Err(nodedb_bridge::BridgeError::Empty) => break,
                // Producer may drop after sending all responses; drain what's left.
                Err(nodedb_bridge::BridgeError::Disconnected { .. }) => break,
                Err(e) => panic!("recv error: {e}"),
            }
        }

        // If we haven't sent everything and no responses came, yield.
        if responses.len() < MESSAGE_COUNT as usize {
            tokio::task::yield_now().await;
        }
    }

    tpc_handle.join().unwrap();

    // Verify all responses.
    responses.sort();
    assert_eq!(responses.len(), MESSAGE_COUNT as usize);
    for (i, rsp) in responses.iter().enumerate() {
        let expected = (i as u64 + 1) * 2;
        assert_eq!(
            *rsp, expected,
            "response mismatch at index {i}: got {rsp}, expected {expected}"
        );
    }

    assert!(done.load(Ordering::Acquire));
}

#[tokio::test]
async fn tpc_wakes_tokio_on_response() {
    let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
    let mut data = bridge.data;
    let control = bridge.control;

    let mut async_ctrl = nodedb_bridge::tokio_fd::AsyncControlHandle::new(control).unwrap();

    // Spawn a task waiting for a response (will block on eventfd).
    let recv_task = tokio::spawn(async move {
        let rsp = async_ctrl.recv_response().await.unwrap();
        (async_ctrl, rsp)
    });

    // Small delay to ensure the Tokio task is waiting.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Simulate TPC core sending a response + signaling eventfd.
    data.try_send_response(999).unwrap();

    // The recv task should wake up and complete.
    let (_ctrl, rsp) = tokio::time::timeout(Duration::from_secs(5), recv_task)
        .await
        .expect("recv should complete within 5s")
        .unwrap();

    assert_eq!(rsp, 999);
}
