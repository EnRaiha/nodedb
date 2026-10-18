// SPDX-License-Identifier: BUSL-1.1

//! Tokio `AsyncFd` integration for eventfd-based waking.
//!
//! This module wraps our `EventFd` in Tokio's `AsyncFd`, enabling truly async
//! push/pop operations on the Control Plane side:
//!
//! - `async_send_request()` — pushes to the SPSC ring; if full, awaits the
//!   eventfd signal from the consumer (TPC core freed a slot).
//! - `async_recv_response()` — pops from the SPSC ring; if empty, awaits the
//!   eventfd signal from the producer (TPC core sent a response).
//!
//! ## Why AsyncFd?
//!
//! Tokio's `AsyncFd` registers a raw file descriptor with the runtime's epoll
//! instance. When the eventfd becomes readable (the TPC core wrote to it),
//! Tokio wakes the waiting task — no polling, no busy-waiting.

use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;

use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use crate::async_bridge::ControlHandle;
use crate::error::{BridgeError, Result};
use crate::eventfd::{EventFd, WakePair};

/// One eventfd of a shared `WakePair`, watched by a Tokio `AsyncFd`.
///
/// Holds an `Arc` to the pair, so the descriptor stays open for the whole
/// life of the `AsyncFd`. No drop order elsewhere can close it early.
struct WatchedWakeFd {
    pair: Arc<WakePair>,
    select: fn(&WakePair) -> &EventFd,
}

impl AsRawFd for WatchedWakeFd {
    fn as_raw_fd(&self) -> RawFd {
        (self.select)(&self.pair).as_fd().as_raw_fd()
    }
}

/// The eventfd the consumer signals when the request queue frees a slot.
fn producer_wake(pair: &WakePair) -> &EventFd {
    &pair.producer_wake
}

/// The eventfd the producer signals when the response queue has data.
fn consumer_wake(pair: &WakePair) -> &EventFd {
    &pair.consumer_wake
}

/// Register one eventfd of `pair` with the current Tokio reactor.
fn watch(
    pair: &Arc<WakePair>,
    select: fn(&WakePair) -> &EventFd,
) -> std::io::Result<AsyncFd<WatchedWakeFd>> {
    let watched = WatchedWakeFd {
        pair: Arc::clone(pair),
        select,
    };
    // SAFETY: `watched` owns an `Arc` to the pair, so the descriptor stays
    // open for the whole life of the `AsyncFd`. `select` is a fixed function
    // of an immutable pair, so every `as_raw_fd` call returns the same fd.
    unsafe { AsyncFd::register_with_interest(watched, Interest::READABLE) }
        .map_err(|error| error.into_parts().1)
}

/// Async-capable Control Plane handle.
///
/// Wraps `ControlHandle` with Tokio `AsyncFd` instances for non-blocking
/// async push/pop across the bridge. Each `AsyncFd` shares ownership of
/// the eventfd it watches.
pub struct AsyncControlHandle<Req, Rsp> {
    /// AsyncFd for "request queue has space" signal (producer can push).
    req_space_fd: AsyncFd<WatchedWakeFd>,
    /// AsyncFd for "response available" signal (consumer can pop).
    rsp_ready_fd: AsyncFd<WatchedWakeFd>,
    /// The underlying synchronous handle.
    inner: ControlHandle<Req, Rsp>,
}

impl<Req, Rsp> AsyncControlHandle<Req, Rsp> {
    /// Wrap a `ControlHandle` with Tokio async I/O.
    ///
    /// Must be called from within a Tokio runtime context.
    pub fn new(handle: ControlHandle<Req, Rsp>) -> std::io::Result<Self> {
        let req_space_fd = watch(&handle.req_wake, producer_wake)?;
        let rsp_ready_fd = watch(&handle.rsp_wake, consumer_wake)?;

        Ok(Self {
            req_space_fd,
            rsp_ready_fd,
            inner: handle,
        })
    }

    /// Send a request to the Data Plane, awaiting if the queue is full.
    ///
    /// This is the primary async API for the Control Plane. It:
    /// 1. Tries to push immediately.
    /// 2. If full, awaits the eventfd signal from the TPC core (space freed).
    /// 3. Retries the push.
    pub async fn send_request(&mut self, req: Req) -> Result<()>
    where
        Req: Clone,
    {
        // Fast path: try immediate push.
        match self.inner.try_send_request(req.clone()) {
            Ok(()) => return Ok(()),
            Err(BridgeError::Full { .. }) => {}
            Err(e) => return Err(e),
        }

        // Slow path: wait for space.
        loop {
            // Wait for the eventfd to become readable (TPC core freed a slot).
            let mut guard =
                self.req_space_fd
                    .readable()
                    .await
                    .map_err(|_| BridgeError::Backpressure {
                        percent: 100,
                        threshold: 95,
                    })?;

            // Consume the eventfd signal.
            let _ = self.inner.req_wake.producer_wake.try_read();
            guard.clear_ready();

            // Retry push.
            match self.inner.try_send_request(req.clone()) {
                Ok(()) => return Ok(()),
                Err(BridgeError::Full { .. }) => continue, // Spurious wake, retry.
                Err(e) => return Err(e),
            }
        }
    }

    /// Receive a response from the Data Plane, awaiting if none available.
    pub async fn recv_response(&mut self) -> Result<Rsp> {
        // Fast path: try immediate pop.
        match self.inner.try_recv_response() {
            Ok(rsp) => return Ok(rsp),
            Err(BridgeError::Empty) => {}
            Err(e) => return Err(e),
        }

        // Slow path: wait for data.
        loop {
            let mut guard =
                self.rsp_ready_fd
                    .readable()
                    .await
                    .map_err(|_| BridgeError::Backpressure {
                        percent: 0,
                        threshold: 0,
                    })?;

            let _ = self.inner.rsp_wake.consumer_wake.try_read();
            guard.clear_ready();

            match self.inner.try_recv_response() {
                Ok(rsp) => return Ok(rsp),
                Err(BridgeError::Empty) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Non-blocking try to send a request.
    pub fn try_send_request(&mut self, req: Req) -> Result<()> {
        self.inner.try_send_request(req)
    }

    /// Non-blocking try to receive a response.
    pub fn try_recv_response(&mut self) -> Result<Rsp> {
        self.inner.try_recv_response()
    }

    /// Current backpressure state.
    pub fn pressure(&self) -> crate::backpressure::PressureState {
        self.inner.pressure()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_bridge::BridgeChannel;

    #[tokio::test]
    async fn async_send_recv_roundtrip() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut data = bridge.data;

        let mut async_control = AsyncControlHandle::new(bridge.control).unwrap();

        // Send from Tokio async context.
        async_control.send_request(42).await.unwrap();

        // Receive on Data Plane side (sync).
        let req = data.try_recv_request().unwrap();
        assert_eq!(req, 42);

        // Data Plane sends response.
        data.try_send_response(84).unwrap();

        // Receive on Tokio async context.
        let rsp = async_control.recv_response().await.unwrap();
        assert_eq!(rsp, 84);
    }

    #[tokio::test]
    async fn async_send_wakes_on_space() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(4, 4).unwrap();
        let mut data = bridge.data;
        let mut async_control = AsyncControlHandle::new(bridge.control).unwrap();

        // Fill the queue.
        for i in 0..4 {
            async_control.send_request(i).await.unwrap();
        }

        // Spawn a task that will push (will block waiting for space).
        let send_task = tokio::spawn(async move {
            async_control.send_request(99).await.unwrap();
            async_control
        });

        // Give the task a moment to start waiting.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Free a slot by consuming from the data side.
        let _ = data.try_recv_request().unwrap();

        // The send task should complete now.
        let _control = tokio::time::timeout(std::time::Duration::from_secs(5), send_task)
            .await
            .expect("send_request should complete after space freed")
            .unwrap();
    }

    #[tokio::test]
    async fn async_recv_wakes_on_data() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut data = bridge.data;
        let mut async_control = AsyncControlHandle::new(bridge.control).unwrap();

        // Spawn a task that will receive (will block waiting for data).
        let recv_task = tokio::spawn(async move {
            let rsp = async_control.recv_response().await.unwrap();
            (async_control, rsp)
        });

        // Give the task a moment to start waiting.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Data Plane sends a response.
        data.try_send_response(777).unwrap();

        // The recv task should complete now.
        let (_control, rsp) = tokio::time::timeout(std::time::Duration::from_secs(5), recv_task)
            .await
            .expect("recv_response should complete after data sent")
            .unwrap();

        assert_eq!(rsp, 777);
    }
}
