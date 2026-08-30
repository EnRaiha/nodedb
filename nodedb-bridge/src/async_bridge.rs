// SPDX-License-Identifier: BUSL-1.1

//! Async wrappers for the SPSC bridge.
//!
//! These integrate the raw `Producer`/`Consumer` with Tokio's async runtime
//! and the eventfd-based waker, providing `async fn push()` and `async fn pop()`.
//!
//! ## Architecture
//!
//! ```text
//! Tokio (Control Plane)             TPC (Data Plane)
//! ┌─────────────────────┐           ┌─────────────────────┐
//! │  AsyncProducer      │           │  SyncConsumer        │
//! │  .push(req).await   │──push──→  │  .poll_drain()       │
//! │                     │           │                     │
//! │  AsyncReceiver      │           │  SyncSender          │
//! │  .recv(rsp).await   │←──pop──── │  .try_send(rsp)      │
//! └─────────────────────┘           └─────────────────────┘
//! ```
//!
//! The TPC side uses `SyncConsumer`/`SyncSender` (non-async, !Send-compatible).
//! The Tokio side uses `AsyncProducer`/`AsyncReceiver` which wrap `try_push`/`try_pop`
//! with eventfd-based waking.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::backpressure::{BackpressureController, PressureState};
use crate::buffer::{Consumer, Producer, RingBuffer};
use crate::error::{BridgeError, Result};
use crate::eventfd::WakePair;

/// A complete bridge channel: request path (Control→Data) + response path (Data→Control).
///
/// Created once, then split into Tokio-side and TPC-side handles.
pub struct BridgeChannel<Req, Rsp> {
    /// Tokio-side handle for sending requests and receiving responses.
    pub control: ControlHandle<Req, Rsp>,
    /// TPC-side handle for receiving requests and sending responses.
    pub data: DataHandle<Req, Rsp>,
}

/// Handle held by the Control Plane (Tokio).
pub struct ControlHandle<Req, Rsp> {
    /// Send requests to the Data Plane.
    pub producer: Producer<Req>,
    /// Receive responses from the Data Plane.
    pub consumer: Consumer<Rsp>,
    /// Backpressure state for the request queue.
    pub backpressure: Arc<BackpressureController>,
    /// Wakers for the request channel.
    pub req_wake: Arc<WakePair>,
    /// Wakers for the response channel.
    pub rsp_wake: Arc<WakePair>,
}

/// Handle held by the Data Plane (TPC core).
///
/// This type is `Send` so it can be transferred to a TPC core during setup.
/// Call `.pin()` once on the target core to get a `PinnedDataHandle` which is
/// `!Send` — enforcing at compile time that it stays on that core forever.
pub struct DataHandle<Req, Rsp> {
    /// Receive requests from the Control Plane.
    pub consumer: Consumer<Req>,
    /// Send responses back to the Control Plane.
    pub producer: Producer<Rsp>,
    /// Backpressure state for the request queue (read-only from Data Plane).
    pub backpressure: Arc<BackpressureController>,
    /// Wakers for the request channel.
    pub req_wake: Arc<WakePair>,
    /// Wakers for the response channel.
    pub rsp_wake: Arc<WakePair>,
}

/// A pinned Data Plane handle that is `!Send`.
///
/// Created by calling `DataHandle::pin()` on the target TPC core.
/// Once pinned, this handle cannot be moved to another thread — the compiler
/// enforces this. Any attempt to `tokio::spawn` or `thread::spawn` with a
/// `PinnedDataHandle` is a compile error.
pub struct PinnedDataHandle<Req, Rsp> {
    inner: DataHandle<Req, Rsp>,
    /// Makes this type `!Send`.
    _not_send: PhantomData<*const ()>,
}

impl<Req, Rsp> BridgeChannel<Req, Rsp> {
    /// Create a new bridge channel pair.
    ///
    /// `req_capacity`: Size of the Control→Data request queue.
    /// `rsp_capacity`: Size of the Data→Control response queue.
    pub fn new(req_capacity: usize, rsp_capacity: usize) -> std::io::Result<Self> {
        let (req_producer, req_consumer) = RingBuffer::channel::<Req>(req_capacity);
        let (rsp_producer, rsp_consumer) = RingBuffer::channel::<Rsp>(rsp_capacity);

        let req_wake = Arc::new(WakePair::new()?);
        let rsp_wake = Arc::new(WakePair::new()?);
        let backpressure = Arc::new(BackpressureController::default());

        Ok(Self {
            control: ControlHandle {
                producer: req_producer,
                consumer: rsp_consumer,
                backpressure: Arc::clone(&backpressure),
                req_wake: Arc::clone(&req_wake),
                rsp_wake: Arc::clone(&rsp_wake),
            },
            data: DataHandle {
                consumer: req_consumer,
                producer: rsp_producer,
                backpressure,
                req_wake,
                rsp_wake,
            },
        })
    }
}

impl<Req, Rsp> ControlHandle<Req, Rsp> {
    /// Try to send a request to the Data Plane.
    ///
    /// On success, signals the consumer wake eventfd so the TPC core wakes up.
    /// Updates backpressure state based on queue utilization.
    pub fn try_send_request(&mut self, req: Req) -> Result<()> {
        let result = self.producer.try_push(req);

        // Update backpressure state.
        let util = self.producer.utilization();
        if let Some(new_state) = self.backpressure.update(util) {
            tracing::info!(
                utilization = util,
                state = ?new_state,
                "bridge backpressure transition"
            );
        }

        match &result {
            Ok(()) => {
                // Signal consumer that data is available.
                let _ = self.req_wake.consumer_wake.notify();
            }
            Err(BridgeError::Full { .. }) => {
                // Don't signal — queue is full, consumer already has plenty.
            }
            _ => {}
        }

        result
    }

    /// Try to receive a response from the Data Plane.
    ///
    /// On success, signals the producer wake eventfd so the TPC core knows
    /// there's space for more responses.
    pub fn try_recv_response(&mut self) -> Result<Rsp> {
        let result = self.consumer.try_pop();

        if result.is_ok() {
            // Signal the Data Plane that response queue has space.
            let _ = self.rsp_wake.producer_wake.notify();
        }

        result
    }

    /// Drain up to `max` responses into the buffer.
    ///
    /// The disconnect flag is dropped here: this handle exposes the dead
    /// channel through `try_recv_response`, which returns
    /// `BridgeError::Disconnected` once the buffer is empty.
    pub fn drain_responses(&mut self, buf: &mut Vec<Rsp>, max: usize) -> usize {
        let (count, _disconnected) = self.consumer.drain_into(buf, max);
        if count > 0 {
            let _ = self.rsp_wake.producer_wake.notify();
        }
        count
    }

    /// Current backpressure state.
    pub fn pressure(&self) -> PressureState {
        self.backpressure.state()
    }

    /// Raw fd for the response-available signal (register with Tokio's AsyncFd).
    pub fn response_wake_fd(&self) -> std::os::unix::io::RawFd {
        self.rsp_wake.consumer_wake.as_fd()
    }

    /// Raw fd for the request-space-available signal.
    pub fn request_space_fd(&self) -> std::os::unix::io::RawFd {
        self.req_wake.producer_wake.as_fd()
    }
}

impl<Req, Rsp> DataHandle<Req, Rsp> {
    /// Pin this handle to the current TPC core.
    ///
    /// Returns a `PinnedDataHandle` which is `!Send` — the compiler will
    /// reject any attempt to move it to another thread.
    ///
    /// Call this exactly once, on the target TPC core, during setup.
    pub fn pin(self) -> PinnedDataHandle<Req, Rsp> {
        PinnedDataHandle {
            inner: self,
            _not_send: PhantomData,
        }
    }

    /// Try to receive a request from the Control Plane.
    pub fn try_recv_request(&mut self) -> Result<Req> {
        let result = self.consumer.try_pop();
        if result.is_ok() {
            let _ = self.req_wake.producer_wake.notify();
        }
        result
    }

    /// Drain up to `max` requests into the buffer.
    ///
    /// The disconnect flag is dropped here: this handle exposes the dead
    /// channel through `try_recv_request`, which returns
    /// `BridgeError::Disconnected` once the buffer is empty.
    pub fn drain_requests(&mut self, buf: &mut Vec<Req>, max: usize) -> usize {
        let (count, _disconnected) = self.consumer.drain_into(buf, max);
        if count > 0 {
            let _ = self.req_wake.producer_wake.notify();
        }
        count
    }

    /// Try to send a response back to the Control Plane.
    pub fn try_send_response(&mut self, rsp: Rsp) -> Result<()> {
        let result = self.producer.try_push(rsp);
        if result.is_ok() {
            let _ = self.rsp_wake.consumer_wake.notify();
        }
        result
    }

    /// Current backpressure state (read by Data Plane to decide I/O depth).
    pub fn pressure(&self) -> PressureState {
        self.backpressure.state()
    }

    /// Whether the Data Plane should reduce read depth.
    pub fn should_throttle(&self) -> bool {
        matches!(
            self.pressure(),
            PressureState::Throttled | PressureState::Suspended
        )
    }

    /// Whether the Data Plane should suspend new reads entirely.
    pub fn should_suspend(&self) -> bool {
        self.pressure() == PressureState::Suspended
    }

    /// Raw fd for the request-available signal (register with TPC event loop).
    pub fn request_wake_fd(&self) -> std::os::unix::io::RawFd {
        self.req_wake.consumer_wake.as_fd()
    }

    /// Raw fd for the response-space-available signal.
    pub fn response_space_fd(&self) -> std::os::unix::io::RawFd {
        self.rsp_wake.producer_wake.as_fd()
    }
}

impl<Req, Rsp> PinnedDataHandle<Req, Rsp> {
    /// Try to receive a request from the Control Plane.
    pub fn try_recv_request(&mut self) -> Result<Req> {
        self.inner.try_recv_request()
    }

    /// Drain up to `max` requests into the buffer.
    pub fn drain_requests(&mut self, buf: &mut Vec<Req>, max: usize) -> usize {
        self.inner.drain_requests(buf, max)
    }

    /// Try to send a response back to the Control Plane.
    pub fn try_send_response(&mut self, rsp: Rsp) -> Result<()> {
        self.inner.try_send_response(rsp)
    }

    /// Current backpressure state.
    pub fn pressure(&self) -> PressureState {
        self.inner.pressure()
    }

    /// Whether the Data Plane should reduce read depth.
    pub fn should_throttle(&self) -> bool {
        self.inner.should_throttle()
    }

    /// Whether the Data Plane should suspend new reads entirely.
    pub fn should_suspend(&self) -> bool {
        self.inner.should_suspend()
    }

    /// Raw fd for the request-available signal.
    pub fn request_wake_fd(&self) -> std::os::unix::io::RawFd {
        self.inner.request_wake_fd()
    }

    /// Raw fd for the response-space-available signal.
    pub fn response_space_fd(&self) -> std::os::unix::io::RawFd {
        self.inner.response_space_fd()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_channel_roundtrip() {
        let bridge: BridgeChannel<u64, String> = BridgeChannel::new(16, 16).unwrap();
        let mut control = bridge.control;
        let mut data = bridge.data;

        // Control sends request.
        control.try_send_request(42).unwrap();

        // Data receives request.
        let req = data.try_recv_request().unwrap();
        assert_eq!(req, 42);

        // Data sends response.
        data.try_send_response("result".to_string()).unwrap();

        // Control receives response.
        let rsp = control.try_recv_response().unwrap();
        assert_eq!(rsp, "result");
    }

    #[test]
    fn backpressure_updates_on_send() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut control = bridge.control;

        // Fill to 87.5% (14/16 slots).
        for i in 0..14 {
            control.try_send_request(i).unwrap();
        }

        // Should have transitioned to Throttled.
        assert_eq!(control.pressure(), PressureState::Throttled);
    }

    #[test]
    fn eventfd_wake_on_push() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut control = bridge.control;
        let data = bridge.data;

        control.try_send_request(1).unwrap();

        // Consumer wake fd should be signaled.
        let count = data.req_wake.consumer_wake.try_read().unwrap();
        assert!(count > 0);
    }

    #[test]
    fn drain_responses_signals_producer() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut control = bridge.control;
        let mut data = bridge.data;

        // Data sends multiple responses.
        data.try_send_response(10).unwrap();
        data.try_send_response(20).unwrap();
        data.try_send_response(30).unwrap();

        // Control drains them.
        let mut buf = Vec::new();
        let count = control.drain_responses(&mut buf, 10);
        assert_eq!(count, 3);
        assert_eq!(buf, vec![10, 20, 30]);
    }

    #[test]
    fn data_handle_throttle_queries() {
        let bridge: BridgeChannel<u64, u64> = BridgeChannel::new(16, 16).unwrap();
        let mut control = bridge.control;
        let data = bridge.data;

        assert!(!data.should_throttle());
        assert!(!data.should_suspend());

        // Fill past 85%.
        for i in 0..14 {
            control.try_send_request(i).unwrap();
        }

        assert!(data.should_throttle());
        assert!(!data.should_suspend());

        // Fill past 95%.
        control.try_send_request(14).unwrap();
        control.try_send_request(15).unwrap();

        assert!(data.should_throttle());
        assert!(data.should_suspend());
    }
}
