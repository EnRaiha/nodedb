// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use tokio::sync::mpsc;

use crate::bridge::envelope::Response;
use crate::types::RequestId;

/// Per-request channel capacity. A streaming scan produces at most
/// `ceil(rows / STREAM_CHUNK_SIZE)` partials — a few hundred for the
/// largest realistic queries. Capacity here bounds how many chunks can
/// sit in RAM while the Control-Plane session's TCP write buffer is
/// stalled; once full, `complete` returns false so the Data Plane can
/// observe backpressure instead of silently growing RSS.
pub const REQUEST_CHANNEL_CAPACITY: usize = 256;

/// Routes Data Plane responses back to the waiting Control Plane session.
///
/// Each dispatched request registers an mpsc sender here. The background
/// response poller forwards responses as they arrive. For streaming queries,
/// multiple partial responses arrive before the final one.
///
/// - Partial responses (`response.partial == true`): forwarded but request
///   stays in the map for more chunks.
/// - Final response (`response.partial == false`): forwarded and request
///   removed from the map.
#[derive(Default)]
pub struct RequestTracker {
    pending: Mutex<HashMap<RequestId, mpsc::Sender<Response>>>,
}

impl RequestTracker {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(HashMap::new()),
        }
    }

    fn lock_pending(&self) -> MutexGuard<'_, HashMap<RequestId, mpsc::Sender<Response>>> {
        match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Register a pending request. Returns a bounded receiver the session awaits.
    ///
    /// For non-streaming requests, exactly one response arrives.
    /// For streaming requests, multiple partial responses arrive before the final one.
    /// Channel capacity applies backpressure when the session is slow.
    pub fn register(&self, id: RequestId) -> mpsc::Receiver<Response> {
        let (tx, rx) = mpsc::channel(REQUEST_CHANNEL_CAPACITY);
        self.lock_pending().insert(id, tx);
        rx
    }

    /// Forward a response from the Data Plane to the waiting session.
    ///
    /// - If `response.partial` is true: sends the chunk but keeps the
    ///   request in the map for subsequent chunks.
    /// - If `response.partial` is false: sends the final chunk and
    ///   removes the request from the map.
    ///
    /// Returns `false` if the request was cancelled, timed out, or the
    /// session buffer is full (backpressure signal — the Data Plane should
    /// stop producing further chunks for this request).
    pub fn complete(&self, response: Response) -> bool {
        let is_final = !response.partial;
        let mut pending = self.lock_pending();

        if is_final {
            if let Some(tx) = pending.remove(&response.request_id) {
                tx.try_send(response).is_ok()
            } else {
                false
            }
        } else {
            let request_id = response.request_id;
            if let Some(tx) = pending.get(&request_id) {
                match tx.try_send(response) {
                    Ok(()) => true,
                    Err(_) => {
                        // Full channel (session stalled) or closed (cancelled):
                        // evict the pending entry so the Data Plane stops
                        // producing further chunks for this request.
                        pending.remove(&request_id);
                        false
                    }
                }
            } else {
                false
            }
        }
    }

    /// Remove a pending request (e.g., on session disconnect).
    pub fn cancel(&self, id: &RequestId) {
        self.lock_pending().remove(id);
    }

    /// Number of in-flight requests.
    pub fn in_flight(&self) -> usize {
        self.lock_pending().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{Payload, Status};
    use crate::types::Lsn;

    fn make_response(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    fn make_partial(id: u64, data: &str) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(data.as_bytes().to_vec()),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[tokio::test]
    async fn register_and_complete() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(1));
        assert_eq!(tracker.in_flight(), 1);

        assert!(tracker.complete(make_response(1)));
        assert_eq!(tracker.in_flight(), 0);

        let resp = rx.recv().await.unwrap();
        assert_eq!(resp.request_id, RequestId::new(1));
    }

    #[test]
    fn complete_unknown_returns_false() {
        let tracker = RequestTracker::new();
        assert!(!tracker.complete(make_response(999)));
    }

    #[test]
    fn cancel_removes_pending() {
        let tracker = RequestTracker::new();
        let _rx = tracker.register(RequestId::new(5));
        assert_eq!(tracker.in_flight(), 1);
        tracker.cancel(&RequestId::new(5));
        assert_eq!(tracker.in_flight(), 0);
    }

    #[tokio::test]
    async fn streaming_partial_then_final() {
        let tracker = RequestTracker::new();
        let mut rx = tracker.register(RequestId::new(10));

        assert!(tracker.complete(make_partial(10, "chunk1")));
        assert_eq!(tracker.in_flight(), 1);
        assert!(tracker.complete(make_partial(10, "chunk2")));
        assert_eq!(tracker.in_flight(), 1);

        assert!(tracker.complete(make_response(10)));
        assert_eq!(tracker.in_flight(), 0);

        let r1 = rx.recv().await.unwrap();
        assert!(r1.partial);
        let r2 = rx.recv().await.unwrap();
        assert!(r2.partial);
        let r3 = rx.recv().await.unwrap();
        assert!(!r3.partial);
    }

    #[test]
    fn full_channel_signals_backpressure() {
        let tracker = RequestTracker::new();
        let _rx = tracker.register(RequestId::new(7));
        let mut rejected = 0usize;
        for i in 0u32..(REQUEST_CHANNEL_CAPACITY as u32 * 2) {
            if !tracker.complete(make_partial(7, &format!("chunk-{i}"))) {
                rejected += 1;
            }
        }
        assert!(rejected > 0);
        // Entry was evicted on first full-channel hit.
        assert_eq!(tracker.in_flight(), 0);
    }
}
