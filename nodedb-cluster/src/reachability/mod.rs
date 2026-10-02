// SPDX-License-Identifier: BUSL-1.1

//! Reachability driver — the active half of circuit-breaker recovery.
//!
//! `CircuitBreaker` transitions `Open → HalfOpen` only on the next
//! `check()` call. Without periodic traffic to an offline peer, that
//! check never happens and the breaker stays `Open` forever even after
//! the peer has recovered. This module closes that blind spot:
//!
//! - [`ReachabilityDriver`] periodically walks the breaker's open set
//!   and sends a lightweight probe RPC to each peer via the
//!   `send_probe_rpc` path, which drives the HalfOpen → Closed /
//!   HalfOpen → Open transitions.
//! - [`ReachabilityProber`] is the injection seam: production wraps
//!   [`crate::transport::NexarTransport`], tests use a mock.
//!
//! The driver is shutdown-aware (watch channel) and bounded — one
//! probe per open peer per tick, fire-and-forget.

pub mod driver;
pub mod prober;

pub use driver::{ReachabilityDriver, ReachabilityDriverConfig};
pub use prober::{NoopProber, ReachabilityProber, TransportProber};
