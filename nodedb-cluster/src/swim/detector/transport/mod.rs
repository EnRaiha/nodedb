// SPDX-License-Identifier: BUSL-1.1

//! SWIM transport abstraction.
//!
//! The detector talks to the network exclusively through the [`Transport`]
//! trait. Two production-facing impls exist:
//!
//! 1. [`in_memory::InMemoryTransport`] — a tokio-mpsc fabric used by every
//!    unit test. Supports per-edge drop and partition injection so tests
//!    can deterministically simulate unreachable peers.
//! 2. [`udp::UdpTransport`] — the real wire-level transport that binds a
//!    `tokio::net::UdpSocket` and framing-encodes every datagram via
//!    [`crate::swim::wire::encode`].
//!
//! The trait is `Send + Sync` and its methods are `async`. Errors are
//! typed [`SwimError`] variants so callers never see raw `io::Error`.

mod contract;
pub mod in_memory;
pub mod udp;

pub use contract::Transport;
pub use in_memory::{InMemoryTransport, TransportFabric};
pub use udp::UdpTransport;
