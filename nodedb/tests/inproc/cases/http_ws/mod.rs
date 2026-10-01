// SPDX-License-Identifier: BUSL-1.1

//! Smoke tests for WebSocket RPC endpoint.
//!
//! Endpoint covered:
//! - GET /v1/ws  — WebSocket upgrade
//!
//! Contracts asserted:
//! - Under Trust mode: upgrade succeeds
//! - After upgrade: `query` method with valid SQL returns a JSON response with `result` field
//! - `ping` method returns `"pong"` result
//! - Under Password mode: upgrade is refused before any WS state is created (401)
//! - Non-upgrade GET does not hang (axum rejects it)
//! - Resume auth replays the selected database's events in publication order
//!   behind opaque cursors
//! - A LIVE SELECT flooded past its buffers never skips an event silently

mod live_lag;
mod resume;
mod upgrade_and_rpc;
