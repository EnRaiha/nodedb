// SPDX-License-Identifier: BUSL-1.1

//! Shared pgwire end-to-end test harness.
//!
//! Spawns a full NodeDB server (Data Plane core + pgwire listener + response
//! poller) on a one-node Raft cluster booted by [`crate::single_node`], and
//! provides a connected `tokio_postgres::Client` for SQL execution.

mod backup;
mod multicore;
mod query;
pub mod raw_pgwire;
mod restart;
mod start;
mod support;
mod types;

pub use types::{TestClient, TestDataDir, TestServer};
