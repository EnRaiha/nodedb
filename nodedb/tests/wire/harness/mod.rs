// SPDX-License-Identifier: BUSL-1.1

//! Out-of-process pgwire test harness: spawns the real `nodedb` binary as a
//! subprocess and drives it over a connected `tokio_postgres::Client`.
//! Unlike `nodedb-test-support::pgwire_harness`, this depends on neither the
//! `nodedb` library nor `nodedb-test-support`, so every file under
//! `tests/wire/cases/` compiles into one test binary.

mod config_toml;
mod connect;
mod lifecycle;
mod process;
mod query;
mod types;

pub mod insert_returning_engines;

// Mirrors the whole `TestServer` API surface `pgwire_harness` exposes, so
// porting a test file is an import swap; unused parts have no caller yet.
#[allow(unused_imports)]
pub use types::{TestClient, TestDataDir, TestServer};
