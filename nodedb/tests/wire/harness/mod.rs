// SPDX-License-Identifier: BUSL-1.1

//! Out-of-process pgwire test harness: spawns the real `nodedb` binary as a
//! subprocess and drives it over a connected `tokio_postgres::Client`.
//!
//! Unlike `nodedb-test-support::pgwire_harness`, which links the server
//! library in-process, this harness depends on neither the `nodedb` library
//! nor `nodedb-test-support` — every file under `tests/wire/cases/` compiles
//! into ONE test binary instead of one binary per file.

mod config_toml;
mod connect;
mod lifecycle;
mod process;
mod query;
mod types;

pub mod insert_returning_engines;

// The harness deliberately mirrors the whole `TestServer` API surface that
// `nodedb-test-support::pgwire_harness` exposes, so porting a test file is an
// import swap and nothing else. Only part of `tests/*.rs` has moved so far, so
// parts of that surface have no caller yet. Same reason the in-process harness
// carries this attribute.
#[allow(unused_imports)]
pub use types::{TestClient, TestDataDir, TestServer};
