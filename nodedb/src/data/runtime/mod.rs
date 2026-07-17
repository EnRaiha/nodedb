// SPDX-License-Identifier: BUSL-1.1

//! TPC (Thread-per-Core) runtime for Data Plane cores.
//!
//! Replaces the naive `sleep(50µs)` busy-poll with an eventfd-driven wake
//! mechanism. Each core thread:
//!
//! 1. Pins itself to a dedicated jemalloc arena (zero allocator contention).
//! 2. Blocks on `libc::poll(eventfd)` when idle (zero CPU waste).
//! 3. Wakes instantly when the Control Plane signals via `EventFdNotifier`.
//! 4. Processes all pending requests in a tight loop, then re-parks.
//!
//! # Panic Isolation
//!
//! Every `core.tick()` invocation is wrapped in `catch_unwind`. A panic in
//! any engine execution (bad index, arithmetic overflow, corrupted data)
//! is caught without killing the core thread. The faulting request receives
//! an `INTERNAL_ERROR` response, and the core continues serving subsequent
//! requests. A health watchdog tracks consecutive panics: if the threshold
//! is exceeded, the core stops accepting new work and logs an alert.
//!
//! # Boot ordering
//!
//! `spawn.rs` runs the three boot stages — `boot_restore`, then `boot_seed`,
//! then `boot_replay` — in exactly that order. The order is load-bearing, not
//! stylistic: a checkpoint restores state as of the LSN it was stamped with and
//! replay resumes strictly ABOVE that LSN, so restoring after replay would
//! overwrite newer state with older rows. Each stage's entry point documents the
//! constraint it rests on.

mod boot_replay;
mod boot_restore;
mod boot_seed;
mod config;
mod event_loop;
mod params;
mod spawn;

pub use config::CoreCompactionConfig;
pub use params::SpawnCoreParams;
pub use spawn::spawn_core;
