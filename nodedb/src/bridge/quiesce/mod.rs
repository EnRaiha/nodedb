// SPDX-License-Identifier: BUSL-1.1

//! Collection-scoped scan quiesce primitive.
//!
//! Purpose: before unlinking on-disk files for a purged collection, every
//! in-flight scan against that collection must complete. An active TPC
//! scan dereferencing a freed mmap page faults the whole shard reactor —
//! a production crash, not a test flake. This module guarantees the
//! ordering: *stop accepting new scans → drain existing scans → unlink*.
//!
//! Split:
//! - [`refcount`] — per-`(tenant, collection)` scan refcount, `ScanGuard`
//!   RAII type, `try_start_scan` entry point.
//! - [`drain`] — `wait_until_drained`.
//! - [`hold`] — owned drain holds and the pending-reclaim hold.
//!
//! The backing [`CollectionQuiesce`] is shared (Arc). Calls are rare
//! (one bump per scan, one drain per purge), so the internal `Mutex`
//! is not a hot-path bottleneck.

pub mod drain;
pub mod hold;
pub mod refcount;

pub use drain::WaitDrain;
pub use hold::{DrainHold, ReclaimOwner};
pub use refcount::{CollectionQuiesce, ScanGuard, ScanStartError};
