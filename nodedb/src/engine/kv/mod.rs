// SPDX-License-Identifier: BUSL-1.1

pub mod engine;
pub mod engine_atomic;
pub mod engine_atomic_compute;
mod engine_helpers;
mod engine_index;
mod engine_rename;
pub mod engine_sorted;
mod engine_stats;
mod engine_write;
pub mod entry;
pub mod expiry_wheel;
mod hash_helpers;
pub mod hash_table;
pub mod index;
pub mod scan;
pub mod slab;
pub mod sorted_index;

pub use engine::KvEngine;
pub use engine_atomic::{AtomicError, CasResult};
pub use engine_atomic_compute as atomic_compute;
pub use engine_stats::{ExpiredKey, KvStats};
pub use scan::KvScanParams;

/// Get current wall-clock time in milliseconds since Unix epoch.
///
/// Used as a fallback for non-Calvin write paths. Calvin write paths call
/// `CoreLoop::epoch_system_ms.unwrap_or_else(current_ms)` so that the epoch's
/// deterministic timestamp anchor is used when available.
pub fn current_ms() -> u64 {
    // no-determinism: fallback for non-Calvin paths; Calvin paths gate through epoch_system_ms
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
