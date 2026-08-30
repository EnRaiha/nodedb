// SPDX-License-Identifier: BUSL-1.1

//! Wall-clock fallback for non-Calvin KV write paths.

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
