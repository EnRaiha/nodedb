// SPDX-License-Identifier: BUSL-1.1

//! Flush cadence and cluster-mode HiLo batch size constants.

use std::time::Duration;

/// Flush trigger: every N allocations.
pub const FLUSH_OPS_THRESHOLD: u64 = 1024;

/// Flush trigger: every T elapsed since the last flush.
pub const FLUSH_ELAPSED_THRESHOLD: Duration = Duration::from_millis(200);

/// Cluster-mode HiLo reservation batch size.
pub const RESERVE_BATCH_SIZE: u32 = 4096;
