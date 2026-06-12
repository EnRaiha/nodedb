// SPDX-License-Identifier: Apache-2.0

//! Sync provenance: producer identity and monotonic sequence for idempotency.

use serde::{Deserialize, Serialize};

/// Producer identity and sequence used by the WAL / Data Plane for
/// idempotency checks.
///
/// On the wire, ingest messages carry the three flat fields
/// (`producer_id`, `epoch`, `seq`) directly for zero nesting overhead.
/// This struct is the in-process representation used after deserialization.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SyncProvenance {
    /// Stable identity of the originating producer (Lite peer ID or Origin node ID).
    pub producer_id: u64,
    /// Monotonic epoch counter incremented on every producer restart.
    pub epoch: u64,
    /// Per-stream monotonic sequence number. Gaps signal message loss.
    pub stream_id: u64,
    /// Per-stream monotonic sequence number within the epoch.
    pub seq: u64,
}
