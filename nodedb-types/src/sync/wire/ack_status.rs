// SPDX-License-Identifier: Apache-2.0

//! `AckStatus` — outcome of an idempotency check on the server side.

use serde::{Deserialize, Serialize};

/// Server-side outcome for an acknowledged ingest message.
///
/// Receivers can match on this to surface duplicate-detection and
/// gap-detection UX without special-casing the `applied_seq` value.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum AckStatus {
    /// The message was applied for the first time.
    #[default]
    Applied,
    /// The message was a duplicate and was ignored (idempotent replay).
    Duplicate,
    /// The producer's epoch is older than the server's recorded epoch;
    /// the message was fenced to prevent stale-epoch writes.
    Fenced,
    /// A gap in the sequence was detected; `expected` is the next seq
    /// the server expected from this producer.
    Gap { expected: u64 },
}
