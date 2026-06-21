// SPDX-License-Identifier: BUSL-1.1

//! `CrossShardTxnMode` — how a multi-vShard write is committed.
//!
//! This is a cross-shard *transaction* concept owned by the Calvin layer (the
//! cross-shard transaction subsystem), not a wire/protocol concept. Both the
//! pgwire and native server paths consume it to decide whether a multi-shard
//! write routes through the Calvin sequencer (`Strict`) or is dispatched per
//! vShard independently (`BestEffortNonAtomic`). The pgwire `SET`/`SHOW`
//! `cross_shard_txn` session-parameter parsing/formatting lives in the pgwire
//! session module and imports this enum from here.

/// The cross-shard transaction mode for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CrossShardTxnMode {
    /// Full Calvin atomicity — cross-shard writes go through the sequencer.
    /// This is the default.
    #[default]
    Strict,
    /// Multi-vshard writes dispatched to each vshard independently —
    /// **NOT atomic.** Suitable for bulk loads only.
    BestEffortNonAtomic,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_strict() {
        assert_eq!(CrossShardTxnMode::default(), CrossShardTxnMode::Strict);
    }
}
