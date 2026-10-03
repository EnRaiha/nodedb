// SPDX-License-Identifier: BUSL-1.1

//! Configuration for the Calvin sequencer service.

use std::time::Duration;

use crate::error::ClusterError;

/// Unique Raft group identifier for the Calvin sequencer.
///
/// Deliberately outside the data-group range (1..=N) and the metadata group
/// (0). The high bits make accidental collision with a data group impossible
/// even if the cluster is extended to thousands of vShards.
pub const SEQUENCER_GROUP_ID: u64 = 0xFFFF_FFFF_0000_0001;

/// Configuration knobs for [`super::service::SequencerService`].
#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// How long each epoch window lasts. The leader drains the inbox at the
    /// end of each window and proposes the resulting batch to the Raft group.
    ///
    /// Default: 20 ms. Trade-off: shorter → lower latency, more Raft RPCs;
    /// longer → higher latency, higher batch throughput.
    pub epoch_duration: Duration,

    /// Maximum number of pending transactions the inbox can hold before
    /// back-pressuring submitters with [`super::inbox::SubmitError::Overloaded`].
    ///
    /// Default: 10 000.
    pub inbox_capacity: usize,

    /// Maximum number of sequenced transactions buffered per vshard output
    /// channel before the apply loop drops (not blocks) on a full channel.
    ///
    /// Default: 512.
    pub vshard_channel_depth: usize,

    /// Maximum plan bytes one sequencer entry carries: a single-entry
    /// transaction's `plans`, or one part of a multi-part transaction. A
    /// coordinator splits a larger transaction into parts. The inbox refuses a
    /// single entry or a part above it with `TxnTooLarge`.
    ///
    /// Default: 1 MiB (1 << 20).
    pub max_plans_bytes_per_txn: usize,

    /// Maximum vShards one sequencer entry's plans target: a single-entry
    /// transaction's participants, or one part's targets. A coordinator
    /// splits a transaction with more participants into parts. The inbox
    /// refuses a part above it with `FanoutTooWide`. A multi-part
    /// transaction's participants are bounded only by the vShard count.
    ///
    /// Default: 64.
    pub max_participating_vshards_per_txn: usize,

    /// Maximum part bytes the sequencer leader queues across every part
    /// stream, streamed in and not yet proposed. A stream that finds the
    /// queue full is told to retry, so the leader's memory stays bounded
    /// whatever the transaction's size. An empty queue always takes one
    /// part.
    ///
    /// Default: 64 MiB (64 << 20), four epochs of `max_bytes_per_epoch`.
    pub max_queued_part_bytes: usize,

    /// How long the leader keeps a part stream that sends nothing. A
    /// coordinator that stops streaming leaves its transaction holding
    /// locks on every participant, so the leader then abandons it.
    ///
    /// Default: 30 s.
    pub part_stream_stall: Duration,

    /// Maximum number of transactions drained from the inbox per epoch.
    /// The epoch tick stops draining once this count is reached. It must be
    /// below [`nodedb_types::MAX_POSITIONS_PER_EPOCH`]: each position owns
    /// one nanosecond of its epoch's system-time millisecond.
    ///
    /// Default: 1 024.
    pub max_txns_per_epoch: usize,

    /// Maximum total `plans` bytes across all transactions in a single epoch.
    /// When adding the next transaction would push the running total past this
    /// cap, the drain stops and the next transaction is held for the following
    /// epoch.
    ///
    /// Default: 16 MiB (16 << 20).
    pub max_bytes_per_epoch: usize,

    /// Maximum number of in-flight (admitted but not yet drained) transactions
    /// per tenant. Submissions from a tenant that already has
    /// `tenant_inbox_quota` items in the inbox are rejected with
    /// `TenantQuotaExceeded`.
    ///
    /// Default: `(inbox_capacity / 8).max(1)`.
    pub tenant_inbox_quota: usize,

    /// Maximum total estimated serialized bytes across all passive read keys
    /// in a single dependent-read transaction.
    ///
    /// Each passive read result is Raft-replicated as one
    /// `CalvinReadResult` entry per passive vshard per txn. Keeping each
    /// txn's read payload bounded prevents a few large dependent-read txns
    /// from overwhelming the per-vshard Raft log.
    ///
    /// Default: 4 MiB (4 << 20).
    pub max_dependent_read_bytes_per_txn: usize,

    /// Maximum number of passive participants (distinct vshards) in a
    /// single dependent-read transaction.
    ///
    /// Each passive vshard produces one `CalvinReadResult` Raft entry.
    /// Bounding the fan-in prevents a single txn from driving unbounded
    /// Raft entry replication across many shards.
    ///
    /// Default: 16.
    pub max_dependent_read_passives_per_txn: usize,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        let inbox_capacity = 10_000;
        Self {
            epoch_duration: Duration::from_millis(20),
            inbox_capacity,
            vshard_channel_depth: 512,
            max_plans_bytes_per_txn: 1 << 20,
            max_participating_vshards_per_txn: 64,
            max_queued_part_bytes: 64 << 20,
            part_stream_stall: Duration::from_secs(30),
            max_txns_per_epoch: 1_024,
            max_bytes_per_epoch: 16 << 20,
            tenant_inbox_quota: (inbox_capacity / 8).max(1),
            max_dependent_read_bytes_per_txn: 4 << 20,
            max_dependent_read_passives_per_txn: 16,
        }
    }
}

impl SequencerConfig {
    /// Validate that all caps are `>= 1` and that an epoch fits its
    /// system-time millisecond. Returns an error describing the first
    /// violation found.
    pub fn validate(&self) -> Result<(), ClusterError> {
        if self.max_txns_per_epoch >= nodedb_types::MAX_POSITIONS_PER_EPOCH {
            return Err(ClusterError::Config {
                detail: format!(
                    "SequencerConfig.max_txns_per_epoch must be < {}, got {}: each position \
                     owns one nanosecond of its epoch's millisecond",
                    nodedb_types::MAX_POSITIONS_PER_EPOCH,
                    self.max_txns_per_epoch
                ),
            });
        }
        let caps: &[(&'static str, usize)] = &[
            ("inbox_capacity", self.inbox_capacity),
            ("vshard_channel_depth", self.vshard_channel_depth),
            ("max_plans_bytes_per_txn", self.max_plans_bytes_per_txn),
            (
                "max_participating_vshards_per_txn",
                self.max_participating_vshards_per_txn,
            ),
            ("max_queued_part_bytes", self.max_queued_part_bytes),
            ("max_txns_per_epoch", self.max_txns_per_epoch),
            ("max_bytes_per_epoch", self.max_bytes_per_epoch),
            ("tenant_inbox_quota", self.tenant_inbox_quota),
            (
                "max_dependent_read_bytes_per_txn",
                self.max_dependent_read_bytes_per_txn,
            ),
            (
                "max_dependent_read_passives_per_txn",
                self.max_dependent_read_passives_per_txn,
            ),
        ];
        for (name, value) in caps {
            if *value < 1 {
                return Err(ClusterError::Config {
                    detail: format!("SequencerConfig.{name} must be >= 1, got {value}"),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_sane() {
        let cfg = SequencerConfig::default();
        assert_eq!(cfg.epoch_duration, Duration::from_millis(20));
        assert_eq!(cfg.inbox_capacity, 10_000);
        assert_eq!(cfg.vshard_channel_depth, 512);
        assert_eq!(cfg.max_plans_bytes_per_txn, 1 << 20);
        assert_eq!(cfg.max_participating_vshards_per_txn, 64);
        assert_eq!(cfg.max_txns_per_epoch, 1_024);
        assert_eq!(cfg.max_bytes_per_epoch, 16 << 20);
        assert_eq!(cfg.tenant_inbox_quota, 10_000 / 8);
        assert_eq!(cfg.max_dependent_read_bytes_per_txn, 4 << 20);
        assert_eq!(cfg.max_dependent_read_passives_per_txn, 16);
    }

    #[test]
    fn sequencer_group_id_not_zero_and_not_small() {
        // Must be outside data group range and != metadata group (0).
        assert_ne!(SEQUENCER_GROUP_ID, 0);
        // Cast to u64 to avoid constant-comparison lint on the literal comparison.
        let id: u64 = SEQUENCER_GROUP_ID;
        assert!(id > 0xFFFF_0000);
    }

    #[test]
    fn validate_accepts_default_config() {
        SequencerConfig::default()
            .validate()
            .expect("default should be valid");
    }

    #[test]
    fn validate_rejects_zero_cap() {
        let cfg = SequencerConfig {
            max_txns_per_epoch: 0,
            ..SequencerConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_an_epoch_past_its_millisecond() {
        let cfg = SequencerConfig {
            max_txns_per_epoch: nodedb_types::MAX_POSITIONS_PER_EPOCH,
            ..SequencerConfig::default()
        };
        let err = cfg
            .validate()
            .expect_err("an epoch of 1e6 positions is refused");
        let text = err.to_string();
        assert!(text.contains("max_txns_per_epoch"), "{text}");
        assert!(text.contains("1000000"), "{text}");

        let at_limit = SequencerConfig {
            max_txns_per_epoch: nodedb_types::MAX_POSITIONS_PER_EPOCH - 1,
            ..SequencerConfig::default()
        };
        at_limit
            .validate()
            .expect("the largest epoch that fits is accepted");
    }

    #[test]
    fn tenant_quota_floor_is_one_for_tiny_capacity() {
        // (1 / 8).max(1) == 1 — use a variable to avoid constant-folding lint.
        let capacity: usize = 1;
        let quota = (capacity / 8).max(1);
        let cfg = SequencerConfig {
            inbox_capacity: capacity,
            tenant_inbox_quota: quota,
            ..SequencerConfig::default()
        };
        assert_eq!(cfg.tenant_inbox_quota, 1);
    }
}
