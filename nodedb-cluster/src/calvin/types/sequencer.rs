// SPDX-License-Identifier: BUSL-1.1

//! Sequencer output types: [`SequencedTxn`] and [`EpochBatch`].
//!
//! These are the Raft-replicated entries produced by the Calvin sequencer.
//! Every replica applies the same `EpochBatch` in the same order, guaranteeing
//! determinism.

use serde::{Deserialize, Serialize};

use super::lock_wire::TxnIdWire;
use super::transaction::TxClass;

// ── SequencedTxn ──────────────────────────────────────────────────────────────

/// A transaction that has been assigned a global position by the sequencer.
///
/// The `(epoch, position)` pair is globally unique and totally ordered across
/// all vShards. Every shard that participates in the transaction will see this
/// txn at the same `(epoch, position)` in its scheduler input stream.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct SequencedTxn {
    /// Sequencer epoch in which this transaction was admitted.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
    /// The fully-declared transaction class.
    pub tx_class: TxClass,
    /// Wall-clock ms at epoch creation (read once on the sequencer leader).
    ///
    /// This is the single deterministic timestamp source for all Calvin write
    /// paths. Engine handlers that need a "current time" (bitemporal sys_from,
    /// KV TTL expire_at, timeseries system_ms) MUST use this value instead of
    /// reading the wall clock independently, ensuring byte-identical state
    /// across all replicas. Wire-additive: zerompk returns default (0) when
    /// decoding older entries that lack this field.
    #[serde(default)]
    #[msgpack(default)]
    pub epoch_system_ms: i64,
    /// Number of positions in this epoch that target the vShard this copy is
    /// delivered to.
    ///
    /// Stamped per-vShard by the sequencer state machine at fan-out time (like
    /// `epoch_system_ms`), NOT at sequencing time — the value is `0` on the
    /// replicated log and is filled in when the batch is fanned out to each
    /// participating vShard's scheduler. It lets a scheduler know, without a
    /// priori knowledge, how many positions of an epoch it must apply before
    /// that epoch is fully applied on its vShard — the input to the scheduler's
    /// exactly-once per-`(epoch, position)` applied gate and fully-applied
    /// watermark. Wire-additive: zerompk returns default (0) when decoding
    /// entries that predate this field.
    #[serde(default)]
    #[msgpack(default)]
    pub epoch_vshard_txn_count: u32,
    /// Optional lock-table identity distinct from this txn's `(epoch, position)`
    /// apply-slot. `None` (the default) means the lock owner is the apply slot —
    /// the established behavior. `Some(id)` lets a different identity (e.g. a
    /// read-reservation id from a separate position band) own the exclusive lock
    /// while this txn keeps its own fresh apply-slot for the watermark and
    /// completion path. Wire-additive: decodes to `None` on older log entries.
    #[serde(default)]
    #[msgpack(default)]
    pub lock_owner: Option<TxnIdWire>,
}

// ── EpochBatch ────────────────────────────────────────────────────────────────

/// A fully-ordered batch of transactions for one sequencer epoch.
///
/// This is the Raft-replicated entry emitted by the sequencer. Every replica
/// applies the same `EpochBatch` in the same order, guaranteeing determinism.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct EpochBatch {
    /// The epoch number. Monotonically increasing across all batches.
    pub epoch: u64,
    /// Ordered transactions in this epoch. Position is the index in this vec.
    pub txns: Vec<SequencedTxn>,
    /// Wall-clock ms read ONCE on the sequencer leader at epoch creation.
    ///
    /// This single timestamp is the deterministic time anchor for every
    /// transaction in this epoch. When the state machine fans `SequencedTxn`s
    /// out to per-shard channels it copies this value into each txn's
    /// `epoch_system_ms` field, making it available to engine handlers without
    /// threading it through every intermediate layer.
    pub epoch_system_ms: i64,
}
