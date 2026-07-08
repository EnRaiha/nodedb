// SPDX-License-Identifier: BUSL-1.1

//! Raft propose/compact callback type aliases and their serde defaults.

/// Type alias for the synchronous Raft propose callback.
///
/// Takes `(vshard_id, serialized_entry)` and returns `(group_id, log_index)`.
/// Works only when the current node is the group leader. Use
/// [`AsyncRaftProposer`] when proposals may originate from non-leader nodes.
pub type RaftProposer =
    dyn Fn(u32, Vec<u8>) -> std::result::Result<(u64, u64), crate::Error> + Send + Sync;

/// Type alias for the asynchronous Raft propose callback with leader forwarding.
///
/// Takes `(vshard_id, idempotency_key, serialized_entry)` and returns the Data
/// Plane apply payload bytes on success. The `idempotency_key` matches the one
/// embedded in the serialized `ReplicatedEntry`; the proposer registers the
/// tracker waiter with this key so apply-side mismatch detection can surface
/// `RetryableLeaderChange` when a new leader's entry overwrites this one.
pub type AsyncRaftProposer = dyn Fn(
        u32,
        u64,
        Vec<u8>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = std::result::Result<Vec<u8>, crate::Error>> + Send>,
    > + Send
    + Sync;

/// Type alias for the Raft log-compaction callback.
///
/// Takes `(group_id, applied_index)` where `applied_index` is the index the
/// DATA-PLANE state machine has durably applied to (NOT raft's commit
/// index). Invoked from the apply-completion path so a log can only be
/// compacted up to an index the engines have actually persisted — never
/// past it, which would corrupt a rebuilt snapshot. Returns `true` when a
/// compaction was performed. A no-op when the group's
/// `log_compaction_threshold` is `None`.
pub type RaftCompactor = dyn Fn(u64, u64) -> std::result::Result<bool, crate::Error> + Send + Sync;

pub(crate) fn default_pq_m() -> usize {
    crate::engine::vector::index_config::DEFAULT_PQ_M
}
pub(crate) fn default_ivf_cells() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_CELLS
}
pub(crate) fn default_ivf_nprobe() -> usize {
    crate::engine::vector::index_config::DEFAULT_IVF_NPROBE
}
