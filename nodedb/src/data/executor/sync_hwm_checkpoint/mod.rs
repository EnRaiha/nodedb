// SPDX-License-Identifier: BUSL-1.1

//! Sync idempotency-gate checkpoint write + load operations for `CoreLoop`.
//!
//! ## What is at stake
//!
//! `sync_hwm` and `producer_epoch_floor` (see `sync_gate.rs`) are the gate that
//! decides whether an inbound sync frame is new, a duplicate, fenced, or a gap.
//! Both are plain in-memory maps with no store behind them, rebuilt at boot
//! ONLY by `replay_sync_hwm_records` from the `SyncSeqAdvance` WAL records —
//! the WAL is their only durable copy.
//!
//! That made them a data-loss path of a different shape from the engines'. The
//! periodic checkpoint reported the core watermark, the manager truncated the
//! segments below it, and the `SyncSeqAdvance` records went with them. On the
//! next restart the gate came back EMPTY, every HWM reset to zero, and frames a
//! producer had already had applied and acknowledged were admitted a second
//! time: `sync_admit` compares `seq <= hwm`, and against a zeroed HWM every
//! replayed frame looks new. The visible failure is not a missing row but a
//! duplicated one — an already-applied sync write re-applied.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/sync-hwm-ckpt/core-{core_id}/STATE
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and the gate maps are per-core state, never shared.
//!
//! ## Why one file and no generation
//!
//! The KV checkpoint publishes a `gen-{n}/` directory named by a separate
//! manifest because its state is split across one file per collection and a
//! multi-file write cannot be made atomic by rename alone. This state is two
//! small maps that live or die together, so it fits in ONE file — and a single
//! `atomic_write_fsync` is already all-or-nothing. A generation would add a
//! second file to keep consistent with the first and buy nothing.
//!
//! ## Why no replay floor
//!
//! Both maps advance by max-wins (`apply_sync_seq_advance`), so re-folding a
//! record already contained in the restored state cannot change it — unlike the
//! KV deltas (`kv_incr`) that `ReplayFloors` exists to gate out. What the
//! restore DOES require is that replay MERGES into it rather than replacing it,
//! which is why `install_sync_hwm_maps` folds max-wins over the current maps
//! instead of assigning them.

mod format;
mod load;
mod paths;
mod write;
