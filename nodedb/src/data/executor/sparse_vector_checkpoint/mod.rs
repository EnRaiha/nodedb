// SPDX-License-Identifier: BUSL-1.1

//! Sparse-vector inverted-index checkpoint write + load operations for
//! `CoreLoop`.
//!
//! ## Why the sparse-vector engine needs this
//!
//! `sparse_vector_indexes` is a plain in-memory `HashMap<_, SparseInvertedIndex>`
//! with no redb store behind it, so its only durable copy is the
//! `SparseVectorPut` / `SparseVectorDelete` WAL records that rebuild it at boot.
//! A flush existed before this module, but it hung off an INDEPENDENT periodic
//! timer in the data-plane runtime, so it had no ordering relationship with the
//! checkpoint that authorises WAL truncation: the checkpoint could report the
//! core watermark and delete the segments holding the sparse-vector records
//! while that timer had not yet fired. The flush is now driven from
//! `execute_checkpoint` and reports the LSN it made durable, so truncation can
//! never outrun it.
//!
//! ## On-disk layout
//!
//! ```text
//! {data_dir}/sparse-vector-ckpt/core-{core_id}/
//!     MANIFEST                                       # names the live generation
//!     gen-{n}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt
//! ```
//!
//! The per-core directory is required because `data_dir` is shared across cores
//! and each core only owns the collections routed to its vShards; it also means
//! the loader needs no core-ownership filter on the filename.
//!
//! ## Why a generation + manifest
//!
//! The flush is per-index and can partially fail. The LSN this checkpoint
//! reports is a deletion authority — WAL segments below it are unlinked — so it
//! has to describe the WHOLE engine, not the indexes that happened to succeed.
//! Writing every index into a fresh `gen-{n}/` and publishing the set with ONE
//! atomic manifest write makes that statement true by construction: either every
//! live index advanced to a single LSN, or the previous generation stays live at
//! its older LSN and the caller clamps to it. A half-published state is not
//! expressible, so a torn or abandoned write is inert garbage rather than a
//! generation whose LSN overstates what is on disk.
//!
//! ## Why no replay floor
//!
//! Unlike KV — whose `kv_incr` / `kv_cas` records are deltas that double-count
//! if replayed over a checkpoint that already folded them in — every
//! sparse-vector record is idempotent: `SparseVectorPut` is an upsert keyed by
//! `doc_id` and `SparseVectorDelete` is a no-op against an absent document (see
//! `wal_replay_vector_extended.rs`, whose replay arms document exactly this and
//! take no watermark gate). Replaying records at or below the restored
//! generation's LSN therefore reproduces the same state rather than corrupting
//! it, so this engine needs no entry in `ReplayFloors`; restoring before replay
//! is the whole requirement.

mod format;
mod load;
mod manifest;
mod paths;
mod write;

#[cfg(test)]
pub(crate) use format::test_manifest_bytes;
pub(crate) use manifest::read_sparse_vector_manifest_at;
pub(crate) use paths::{sparse_vector_checkpoint_prefix, sparse_vector_ckpt_gen_dir};
// Only reclaim's tests build a checkpoint dir from the outside; the write and
// load paths reach `paths` directly.
#[cfg(test)]
pub(crate) use paths::sparse_vector_ckpt_dir;
