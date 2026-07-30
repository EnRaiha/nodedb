// SPDX-License-Identifier: BUSL-1.1

//! `plan_requires_txn_buffering`: is this in-transaction statement a write
//! that must be buffered until COMMIT, or a read that executes immediately?
//!
//! `route_in_tx_write` (`control/server/shared/session/staging_gate.rs`) used
//! to answer this by calling `to_replicated_entry(..).is_some()` — but that
//! function is a WAL/Raft ENCODER, not a classifier: it ends in a catch-all
//! `_ => None`, so any `PhysicalPlan` variant it has no encoder arm for is
//! silently treated as a read (executed immediately, visible before COMMIT,
//! and NOT rolled back by ROLLBACK).
//!
//! `plan_requires_txn_buffering` reproduces `to_replicated_entry(..).is_some()`
//! variant-for-variant as a compile-time-exhaustive match, so a new
//! `PhysicalPlan` variant forces an explicit staging decision instead of
//! silently inheriting a wrong default. For most arms this is a
//! behavior-preserving reclassification, not a bug fix: every remaining
//! `false` arm below that is semantically a write (`required_permission` says
//! `Write`) carries a comment stating that fact about TODAY's behavior, and
//! fixing those is a separate, later change.
//!
//! One documented set of arms is the exception:
//! `DocumentOp::{Merge, UpdateFromJoin}` and `CrdtOp::RestoreToVersion`
//! classify `true` here even though `to_replicated_entry` has no encoder arm
//! for any of them — a deliberate divergence from the oracle (see the
//! equivalence test below, which pins the divergence explicitly rather than
//! papering over it).
//! `ArrayOp::{Put, Delete}` used to be in this same exception list, but
//! `to_replicated_entry` now has encoder arms for both (see
//! `control/wal_replication/encode/entry_array.rs::array_write`, which emits
//! the Raft-native `ArrayCellPut` / `ArrayCellDelete` cluster-write variants),
//! so they no longer diverge from the oracle and were moved into
//! `array_and_cluster_array_variants_match_oracle` below.
//! `VectorOp::{DeleteBySurrogate, SparseInsert, SparseDelete,
//! MultiVectorInsert, MultiVectorDelete, DirectUpsert}` used to be in this
//! same exception list, but `to_replicated_entry` now has encoder arms for
//! all six (see `control/wal_replication/encode/vector.rs::encode`), so they
//! no longer diverge from the oracle and were moved to
//! `vector_variants_match_oracle` below.
//! `CrdtOp::{ListInsert, ListDelete, ListMove}` similarly used to be in this
//! exception list, but `to_replicated_entry` now has encoder arms for all
//! three (see `control/wal_replication/encode/crdt.rs::encode`), so they no
//! longer diverge from the oracle and were moved into
//! `crdt_variants_match_oracle` below.
//! `DocumentOp::BatchInsert` and `CrdtOp::{SetConstraints, DropConstraints}`
//! were also in this exception list, but `to_replicated_entry` now has
//! encoder arms for all three (see
//! `control/wal_replication/encode/document.rs::encode` and
//! `control/wal_replication/encode/crdt.rs::encode`), so they too no longer
//! diverge from the oracle and were moved into `document_variants_match_oracle`
//! / `crdt_variants_match_oracle` below. Without buffering,
//! each of these executed immediately against base state inside an explicit
//! transaction, was visible before COMMIT, and survived ROLLBACK: a
//! correctness bug, not a classification nuance. Closing it costs two
//! deliberate, documented trade-offs:
//!
//! 1. RYOW LOSS: a `Buffered` plan does not stage into the per-transaction
//!    overlay, so a read later in the SAME transaction does not observe the
//!    write until COMMIT. This matches how bulk Document DML already behaved
//!    in a transaction before it was staged.
//! 2. NO-UNDO GAP (pre-existing, not fixed here): every flipped variant
//!    reaches `exec_tx_passthrough`
//!    (`data/executor/handlers/transaction/sub_plan_write.rs`) at COMMIT,
//!    which pushes no `UndoEntry`. If a sibling sub-plan fails later in the
//!    same COMMIT batch, these writes cannot be reversed by
//!    `rollback_undo_log`. Spatial, Text, and bulk Document writes already
//!    ride this exact path.
//!
//! A second, inverse divergence exists in the opposite direction:
//! `DocumentOp::Truncate` and `KvOp::{Truncate, RegisterIndex, DropIndex}`
//! classify `false` here (not buffered) even though `to_replicated_entry` has
//! an encoder arm for each and returns `Some`. This is not a bug in either
//! function: every one of them is autocommit-only — `resolve/entry.rs`
//! (`data/executor/handlers/transaction/resolve/entry.rs:329-334` for the Kv
//! index/DDL/truncate arm, `:394-400` for the Document arm) rejects them with
//! `PlanError` when they appear inside an explicit transaction, so they are
//! never routed through `plan_requires_txn_buffering` for staging in practice.
//! They only ever reach `to_replicated_entry` via the autocommit path, where
//! they replicate normally. Pinned by
//! `truncate_and_index_variants_are_encoded_but_not_buffered` below via
//! `assert_encoded_but_not_buffered` — the inverse of
//! `assert_buffered_but_unencoded` — and correspondingly excluded from
//! `kv_variants_match_oracle`.

#![deny(clippy::wildcard_enum_match_arm)]

pub mod classify;

pub use classify::plan_requires_txn_buffering;
