// SPDX-License-Identifier: BUSL-1.1

//! Single-node integration test for surrogate identity round-trip.
//!
//! Asserts three properties:
//!
//! 1. **Per-engine uniqueness** — the surrogate values assigned to rows in
//!    each engine are distinct from one another within the engine's own
//!    insert batch (no engine re-uses a surrogate for two different rows).
//!
//! 2. **Global uniqueness** — all surrogates across every engine form a
//!    set with no duplicates.
//!
//! 3. **Cross-engine bitmap intersection** — a three-way intersection of
//!    `SurrogateBitmap`s produced from Vector, Graph, and Array engine
//!    inserts yields the correct set, and actually running queries with
//!    the intersected bitmap as `filter_bitmap` / `frontier_bitmap` /
//!    `cell_filter` returns only rows whose surrogate is in the
//!    intersection.
//!
//! The test drives a single `CoreLoop` via the ring-buffer harness (the
//! same pattern as `cross_engine_bitmap_currency.rs`) so no server or
//! network is required.

mod harness;
mod inserts;
mod intersection;
mod round_trip;
mod uniqueness;
