// SPDX-License-Identifier: BUSL-1.1

//! Convert write-side PhysicalPlan variants to ReplicatedWrite for Raft proposal.
//!
//! Split by the `PhysicalPlan` family each encode helper consumes. The
//! `entry_*` modules hold the exhaustive per-op classification (write vs.
//! not-a-write) for each engine; the sibling modules hold the per-op wire
//! encoders they call into:
//! - [`entry`]: top-level dispatcher (`to_replicated_entry`) + shared
//!   provenance-encoding helper.
//! - [`entry_document`] / [`document`]: `PhysicalPlan::Document`.
//! - [`entry_kv`] / [`kv`]: `PhysicalPlan::Kv`.
//! - [`entry_graph`] / [`graph`]: `PhysicalPlan::Graph`.
//! - [`entry_columnar_family`] / [`columnar`]: `PhysicalPlan::Columnar` /
//!   `Timeseries` / `Text` / `Spatial`.
//! - [`entry_array`]: `PhysicalPlan::Array` — `Put` / `Delete` replicate as the
//!   Raft-native `ArrayCellPut` / `ArrayCellDelete`; `Flush` / reads / DDL don't.
//! - [`vector`]: `PhysicalPlan::Vector` (exhaustive `encode`).
//! - [`crdt`]: `PhysicalPlan::Crdt` (exhaustive `encode`).

mod columnar;
mod crdt;
mod document;
mod entry;
mod entry_array;
mod entry_columnar_family;
mod entry_document;
mod entry_graph;
mod entry_kv;
mod graph;
mod kv;
mod vector;

pub use entry::to_replicated_entry;
