// SPDX-License-Identifier: BUSL-1.1

//! Convert write-side PhysicalPlan variants to ReplicatedWrite for Raft proposal.
//!
//! Split by the `PhysicalPlan` family each encode helper consumes:
//! - [`entry`]: dispatcher (`to_replicated_entry`) + shared provenance-encoding helper.
//! - [`document`]: `PhysicalPlan::Document` encoders.
//! - [`vector`]: `PhysicalPlan::Vector` encoders.
//! - [`graph`]: `PhysicalPlan::Graph` encoders.
//! - [`kv`]: `PhysicalPlan::Kv` encoders.
//! - [`crdt`]: `PhysicalPlan::Crdt` encoders.
//! - [`columnar`]: `PhysicalPlan::Columnar` / `Timeseries` / `Text` / `Spatial` encoders.

mod columnar;
mod crdt;
mod document;
mod entry;
mod graph;
mod kv;
mod vector;

pub use entry::to_replicated_entry;
