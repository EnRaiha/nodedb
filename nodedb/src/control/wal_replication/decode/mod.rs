// SPDX-License-Identifier: BUSL-1.1

//! Convert committed ReplicatedWrite entries back to PhysicalPlan for Data Plane execution.
//!
//! Split by the `PhysicalPlan` family each decode helper produces:
//! - [`entry`]: dispatcher (`from_replicated_entry`).
//! - [`ctx`]: shared `DecodeCtx` + surrogate-binding helpers.
//! - [`document`]: `PhysicalPlan::Document` producers.
//! - [`vector`]: `PhysicalPlan::Vector` producers.
//! - [`graph`]: `PhysicalPlan::Graph` producers.
//! - [`kv`]: `PhysicalPlan::Kv` producers.
//! - [`crdt`]: `PhysicalPlan::Crdt` producers.
//! - [`columnar`]: `PhysicalPlan::Columnar` producers.

mod columnar;
mod crdt;
mod ctx;
mod document;
mod entry;
mod graph;
mod kv;
mod vector;

pub use entry::from_replicated_entry;
