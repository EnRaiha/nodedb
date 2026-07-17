// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot restoration: import Data Plane state for all engines.
//!
//! `tenant_snapshot` holds the single dispatch entry point that orchestrates
//! a full-tenant restore across every engine. `engines` holds the per-engine
//! install helpers it calls (sparse/document, vector, KV, CRDT, timeseries).
//! `keys` holds the snapshot-key parsing helpers shared across engines (and,
//! for the timeseries key parser, by `restore_segments.rs`).

mod engines;
mod keys;
mod tenant_snapshot;

pub(in crate::data::executor) use keys::database_id_from_qualified;
pub(in crate::data::executor::handlers::snapshot) use keys::parse_timeseries_snapshot_key;
