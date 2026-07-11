// SPDX-License-Identifier: BUSL-1.1

mod columnar;
mod dispatch;
mod document;
mod kv;
mod reaper;

pub mod progress;
pub mod walker;

pub use progress::CloneMaterializerHandle;
pub use walker::{
    MaterializeParams, force_materialize_blocking, materialize_database, run_scheduled_sweep,
};

// Shared with the `INSERT ... SELECT` orchestrator, which reuses the same
// local-dispatch primitive and source-scan cursor decode.
pub(crate) use dispatch::dispatch_local;
pub(crate) use document::{read_all_source_rows, scan_source_page};
