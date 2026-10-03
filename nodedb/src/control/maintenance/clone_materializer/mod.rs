// SPDX-License-Identifier: BUSL-1.1

mod chained;
mod columnar;
mod dispatch;
mod document;
mod document_copy;
mod kv;
mod reaper;
mod rls_gate;
mod source_drain;
mod status;

pub mod progress;
pub mod walker;

pub use progress::CloneMaterializerHandle;
pub use walker::{MaterializeParams, force_materialize, materialize_database, run_scheduled_sweep};

// Shared with the `INSERT ... SELECT` orchestrator (local dispatch, source
// scan) and the clone copy-up (owner-routed replicated write).
pub(crate) use dispatch::{
    dispatch_local, dispatch_local_on_vshard, dispatch_on_this_node, dispatch_resolve_pass,
    dispatch_to_owner,
};
pub(crate) use document::{read_all_source_rows, scan_source_page};
