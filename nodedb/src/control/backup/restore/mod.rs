// SPDX-License-Identifier: BUSL-1.1

//! RESTORE TENANT — module root.
//!
//! Submodule wiring only. All restore orchestrator logic lives in
//! [`orchestrate`]; column/timeseries re-issue helpers in their respective
//! submodules; supporting primitives in `remote`, `sections`, and `topology`.

pub mod columnar_reissue;
mod orchestrate;
mod remote;
mod sections;
pub mod timeseries_reissue;
mod topology;

pub use orchestrate::{RestoreStats, restore_tenant};
