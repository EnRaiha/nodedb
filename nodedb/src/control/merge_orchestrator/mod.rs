// SPDX-License-Identifier: BUSL-1.1

pub mod expand_staged_merge;
pub mod orchestrator;
pub mod resolve_arms;

pub(crate) use expand_staged_merge::resolve_and_emit_merge_ops;
pub use orchestrator::run_authorized_merge;
