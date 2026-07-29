// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta-apply handler: validate + materialize an applied Loro delta,
//! for both the non-sync (SQL / native client) and sync (peer) paths.

mod entry;
mod frontier;
mod gated;
mod local;
mod params;
mod write_set;

pub(crate) use params::CRDT_PENDING_DEPENDENCIES;
pub(in crate::data::executor) use params::CrdtApplyParams;
