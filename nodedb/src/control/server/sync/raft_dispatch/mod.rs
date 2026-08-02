// SPDX-License-Identifier: BUSL-1.1

//! Sync write dispatch: authorization, Raft proposal, and the two result
//! shapes (`Response` and raw payload bytes) the sync handlers consume.

pub mod admission_guard;
pub mod authorize;
#[cfg(test)]
mod durability_test_support;
pub mod outcome;
pub mod propose;
pub mod response;
pub mod write;

pub use authorize::{authorize_sync_collection, authorize_sync_task};
pub use outcome::SyncDispatchOutcome;
pub(crate) use response::dispatch_trusted_internal_sync_response;
pub use response::{dispatch_authorized_sync_response, dispatch_sync_payload, noop_dispatch_error};
pub use write::{dispatch_sync_bytes, dispatch_write_replicated};
