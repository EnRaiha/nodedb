// SPDX-License-Identifier: BUSL-1.1

//! Single tick of the Raft event loop — split by concern:
//! - [`core`]: `do_tick` orchestration, learner promotion, and the
//!   `should_promote_learner` decision.
//! - [`dispatch_outbound`]: batch + dispatch outbound AppendEntries /
//!   RequestVote / TimeoutNow messages.
//! - [`apply_committed`]: apply a group's committed entries (conf-changes,
//!   metadata/data applier dispatch, watermark advance, epoch bump).
//! - [`snapshot_dispatch`]: dispatch `InstallSnapshot` RPCs to lagging peers.

mod apply_committed;
mod core;
mod dispatch_outbound;
mod snapshot_dispatch;
