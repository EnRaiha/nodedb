// SPDX-License-Identifier: BUSL-1.1

//! Single tick of the Raft event loop — split by concern:
//! - [`core`]: `do_tick` orchestration, learner promotion, and the
//!   `should_promote_learner` decision.
//! - [`dispatch_outbound`]: batch + dispatch outbound AppendEntries /
//!   RequestVote / TimeoutNow messages.
//! - [`apply_committed`]: apply a group's committed entries (conf-changes,
//!   metadata/data applier dispatch, watermark advance, epoch bump).
//! - [`metadata_apply`]: group 0 apply with epochs in log order and the
//!   durable applied floor.
//! - [`snapshot_dispatch`]: dispatch `InstallSnapshot` RPCs to lagging peers.
//! - [`leader_hints`]: write each hosted group's Raft-observed leader into
//!   the routing table's leader hint.
//! - [`metadata_lane`]: apply group 0's committed entries in log order,
//!   off the tick.
//! - [`leader_probe`]: find the leader of a data group this node does not
//!   host when its hint names none.

mod apply_committed;
mod core;
mod dispatch_outbound;
mod epoch_bump;
mod leader_hints;
mod leader_probe;
mod metadata_apply;
pub mod metadata_lane;
mod snapshot_dispatch;
mod vote_dispatch;
