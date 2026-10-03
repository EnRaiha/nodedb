// SPDX-License-Identifier: BUSL-1.1

//! Descriptor-lease holder liveness.
//!
//! A holder that SWIM declares Dead stays in topology, so the non-member lease
//! filter never drops its leases. Its leases count as expired early only when
//! both of these hold:
//!
//! - SWIM has held it Dead for [`DEAD_HOLDER_LEASE_GRACE`].
//! - The metadata leader has seen no Raft response from it for
//!   [`DEAD_HOLDER_RAFT_SILENCE`].
//!
//! Early release relies on the holder's self-fence: a holder refuses its
//! cached lease once its last metadata-leader contact is older than
//! [`LEASE_SELF_FENCE_WINDOW`]. A holder that still hears the leader applies
//! the release before any further use.
//!
//! Every lease expiry is stamped on the holder's own clock. A lease held by
//! another node therefore counts as live until `MAX_CLOCK_SKEW_NS` past its
//! `expires_at`. A lease held by the local node gets no margin.

pub mod dead_holders;
pub mod raft_contact;

pub use dead_holders::{
    DEAD_HOLDER_LEASE_GRACE, DEAD_HOLDER_RAFT_SILENCE, LEASE_CLOCK_SKEW, LEASE_SELF_FENCE_WINDOW,
    LeaseHolderLiveness, LeaseNow,
};
pub use raft_contact::RaftContactClock;
