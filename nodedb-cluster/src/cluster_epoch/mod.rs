// SPDX-License-Identifier: BUSL-1.1

//! Cluster generation/epoch — a fence token stamped on every Raft RPC frame.
//!
//! # What it is for
//!
//! Raft terms already fence stale participants inside a group. The epoch
//! answers the question a term cannot: whether a node's view of the CLUSTER —
//! routing, placement, membership — has been superseded, even though its
//! per-group terms are perfectly current. A node that missed a topology
//! transition can hold entirely valid terms and still plan work against a map
//! that no longer matches the cluster.
//!
//! # How a generation advances
//!
//! The metadata-group leader proposes a [`MetadataEntry::ClusterEpochBump`] on
//! acquiring leadership, and every node advances when it APPLIES that committed
//! entry. Going through the log is the point. An epoch that each node inferred
//! from stamps it happened to overhear would have no agreed value and no
//! ordering — a node could claim a generation it had never processed simply by
//! receiving one frame from a peer that had.
//!
//! # What a node does with it
//!
//! It fences ITSELF. Finding `observed > applied` means some peer has applied a
//! transition this node has not, so the node stands down from coordinating work
//! until the metadata group delivers the bump. It never rejects a peer for
//! being behind: a node knows its own applied state exactly and only ever
//! guesses at where its peers are. Raft traffic is never fenced — that traffic
//! is how a node catches up.
//!
//! See [`state`] for the applied/observed distinction in detail.
//!
//! [`MetadataEntry::ClusterEpochBump`]: crate::metadata_group::entry::MetadataEntry::ClusterEpochBump

pub mod persistence;
pub mod state;

pub use persistence::{load_persisted_epoch, persist_applied_epoch};
pub use state::ClusterEpochState;
