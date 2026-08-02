// SPDX-License-Identifier: BUSL-1.1

//! Cluster mode startup and integration.
//!
//! Bridges `nodedb-cluster` (Raft, transport, routing, metadata group)
//! into the main server. Split into one concern per file:
//!
//! - [`init`] — cluster startup (transport, catalog, bootstrap/join/restart).
//! - [`start_raft`] — Raft event loop + RPC server + applier wiring.
//! - [`handle`] — the `ClusterHandle` passed between init and start_raft.
//! - [`spsc_applier`] — committed data-group entries → SPSC bridge.
//! - [`metadata_applier`] — committed metadata-group entries →
//!   `MetadataCache` + optional redb writeback. The per-Raft-group
//!   apply watermark watchers themselves now live in
//!   [`nodedb_cluster::GroupAppliedWatchers`] and are bumped from
//!   the Raft tick loop so every group (metadata + data) shares one
//!   primitive.

pub mod array_cluster_exec;
pub mod array_cluster_helpers;
pub mod array_executor;
pub mod boot_restore;
pub mod bootstrap_listener;
pub mod calvin;
pub mod handle;
pub mod init;
pub mod metadata_applier;
pub mod pem_io;
pub mod recovery_check;
pub mod sequencer_halt;
pub mod snapshot_applier;
pub mod snapshot_builder;
pub mod snapshot_hook;
pub mod spsc_applier;
pub mod start_raft;
pub mod start_raft_helpers;
pub mod tls;
pub mod warm_peers;

pub use array_cluster_exec::ClusterArrayExecutor;
pub use array_executor::DataPlaneArrayExecutor;
pub use handle::ClusterHandle;
pub use init::{init_cluster, init_cluster_with_transport, init_single_node_calvin};
pub use metadata_applier::MetadataCommitApplier;
pub use recovery_check::{VerifyReport, verify_and_repair};
pub use sequencer_halt::SequencerHaltMarker;
pub use spsc_applier::SpscCommitApplier;
pub use start_raft::start_raft;
pub use tls::resolve_credentials;
pub use warm_peers::{PeerWarmReport, warm_known_peers};
