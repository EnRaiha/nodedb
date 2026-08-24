// SPDX-License-Identifier: BUSL-1.1

//! redb table definitions, key constants, and shared error helpers
//! for the cluster catalog.
//!
//! These are `pub(super)` so every sibling file in `catalog/` can
//! reach them without re-declaring.

use redb::TableDefinition;

use crate::error::ClusterError;

/// Single-row table for the serialised `ClusterTopology`.
pub(super) const TOPOLOGY_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.topology");

/// Single-row table for the serialised `RoutingTable`.
pub(super) const ROUTING_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.routing");

/// Cluster metadata (cluster_id, catalog format version, CA cert, …).
pub(super) const METADATA_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.metadata");

/// Ghost stub table — persists ghost edge refcounts across restarts.
pub(super) const GHOST_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.ghosts");

/// Migration state table — keyed by `migration_id` (UUID as hyphenated string),
/// value is zerompk-encoded `PersistedMigrationCheckpoint` (the latest checkpoint
/// for that migration).  Rows are upserted on every `MigrationCheckpoint` commit
/// and deleted when a `MigrationAbort` commits with all compensations applied.
pub(super) const MIGRATION_STATE_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("_cluster.migration_state");

pub(super) const KEY_TOPOLOGY: &str = "topology";
pub(super) const KEY_ROUTING: &str = "routing";
pub(super) const KEY_CLUSTER_ID: &str = "cluster_id";
/// Cluster epoch (u64 LE) — monotonic, leader-bumped, stamped on every
/// Raft RPC. Persisted so the local high-water mark survives restart.
pub(super) const KEY_CLUSTER_EPOCH: &str = "cluster_epoch";
/// SWIM incarnation (u64 LE) — the local node's last advertised
/// incarnation, persisted so a fast restart can rejoin at `stored + 1`
/// and dominate any lingering `Dead(stored)` rumour. See
/// `crate::swim::incarnation_store`.
pub(super) const KEY_SWIM_INCARNATION: &str = "swim_incarnation";
pub(super) const KEY_CA_CERT: &str = "ca_cert";
/// Metadata key holding the catalog format version (u32 LE).
/// Stored under this key in the metadata table by `ClusterCatalog::open`.
pub(super) const KEY_FORMAT_VERSION: &str = "format_version";

/// Current catalog format version. First and only version shipped.
pub(super) const CATALOG_FORMAT_VERSION: u32 = 1;

/// Wrap any `std::fmt::Display` error as a transport-layer
/// `ClusterError`. Kept here so every catalog file uses the same
/// error shape.
pub(super) fn catalog_err(e: impl std::fmt::Display) -> ClusterError {
    ClusterError::Transport {
        detail: format!("cluster catalog: {e}"),
    }
}
