// SPDX-License-Identifier: BUSL-1.1

//! Replicated metadata Raft group (group 0).
//!
//! All cluster-wide state — DDL (via opaque host payload), topology,
//! routing, descriptor leases, cluster version — is proposed as a
//! [`MetadataEntry`] against this group and applied on every node via
//! a [`MetadataApplier`].

pub mod applier;
pub mod cache;
pub mod codec;
pub mod compensation;
pub mod descriptors;
pub mod entry;
pub mod migration_recovery;
pub mod migration_state;
pub mod state;

pub use applier::{CacheApplier, MetadataApplier, NoopMetadataApplier};
pub use cache::{MetadataCache, apply_migration_abort, apply_migration_checkpoint};
pub use codec::{decode_entry, encode_entry};
pub use compensation::Compensation;
pub use descriptors::{DescriptorHeader, DescriptorId, DescriptorKind, DescriptorLease};
pub use entry::{MetadataEntry, PendingDdlObject, RoutingChange, TopologyChange};
pub use migration_recovery::{
    DEFAULT_ABORT_TIMEOUT, RecoveryDecision, compensations_for_phase, recover_in_flight_migrations,
};
pub use migration_state::{
    MigrationCheckpointPayload, MigrationId, MigrationPhaseTag, PersistedMigrationCheckpoint,
    SharedMigrationStateTable, new_shared,
};
pub use state::DescriptorState;

/// Well-known Raft group ID for the metadata group.
/// Distinct from data vShard groups (which start at group 1).
pub const METADATA_GROUP_ID: u64 = 0;
