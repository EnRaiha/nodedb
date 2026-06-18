// SPDX-License-Identifier: BUSL-1.1

//! [`KeyExtractor`] trait and a placeholder implementation.
//!
//! Key-partitioned routing requires extracting shard keys from a
//! [`PhysicalPlan`] before the router can produce per-key [`TaskRoute`]s. Each
//! key-partitioned engine (graph node-id, array tile) supplies its own extractor;
//! the router stays engine-agnostic by calling through this trait.
//!
//! No collection carries `PartitionStrategy::KeyPartitioned` yet, so any plan
//! that somehow reaches the `KeyPartitioned` arm returns a hard typed
//! `Error::PlanError` — never a panic, never a silent empty (which would
//! misroute every row to vShard 0).

use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_types::KeySpec;

use crate::Result;

/// Extracts the raw shard keys from a plan for key-partitioned routing.
///
/// Implementations are engine-specific (graph node-id, array tile); the router
/// calls this trait and stays engine-agnostic.
pub trait KeyExtractor: Send + Sync {
    /// Extract the shard keys that the plan's primary write/read targets.
    ///
    /// Each returned `Vec<u8>` is passed to [`VShardId::from_key`] to produce
    /// a vShard id; the router builds one [`TaskRoute`] per distinct vShard.
    ///
    /// [`VShardId::from_key`]: nodedb_types::id::VShardId::from_key
    fn extract_keys(&self, plan: &PhysicalPlan, key_spec: &KeySpec) -> Result<Vec<Vec<u8>>>;
}

/// Placeholder extractor: returns a hard error for any key-partitioned collection.
///
/// No collection carries `PartitionStrategy::KeyPartitioned` yet, so the router
/// never calls this in practice. It exists as a typed sentinel so the
/// `KeyPartitioned` arm can return a real `Err` rather than `todo!()`/panic.
/// Engine-specific extractors (graph node-id, array tile) replace it once
/// key-partitioned routing is wired.
pub struct UnwiredKeyExtractor;

impl KeyExtractor for UnwiredKeyExtractor {
    fn extract_keys(&self, _plan: &PhysicalPlan, key_spec: &KeySpec) -> Result<Vec<Vec<u8>>> {
        Err(crate::Error::PlanError {
            detail: format!("key-partitioned routing not yet wired for key_spec={key_spec:?}"),
        })
    }
}
