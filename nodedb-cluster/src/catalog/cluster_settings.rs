// SPDX-License-Identifier: BUSL-1.1

//! Cluster-wide settings persisted in the catalog.
//!
//! [`ClusterSettings`] is written once at bootstrap (from the operator's
//! [`ClusterConfig`]) and read back on every open. Changing these values
//! after the first bootstrap requires a coordinated cluster-wide migration;
//! the format version guard in `migration.rs` gates upgrades.

use redb::ReadableDatabase;

use crate::catalog::core::ClusterCatalog;
use crate::catalog::schema::{METADATA_TABLE, catalog_err};
use crate::error::Result;

/// Key under which the zerompk-encoded `ClusterSettings` blob is stored
/// in the metadata table.
pub(super) const KEY_CLUSTER_SETTINGS: &str = "cluster_settings";

/// Which hash algorithm is used to partition keys across vShards.
///
/// The value is persisted as a `u8` discriminant — the numeric
/// representation must remain stable across software versions.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
pub enum PlacementHashId {
    /// FNV-1a 64-bit — the cluster default prior to launch.
    Fnv1a = 0,
    /// xxHash3 64-bit — higher throughput at large key sizes.
    XxHash3 = 1,
}

/// Cluster-wide settings that are fixed at bootstrap time.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ClusterSettings {
    /// Hash algorithm used to map row keys to vShards.
    pub placement_hash_id: PlacementHashId,
    /// Number of virtual shards in this cluster. Must match the
    /// `VSHARD_COUNT` constant used when the routing table was built.
    pub vshard_count: u32,
    /// Number of replicas per Raft group.
    pub replication_factor: u32,
    /// Minimum wire-protocol version that peers must speak.
    pub min_wire_version: u16,
}

impl Default for ClusterSettings {
    fn default() -> Self {
        Self {
            placement_hash_id: PlacementHashId::Fnv1a,
            vshard_count: crate::routing::VSHARD_COUNT,
            replication_factor: 1,
            min_wire_version: 1,
        }
    }
}

impl ClusterSettings {
    /// Construct settings from a [`ClusterConfig`].
    pub fn from_config(config: &crate::bootstrap::ClusterConfig) -> Self {
        Self {
            placement_hash_id: PlacementHashId::Fnv1a,
            vshard_count: crate::routing::VSHARD_COUNT,
            replication_factor: config.replication_factor as u32,
            min_wire_version: 1,
        }
    }
}

/// Dispatch a key through the configured placement hash, returning a
/// deterministic 64-bit value. The caller is responsible for reducing
/// the output into a vShard ID (e.g. `% vshard_count`).
pub fn placement_hash(id: PlacementHashId, key: &[u8]) -> u64 {
    match id {
        PlacementHashId::Fnv1a => {
            // FNV-1a 64-bit (offset basis 0xcbf29ce484222325, prime 0x100000001b3).
            let mut hash: u64 = 0xcbf29ce484222325;
            for byte in key {
                hash ^= *byte as u64;
                hash = hash.wrapping_mul(0x100000001b3);
            }
            hash
        }
        PlacementHashId::XxHash3 => xxhash_rust::xxh3::xxh3_64(key),
    }
}

// ── ClusterCatalog methods ───────────────────────────────────────────────────

impl ClusterCatalog {
    /// Persist the cluster settings blob. Called once at bootstrap.
    pub fn save_cluster_settings(&self, settings: &ClusterSettings) -> Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(settings).map_err(|e| crate::error::ClusterError::Codec {
                detail: format!("serialize ClusterSettings: {e}"),
            })?;
        let txn = self.db.begin_write().map_err(catalog_err)?;
        {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            table
                .insert(KEY_CLUSTER_SETTINGS, bytes.as_slice())
                .map_err(catalog_err)?;
        }
        txn.commit().map_err(catalog_err)?;
        Ok(())
    }

    /// Load the cluster settings. Returns `None` if not yet bootstrapped.
    pub fn load_cluster_settings(&self) -> Result<Option<ClusterSettings>> {
        let txn = self.db.begin_read().map_err(catalog_err)?;
        let table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
        match table.get(KEY_CLUSTER_SETTINGS).map_err(catalog_err)? {
            Some(guard) => {
                let settings = zerompk::from_msgpack(guard.value()).map_err(|e| {
                    crate::error::ClusterError::Codec {
                        detail: format!("deserialize ClusterSettings: {e}"),
                    }
                })?;
                Ok(Some(settings))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ClusterCatalog;

    fn temp_catalog() -> (tempfile::TempDir, ClusterCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.redb");
        let catalog = ClusterCatalog::open(&path).unwrap();
        (dir, catalog)
    }

    // ── zerompk roundtrip ───────────────────────────────────────────

    #[test]
    fn cluster_settings_msgpack_roundtrip_fnv1a() {
        let original = ClusterSettings {
            placement_hash_id: PlacementHashId::Fnv1a,
            vshard_count: 1024,
            replication_factor: 3,
            min_wire_version: 1,
        };
        let bytes = zerompk::to_msgpack_vec(&original).unwrap();
        let decoded: ClusterSettings = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn cluster_settings_msgpack_roundtrip_xxhash3() {
        let original = ClusterSettings {
            placement_hash_id: PlacementHashId::XxHash3,
            vshard_count: 512,
            replication_factor: 1,
            min_wire_version: 2,
        };
        let bytes = zerompk::to_msgpack_vec(&original).unwrap();
        let decoded: ClusterSettings = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    // ── catalog persistence ─────────────────────────────────────────

    #[test]
    fn save_and_load_cluster_settings() {
        let (_dir, catalog) = temp_catalog();
        assert_eq!(catalog.load_cluster_settings().unwrap(), None);

        let settings = ClusterSettings {
            placement_hash_id: PlacementHashId::Fnv1a,
            vshard_count: 1024,
            replication_factor: 3,
            min_wire_version: 1,
        };
        catalog.save_cluster_settings(&settings).unwrap();

        let loaded = catalog.load_cluster_settings().unwrap().unwrap();
        assert_eq!(loaded, settings);
    }

    #[test]
    fn save_cluster_settings_overwrite_roundtrip() {
        let (_dir, catalog) = temp_catalog();

        let first = ClusterSettings {
            placement_hash_id: PlacementHashId::Fnv1a,
            vshard_count: 1024,
            replication_factor: 1,
            min_wire_version: 1,
        };
        catalog.save_cluster_settings(&first).unwrap();

        let updated = ClusterSettings {
            placement_hash_id: PlacementHashId::XxHash3,
            vshard_count: 1024,
            replication_factor: 3,
            min_wire_version: 2,
        };
        catalog.save_cluster_settings(&updated).unwrap();

        let loaded = catalog.load_cluster_settings().unwrap().unwrap();
        assert_eq!(loaded, updated);
    }

    // ── placement_hash dispatch ─────────────────────────────────────

    #[test]
    fn placement_hash_deterministic() {
        let key = b"my-collection-key";
        let a = placement_hash(PlacementHashId::Fnv1a, key);
        let b = placement_hash(PlacementHashId::Fnv1a, key);
        assert_eq!(a, b, "FNV-1a must be deterministic");

        let c = placement_hash(PlacementHashId::XxHash3, key);
        let d = placement_hash(PlacementHashId::XxHash3, key);
        assert_eq!(c, d, "XxHash3 must be deterministic");
    }

    #[test]
    fn placement_hash_different_ids_produce_different_values() {
        let key = b"my-collection-key";
        let fnv = placement_hash(PlacementHashId::Fnv1a, key);
        let xx3 = placement_hash(PlacementHashId::XxHash3, key);
        assert_ne!(fnv, xx3, "FNV-1a and XxHash3 must differ for the same key");
    }
}
