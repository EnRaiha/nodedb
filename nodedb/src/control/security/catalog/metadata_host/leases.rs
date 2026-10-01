// SPDX-License-Identifier: BUSL-1.1

//! Descriptor leases of `MetadataCache.leases`, one row per
//! `(descriptor, holder node)`.

use nodedb_cluster::{DescriptorId, DescriptorLease};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::super::types::{SystemCatalog, catalog_err};

/// Table: MessagePack `LeaseKey` -> MessagePack `DescriptorLease`.
pub(in crate::control::security::catalog) const METADATA_LEASES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.metadata_leases");

/// Row key. A map encoding keeps it deterministic, so the same lease always
/// lands on the same row.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct LeaseKey {
    descriptor_id: DescriptorId,
    node_id: u64,
}

fn lease_key(descriptor_id: &DescriptorId, node_id: u64) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&LeaseKey {
        descriptor_id: descriptor_id.clone(),
        node_id,
    })
    .map_err(|e| catalog_err("encode metadata lease key", e))
}

impl SystemCatalog {
    /// Write or replace the lease `lease.node_id` holds on its descriptor.
    pub fn put_descriptor_lease(&self, lease: &DescriptorLease) -> crate::Result<()> {
        let key = lease_key(&lease.descriptor_id, lease.node_id)?;
        let value =
            zerompk::to_msgpack_vec(lease).map_err(|e| catalog_err("encode metadata lease", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata lease write txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_LEASES)
                .map_err(|e| catalog_err("open metadata_leases", e))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| catalog_err("insert metadata lease", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata lease", e))
    }

    /// Remove the leases `node_id` holds on `descriptor_ids`. Idempotent.
    pub fn remove_descriptor_leases(
        &self,
        node_id: u64,
        descriptor_ids: &[DescriptorId],
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata lease remove txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_LEASES)
                .map_err(|e| catalog_err("open metadata_leases", e))?;
            for descriptor_id in descriptor_ids {
                let key = lease_key(descriptor_id, node_id)?;
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("remove metadata lease", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata lease remove", e))
    }

    /// Every persisted lease.
    pub fn load_descriptor_leases(&self) -> crate::Result<Vec<DescriptorLease>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("metadata lease read txn", e))?;
        let table = txn
            .open_table(METADATA_LEASES)
            .map_err(|e| catalog_err("open metadata_leases", e))?;
        let mut leases = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range metadata_leases", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read metadata lease", e))?;
            leases.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode metadata lease", e))?,
            );
        }
        Ok(leases)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::DescriptorKind;
    use nodedb_types::Hlc;

    use super::*;

    fn lease(name: &str, node_id: u64, expires: u64) -> DescriptorLease {
        DescriptorLease {
            descriptor_id: DescriptorId::new(0, 1, DescriptorKind::Collection, name),
            version: 3,
            node_id,
            expires_at: Hlc::new(expires, 0),
        }
    }

    /// Leases survive a reopen of the catalog file, and a release removes
    /// only the holder's row.
    #[test]
    fn leases_survive_reopen_and_release_removes_one_holder() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_descriptor_lease(&lease("orders", 1, 10))
                .unwrap();
            catalog
                .put_descriptor_lease(&lease("orders", 2, 20))
                .unwrap();
            catalog
                .put_descriptor_lease(&lease("orders", 1, 30))
                .unwrap();
            catalog
                .remove_descriptor_leases(2, &[lease("orders", 2, 0).descriptor_id])
                .unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        let leases = catalog.load_descriptor_leases().unwrap();
        assert_eq!(leases, vec![lease("orders", 1, 30)]);
    }
}
