// SPDX-License-Identifier: BUSL-1.1

//! Descriptor drains, one row per `(descriptor, owner)`.

use nodedb_cluster::{DescriptorId, DrainOwner};
use nodedb_types::Hlc;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::super::types::{SystemCatalog, catalog_err};

/// Table: MessagePack `DrainKey` -> MessagePack `StoredDrain`.
pub(in crate::control::security::catalog) const METADATA_DRAINS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("_system.metadata_drains");

/// Row key. A map encoding keeps it deterministic.
#[derive(zerompk::ToMessagePack)]
#[msgpack(map)]
struct DrainKey {
    descriptor_id: DescriptorId,
    owner: DrainOwner,
}

/// One owner's drain of one descriptor.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq, Eq)]
#[msgpack(map)]
pub struct StoredDrain {
    pub descriptor_id: DescriptorId,
    pub owner: DrainOwner,
    pub up_to_version: u64,
    pub expires_at: Hlc,
    pub proposer_node_id: u64,
}

fn drain_key(descriptor_id: &DescriptorId, owner: &DrainOwner) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&DrainKey {
        descriptor_id: descriptor_id.clone(),
        owner: owner.clone(),
    })
    .map_err(|e| catalog_err("encode metadata drain key", e))
}

impl SystemCatalog {
    /// Write or replace `drain.owner`'s drain of `drain.descriptor_id`.
    pub fn put_descriptor_drain(&self, drain: &StoredDrain) -> crate::Result<()> {
        let key = drain_key(&drain.descriptor_id, &drain.owner)?;
        let value =
            zerompk::to_msgpack_vec(drain).map_err(|e| catalog_err("encode metadata drain", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata drain write txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_DRAINS)
                .map_err(|e| catalog_err("open metadata_drains", e))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| catalog_err("insert metadata drain", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata drain", e))
    }

    /// Remove each `(descriptor, owner)` drain row. Other owners' rows stay.
    /// Idempotent.
    pub fn remove_descriptor_drains(
        &self,
        drains: &[(DescriptorId, DrainOwner)],
    ) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata drain remove txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_DRAINS)
                .map_err(|e| catalog_err("open metadata_drains", e))?;
            for (descriptor_id, owner) in drains {
                let key = drain_key(descriptor_id, owner)?;
                table
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("remove metadata drain", e))?;
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata drain remove", e))
    }

    /// Every persisted drain.
    pub fn load_descriptor_drains(&self) -> crate::Result<Vec<StoredDrain>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("metadata drain read txn", e))?;
        let table = txn
            .open_table(METADATA_DRAINS)
            .map_err(|e| catalog_err("open metadata_drains", e))?;
        let mut drains = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range metadata_drains", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read metadata drain", e))?;
            drains.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode metadata drain", e))?,
            );
        }
        Ok(drains)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::DescriptorKind;

    use super::*;

    fn drain(owner: DrainOwner, up_to: u64) -> StoredDrain {
        StoredDrain {
            descriptor_id: DescriptorId::new(0, 1, DescriptorKind::Collection, "orders"),
            owner,
            up_to_version: up_to,
            expires_at: Hlc::new(50, 0),
            proposer_node_id: 3,
        }
    }

    /// Drains survive a reopen, and removing one owner's row leaves the
    /// other owner's drain of the same descriptor.
    #[test]
    fn drains_survive_reopen_and_end_per_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let moving = DrainOwner::MoveTenant {
            tenant_id: 1,
            source_db_id: 0,
        };
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog
                .put_descriptor_drain(&drain(DrainOwner::Ddl, 4))
                .unwrap();
            catalog
                .put_descriptor_drain(&drain(moving.clone(), 6))
                .unwrap();
            catalog
                .remove_descriptor_drains(&[(
                    drain(DrainOwner::Ddl, 0).descriptor_id,
                    DrainOwner::Ddl,
                )])
                .unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(
            catalog.load_descriptor_drains().unwrap(),
            vec![drain(moving, 6)]
        );
    }
}
