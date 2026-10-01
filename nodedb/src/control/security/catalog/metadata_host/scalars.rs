// SPDX-License-Identifier: BUSL-1.1

//! Single-value host state of the metadata group: the cluster version, the
//! owner token of the DDL preparation lease, the highest applied stamp, and
//! the metadata timeline.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::super::types::{SystemCatalog, catalog_err};

/// Table: scalar name -> value.
pub(in crate::control::security::catalog) const METADATA_HOST_SCALARS: TableDefinition<&str, u64> =
    TableDefinition::new("_system.metadata_host_scalars");

const CLUSTER_VERSION: &str = "cluster_version";
const DDL_OWNER_TOKEN: &str = "ddl_owner_token";
/// The node that owns the DDL preparation lease. Written with the token.
const DDL_OWNER_NODE: &str = "ddl_owner_node";
/// The highest metadata entry stamp applied. The node's HLC starts above it
/// at boot and after a snapshot install, so the stamps a leader takes rise
/// above every entry the log ever held.
const METADATA_STAMP_HWM: &str = "metadata_stamp_hwm";
/// The metadata timeline of the catalog's history.
const METADATA_TIMELINE: &str = "metadata_timeline";

impl SystemCatalog {
    fn put_scalar(&self, name: &str, value: Option<u64>) -> crate::Result<()> {
        self.put_scalars(&[(name, value)])
    }

    /// Write every `(name, value)` in one transaction. `None` removes it.
    fn put_scalars(&self, values: &[(&str, Option<u64>)]) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("metadata scalar write txn", e))?;
        {
            let mut table = txn
                .open_table(METADATA_HOST_SCALARS)
                .map_err(|e| catalog_err("open metadata_host_scalars", e))?;
            for (name, value) in values {
                match value {
                    Some(value) => {
                        table
                            .insert(*name, *value)
                            .map_err(|e| catalog_err("insert metadata scalar", e))?;
                    }
                    None => {
                        table
                            .remove(*name)
                            .map_err(|e| catalog_err("remove metadata scalar", e))?;
                    }
                }
            }
        }
        txn.commit()
            .map_err(|e| catalog_err("commit metadata scalar", e))
    }

    fn get_scalar(&self, name: &str) -> crate::Result<Option<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("metadata scalar read txn", e))?;
        let table = txn
            .open_table(METADATA_HOST_SCALARS)
            .map_err(|e| catalog_err("open metadata_host_scalars", e))?;
        Ok(table
            .get(name)
            .map_err(|e| catalog_err("get metadata scalar", e))?
            .map(|value| value.value()))
    }

    /// Record the applied cluster version.
    pub fn put_cluster_version(&self, version: u16) -> crate::Result<()> {
        self.put_scalar(CLUSTER_VERSION, Some(u64::from(version)))
    }

    /// The applied cluster version, `None` before any bump applied.
    pub fn load_cluster_version(&self) -> crate::Result<Option<u16>> {
        self.get_scalar(CLUSTER_VERSION)?
            .map(|value| {
                u16::try_from(value).map_err(|_| {
                    catalog_err(
                        "decode cluster_version",
                        format!("{value} does not fit a cluster version"),
                    )
                })
            })
            .transpose()
    }

    /// Record the owner of the DDL preparation lease as `(token, node_id)`,
    /// or clear it. Both values change in one transaction.
    pub fn put_ddl_owner(&self, owner: Option<(u64, u64)>) -> crate::Result<()> {
        self.put_scalars(&[
            (DDL_OWNER_TOKEN, owner.map(|(token, _)| token)),
            (DDL_OWNER_NODE, owner.map(|(_, node_id)| node_id)),
        ])
    }

    /// The owner of the DDL preparation lease as `(token, node_id)`, if a
    /// lease is held. A token stored without its node is a corrupt row.
    pub fn load_ddl_owner(&self) -> crate::Result<Option<(u64, u64)>> {
        match (
            self.get_scalar(DDL_OWNER_TOKEN)?,
            self.get_scalar(DDL_OWNER_NODE)?,
        ) {
            (Some(token), Some(node_id)) => Ok(Some((token, node_id))),
            (None, None) => Ok(None),
            (token, node_id) => Err(catalog_err(
                "decode ddl_owner",
                format!(
                    "the DDL preparation owner row is partial: token {token:?}, node {node_id:?}"
                ),
            )),
        }
    }

    /// Raise the highest metadata entry stamp this node applied to `stamp`.
    /// A lower `stamp` writes nothing.
    pub fn raise_metadata_stamp_hwm(&self, stamp: u64) -> crate::Result<()> {
        if self
            .load_metadata_stamp_hwm()?
            .is_some_and(|hwm| hwm >= stamp)
        {
            return Ok(());
        }
        self.put_scalar(METADATA_STAMP_HWM, Some(stamp))
    }

    /// The highest metadata entry stamp this node applied, `None` before any.
    pub fn load_metadata_stamp_hwm(&self) -> crate::Result<Option<u64>> {
        self.get_scalar(METADATA_STAMP_HWM)
    }

    /// Record the metadata timeline the catalog belongs to. A restore sets
    /// it, and a snapshot install carries it to every node.
    pub fn put_metadata_timeline(&self, timeline: u64) -> crate::Result<()> {
        self.put_scalar(METADATA_TIMELINE, Some(timeline))
    }

    /// The metadata timeline the catalog belongs to. A catalog no restore
    /// touched belongs to the root timeline.
    pub fn load_metadata_timeline(&self) -> crate::Result<u64> {
        Ok(self
            .get_scalar(METADATA_TIMELINE)?
            .unwrap_or(crate::storage::metadata_timeline::ROOT_TIMELINE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalars_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            assert_eq!(catalog.load_cluster_version().unwrap(), None);
            catalog.put_cluster_version(4).unwrap();
            catalog.put_ddl_owner(Some((77, 3))).unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_cluster_version().unwrap(), Some(4));
        assert_eq!(catalog.load_ddl_owner().unwrap(), Some((77, 3)));
        catalog.put_ddl_owner(None).unwrap();
        assert_eq!(catalog.load_ddl_owner().unwrap(), None);
    }
}
