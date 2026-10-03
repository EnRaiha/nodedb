// SPDX-License-Identifier: BUSL-1.1

//! The node's boot epoch: a counter raised and made durable at every boot.
//!
//! A peer keeps its replay window for this node across this node's restart.
//! So every boot sends its frames in a sequence range above every range an
//! earlier boot used (see [`crate::rpc_codec::PeerSeqSender::enter_boot_epoch`]).
//! The counter lives in the catalog, not the clock: a clock stepped back, a
//! bad RTC or a restored VM snapshot never lowers it.

use redb::ReadableTable;

use crate::error::Result;

use super::core::ClusterCatalog;
use super::schema::{KEY_BOOT_EPOCH, METADATA_TABLE, catalog_err};

impl ClusterCatalog {
    /// Raise the boot epoch by one, commit it durably, and return it. The
    /// first boot returns 1. Blocks on disk: call it off the async threads,
    /// once per boot, before any transport sends.
    pub fn advance_boot_epoch(&self) -> Result<u64> {
        let txn = self.db.begin_write().map_err(catalog_err)?;
        let epoch = {
            let mut table = txn.open_table(METADATA_TABLE).map_err(catalog_err)?;
            let stored = match table.get(KEY_BOOT_EPOCH).map_err(catalog_err)? {
                Some(guard) => {
                    let bytes: [u8; 8] = guard.value().try_into().map_err(|_| {
                        catalog_err(format!(
                            "metadata key {KEY_BOOT_EPOCH} has unexpected length {} (expected 8)",
                            guard.value().len()
                        ))
                    })?;
                    u64::from_le_bytes(bytes)
                }
                None => 0,
            };
            let epoch = stored.checked_add(1).ok_or_else(|| {
                catalog_err(format!(
                    "metadata key {KEY_BOOT_EPOCH} is exhausted at {stored}"
                ))
            })?;
            table
                .insert(KEY_BOOT_EPOCH, epoch.to_le_bytes().as_slice())
                .map_err(catalog_err)?;
            epoch
        };
        txn.commit().map_err(catalog_err)?;
        Ok(epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_boot_gets_a_higher_epoch_that_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("cluster.redb");
        {
            let catalog = ClusterCatalog::open(&path).expect("open");
            assert_eq!(catalog.advance_boot_epoch().expect("advance"), 1);
            assert_eq!(catalog.advance_boot_epoch().expect("advance"), 2);
        }
        let catalog = ClusterCatalog::open(&path).expect("reopen");
        assert_eq!(catalog.advance_boot_epoch().expect("advance"), 3);
    }
}
