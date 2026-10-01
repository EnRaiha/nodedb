// SPDX-License-Identifier: BUSL-1.1

//! In-flight `DdlPendingPropose` records, keyed by fencing token.

use nodedb_cluster::PendingDdlObject;
use nodedb_types::Hlc;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::super::types::{SystemCatalog, catalog_err};

/// Table: fencing token -> MessagePack `StoredPendingDdl`.
pub(in crate::control::security::catalog) const PENDING_DDL: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.pending_ddl");

/// One persisted pending DDL record.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone, PartialEq)]
#[msgpack(map)]
pub struct StoredPendingDdl {
    pub token: u64,
    pub objects: Vec<PendingDdlObject>,
    pub proposed_at: Hlc,
}

impl SystemCatalog {
    /// Write or replace the pending record of `record.token`.
    pub fn put_pending_ddl(&self, record: &StoredPendingDdl) -> crate::Result<()> {
        let value =
            zerompk::to_msgpack_vec(record).map_err(|e| catalog_err("encode pending_ddl", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("pending_ddl write txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_DDL)
                .map_err(|e| catalog_err("open pending_ddl", e))?;
            table
                .insert(record.token, value.as_slice())
                .map_err(|e| catalog_err("insert pending_ddl", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_ddl", e))
    }

    /// Remove the pending record of `token`. Idempotent.
    pub fn remove_pending_ddl(&self, token: u64) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("pending_ddl remove txn", e))?;
        {
            let mut table = txn
                .open_table(PENDING_DDL)
                .map_err(|e| catalog_err("open pending_ddl", e))?;
            table
                .remove(token)
                .map_err(|e| catalog_err("remove pending_ddl", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("commit pending_ddl remove", e))
    }

    /// Every persisted pending record.
    pub fn load_pending_ddl(&self) -> crate::Result<Vec<StoredPendingDdl>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("pending_ddl read txn", e))?;
        let table = txn
            .open_table(PENDING_DDL)
            .map_err(|e| catalog_err("open pending_ddl", e))?;
        let mut records = Vec::new();
        for item in table
            .range(..)
            .map_err(|e| catalog_err("range pending_ddl", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read pending_ddl", e))?;
            records.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode pending_ddl", e))?,
            );
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_ddl_survives_reopen_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");
        let record = |token| StoredPendingDdl {
            token,
            objects: Vec::new(),
            proposed_at: Hlc::new(5, 1),
        };
        {
            let catalog = SystemCatalog::open(&path).unwrap();
            catalog.put_pending_ddl(&record(7)).unwrap();
            catalog.put_pending_ddl(&record(9)).unwrap();
            catalog.remove_pending_ddl(9).unwrap();
        }
        let catalog = SystemCatalog::open(&path).unwrap();
        assert_eq!(catalog.load_pending_ddl().unwrap(), vec![record(7)]);
    }
}
