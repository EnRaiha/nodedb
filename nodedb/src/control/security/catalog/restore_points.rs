// SPDX-License-Identifier: BUSL-1.1

//! `_system.restore_points`: every cluster restore point, keyed by id.
//!
//! The metadata group replicates each point, so every node holds the same
//! rows and any node can list them.

use redb::{ReadableDatabase, ReadableTable, TableError};

use super::types::{SystemCatalog, catalog_err};

/// Redb table: restore point id -> MessagePack [`StoredRestorePoint`].
pub(super) const RESTORE_POINTS: redb::TableDefinition<u64, &[u8]> =
    redb::TableDefinition::new("_system.restore_points");

/// One cluster restore point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredRestorePoint {
    /// The metadata log index of the entry that created the point.
    pub id: u64,
    /// The point's watermark: HLC wall time in nanoseconds.
    pub hlc: u64,
    /// Wall-clock milliseconds when the point was requested.
    pub created_at_ms: u64,
}

impl SystemCatalog {
    /// Record `point`. A second write of the same id overwrites it with the
    /// same values.
    pub fn put_restore_point(&self, point: &StoredRestorePoint) -> crate::Result<()> {
        let bytes =
            zerompk::to_msgpack_vec(point).map_err(|e| catalog_err("encode restore point", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("restore point write txn", e))?;
        {
            let mut table = txn
                .open_table(RESTORE_POINTS)
                .map_err(|e| catalog_err("open restore points", e))?;
            table
                .insert(point.id, bytes.as_slice())
                .map_err(|e| catalog_err("insert restore point", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("restore point commit", e))
    }

    /// Every restore point, oldest first.
    pub fn list_restore_points(&self) -> crate::Result<Vec<StoredRestorePoint>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("restore point read txn", e))?;
        let table = match txn.open_table(RESTORE_POINTS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(e) => return Err(catalog_err("open restore points", e)),
        };
        let mut points = Vec::new();
        for row in table
            .range(..)
            .map_err(|e| catalog_err("range restore points", e))?
        {
            let (_, value) = row.map_err(|e| catalog_err("read restore point", e))?;
            points.push(
                zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("decode restore point", e))?,
            );
        }
        Ok(points)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn points_list_in_id_order_and_rewrites_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        assert!(catalog.list_restore_points().unwrap().is_empty());
        let later = StoredRestorePoint {
            id: 20,
            hlc: 2_000,
            created_at_ms: 2,
        };
        let earlier = StoredRestorePoint {
            id: 10,
            hlc: 1_000,
            created_at_ms: 1,
        };
        catalog.put_restore_point(&later).unwrap();
        catalog.put_restore_point(&earlier).unwrap();
        catalog.put_restore_point(&earlier).unwrap();
        assert_eq!(catalog.list_restore_points().unwrap(), vec![earlier, later]);
    }
}
