// SPDX-License-Identifier: BUSL-1.1

//! Persistent per-group tenant write marks backing
//! `_system.tenant_group_marks` and `_system.tenant_group_restore_marks`.
//!
//! For each data group this node replicates, the newest commit HLC of any
//! write of each tenant the group applied. The apply loop writes a group's
//! marks before it saves the applied floor that covers them, so every
//! committed entry is either covered by a persisted mark or above the floor,
//! where Raft delivers it again after a restart and the loop derives its mark
//! again. A Calvin commit writes its mark before its install is acknowledged.
//!
//! A write a RESTORE re-issued keeps its mark apart, with the id of the
//! restore that wrote it, so a retry of that restore can tell its own writes
//! from every other write.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::types::{SystemCatalog, catalog_err};

/// Table: `(group_id, tenant_id)` -> `(commit_hlc, site_code, collection)`.
pub(super) const TENANT_GROUP_MARKS: TableDefinition<(u64, u64), (u64, u8, &str)> =
    TableDefinition::new("_system.tenant_group_marks");

/// Table: `(group_id, tenant_id)` -> `(commit_hlc, restore_id, collection)`
/// of the newest write a RESTORE re-issued.
pub(super) const TENANT_GROUP_RESTORE_MARKS: TableDefinition<(u64, u64), (u64, u64, &str)> =
    TableDefinition::new("_system.tenant_group_restore_marks");

/// One persisted mark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredGroupMark {
    pub group_id: u64,
    pub tenant_id: u64,
    /// HLC wall time, in nanoseconds, of the newest write.
    pub hlc: u64,
    /// Which apply path recorded the write.
    pub site: u8,
    /// The collection the write named, empty when it named none.
    pub collection: String,
    /// The restore that re-issued the write, `0` for any other write.
    pub restore_id: u64,
}

impl SystemCatalog {
    /// Every persisted mark, user and restore alike.
    pub fn load_tenant_group_marks(&self) -> crate::Result<Vec<StoredGroupMark>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("load_tenant_group_marks read txn", e))?;
        let mut marks = Vec::new();
        let table = read_txn
            .open_table(TENANT_GROUP_MARKS)
            .map_err(|e| catalog_err("open tenant_group_marks", e))?;
        for entry in table
            .iter()
            .map_err(|e| catalog_err("iterate tenant_group_marks", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read tenant_group_mark", e))?;
            let (group_id, tenant_id) = key.value();
            let (hlc, site, collection) = value.value();
            marks.push(StoredGroupMark {
                group_id,
                tenant_id,
                hlc,
                site,
                collection: collection.to_owned(),
                restore_id: 0,
            });
        }
        let restore = read_txn
            .open_table(TENANT_GROUP_RESTORE_MARKS)
            .map_err(|e| catalog_err("open tenant_group_restore_marks", e))?;
        for entry in restore
            .iter()
            .map_err(|e| catalog_err("iterate tenant_group_restore_marks", e))?
        {
            let (key, value) =
                entry.map_err(|e| catalog_err("read tenant_group_restore_mark", e))?;
            let (group_id, tenant_id) = key.value();
            let (hlc, restore_id, collection) = value.value();
            marks.push(StoredGroupMark {
                group_id,
                tenant_id,
                hlc,
                site: RESTORE_SITE_CODE,
                collection: collection.to_owned(),
                restore_id,
            });
        }
        Ok(marks)
    }

    /// Raise every mark in `marks` in one transaction. A persisted mark at or
    /// above the new one stays. A mark with a `restore_id` goes to the
    /// restore table.
    pub fn raise_tenant_group_marks(&self, marks: &[StoredGroupMark]) -> crate::Result<()> {
        if marks.is_empty() {
            return Ok(());
        }
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("raise_tenant_group_marks txn", e))?;
        {
            let mut table = write_txn
                .open_table(TENANT_GROUP_MARKS)
                .map_err(|e| catalog_err("open tenant_group_marks", e))?;
            let mut restore = write_txn
                .open_table(TENANT_GROUP_RESTORE_MARKS)
                .map_err(|e| catalog_err("open tenant_group_restore_marks", e))?;
            for mark in marks {
                let key = (mark.group_id, mark.tenant_id);
                if mark.restore_id == 0 {
                    let current = table
                        .get(key)
                        .map_err(|e| catalog_err("get tenant_group_mark", e))?
                        .map(|guard| guard.value().0);
                    if current.is_some_and(|hlc| hlc >= mark.hlc) {
                        continue;
                    }
                    table
                        .insert(key, (mark.hlc, mark.site, mark.collection.as_str()))
                        .map_err(|e| catalog_err("insert tenant_group_mark", e))?;
                } else {
                    let current = restore
                        .get(key)
                        .map_err(|e| catalog_err("get tenant_group_restore_mark", e))?
                        .map(|guard| guard.value().0);
                    if current.is_some_and(|hlc| hlc >= mark.hlc) {
                        continue;
                    }
                    restore
                        .insert(key, (mark.hlc, mark.restore_id, mark.collection.as_str()))
                        .map_err(|e| catalog_err("insert tenant_group_restore_mark", e))?;
                }
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit tenant_group_marks", e))
    }

    /// Replace every persisted mark, user and restore alike, with `marks`.
    /// A cluster restore rebuilds the marks at its point, offline: the clear
    /// and the writes commit apart, and a failed restore empties the data
    /// directory.
    pub fn replace_tenant_group_marks(&self, marks: &[StoredGroupMark]) -> crate::Result<()> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("replace_tenant_group_marks txn", e))?;
        write_txn
            .delete_table(TENANT_GROUP_MARKS)
            .map_err(|e| catalog_err("clear tenant_group_marks", e))?;
        write_txn
            .delete_table(TENANT_GROUP_RESTORE_MARKS)
            .map_err(|e| catalog_err("clear tenant_group_restore_marks", e))?;
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit tenant_group_marks clear", e))?;
        self.raise_tenant_group_marks(marks)
    }
}

/// The site code a restore mark carries. It matches
/// `MarkSite::Restore::code()`.
pub const RESTORE_SITE_CODE: u8 = 3;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_and_restore_marks_persist_apart() {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        let user = StoredGroupMark {
            group_id: 2,
            tenant_id: 1,
            hlc: 100,
            site: 0,
            collection: "docs".into(),
            restore_id: 0,
        };
        let restore = StoredGroupMark {
            hlc: 200,
            site: RESTORE_SITE_CODE,
            restore_id: 77,
            ..user.clone()
        };
        catalog
            .raise_tenant_group_marks(&[user.clone(), restore.clone()])
            .expect("raise");
        let mut loaded = catalog.load_tenant_group_marks().expect("load");
        loaded.sort_by_key(|m| m.restore_id);
        assert_eq!(loaded, vec![user, restore]);
    }
}
