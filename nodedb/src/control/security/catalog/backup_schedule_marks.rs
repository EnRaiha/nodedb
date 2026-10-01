// SPDX-License-Identifier: BUSL-1.1

//! `_system.backup_schedule_marks`: how far each scheduled backup has run.
//!
//! The metadata group replicates every mark, so every node holds the same
//! rows. The node that runs scheduled backups reads them to pick the due
//! minute, and a node that takes that role later reads the same rows.
//!
//! A row is keyed by the job and its config incarnation. The incarnation is
//! a fingerprint of the schedule's database, target and cron. A changed
//! schedule is another incarnation, so an old mark never suppresses a run
//! of the new schedule.

use redb::{ReadableDatabase, ReadableTable, TableError};

use super::types::{SystemCatalog, catalog_err};

/// Redb table: mark key -> MessagePack [`StoredBackupScheduleMark`].
pub(super) const BACKUP_SCHEDULE_MARKS: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("_system.backup_schedule_marks");

/// Every scheduled minute at or below `through_minute` is settled for one
/// schedule incarnation: its backup completed, or it lies before the
/// schedule was armed.
#[derive(Debug, Clone, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredBackupScheduleMark {
    /// The job name, `backup:<database>:<target>`.
    pub job: String,
    /// The config incarnation of the schedule.
    pub incarnation: u64,
    /// Unix minute. Scheduled minutes at or below it never run again.
    pub through_minute: u64,
}

impl StoredBackupScheduleMark {
    fn key(job: &str, incarnation: u64) -> String {
        format!("{incarnation:016x}:{job}")
    }
}

impl SystemCatalog {
    /// Write `mark` unless the stored mark of its incarnation already reaches
    /// as far. A replayed or late mark never moves a mark back. Returns
    /// whether the row changed.
    pub fn raise_backup_schedule_mark(
        &self,
        mark: &StoredBackupScheduleMark,
    ) -> crate::Result<bool> {
        let key = StoredBackupScheduleMark::key(&mark.job, mark.incarnation);
        let bytes = zerompk::to_msgpack_vec(mark)
            .map_err(|e| catalog_err("encode backup schedule mark", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("backup schedule mark write txn", e))?;
        let raised = {
            let mut table = txn
                .open_table(BACKUP_SCHEDULE_MARKS)
                .map_err(|e| catalog_err("open backup schedule marks", e))?;
            let current = match table
                .get(key.as_str())
                .map_err(|e| catalog_err("read backup schedule mark", e))?
            {
                Some(value) => Some(
                    zerompk::from_msgpack::<StoredBackupScheduleMark>(value.value())
                        .map_err(|e| catalog_err("decode backup schedule mark", e))?,
                ),
                None => None,
            };
            if current.is_some_and(|current| current.through_minute >= mark.through_minute) {
                false
            } else {
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert backup schedule mark", e))?;
                true
            }
        };
        txn.commit()
            .map_err(|e| catalog_err("backup schedule mark commit", e))?;
        Ok(raised)
    }

    /// The mark of `job` at `incarnation`, if one was written.
    pub fn backup_schedule_mark(
        &self,
        job: &str,
        incarnation: u64,
    ) -> crate::Result<Option<StoredBackupScheduleMark>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("backup schedule mark read txn", e))?;
        let table = match txn.open_table(BACKUP_SCHEDULE_MARKS) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(catalog_err("open backup schedule marks", e)),
        };
        let key = StoredBackupScheduleMark::key(job, incarnation);
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("read backup schedule mark", e))?
        {
            Some(value) => zerompk::from_msgpack(value.value())
                .map(Some)
                .map_err(|e| catalog_err("decode backup schedule mark", e)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mark(incarnation: u64, through_minute: u64) -> StoredBackupScheduleMark {
        StoredBackupScheduleMark {
            job: "backup:sales:s3://b/p".into(),
            incarnation,
            through_minute,
        }
    }

    #[test]
    fn a_mark_only_moves_forward_within_its_incarnation() {
        let catalog = SystemCatalog::open_in_memory().unwrap();
        let job = "backup:sales:s3://b/p";
        assert_eq!(catalog.backup_schedule_mark(job, 7).unwrap(), None);
        assert!(catalog.raise_backup_schedule_mark(&mark(7, 100)).unwrap());
        assert!(!catalog.raise_backup_schedule_mark(&mark(7, 90)).unwrap());
        assert!(!catalog.raise_backup_schedule_mark(&mark(7, 100)).unwrap());
        assert_eq!(
            catalog.backup_schedule_mark(job, 7).unwrap(),
            Some(mark(7, 100))
        );
        assert!(catalog.raise_backup_schedule_mark(&mark(7, 101)).unwrap());

        // Another incarnation has its own mark.
        assert_eq!(catalog.backup_schedule_mark(job, 8).unwrap(), None);
        assert!(catalog.raise_backup_schedule_mark(&mark(8, 5)).unwrap());
        assert_eq!(
            catalog
                .backup_schedule_mark(job, 7)
                .unwrap()
                .map(|m| m.through_minute),
            Some(101)
        );
    }
}
