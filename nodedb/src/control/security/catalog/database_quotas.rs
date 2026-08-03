// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for per-database resource quotas.
//!
//! Quotas are stored in `_system.database_quotas` (keyed by `DatabaseId`).
//! At write time, the sum of all database quotas is compared against a
//! cluster-wide global ceiling when one is configured; if no ceiling has been
//! set, the check is skipped — an unconfigured ceiling means the operator
//! accepts the default of "no global limit".

use nodedb_types::{DatabaseId, QuotaRecord};
use redb::{ReadableDatabase, ReadableTable};

use super::types::{DATABASE_QUOTAS, SystemCatalog, catalog_err};

/// Optional global resource ceiling against which the sum of all database
/// quotas is validated at write time.
///
/// All fields default to `0`, which means "no ceiling configured" and
/// causes the corresponding check to be skipped.
#[derive(Debug, Clone, Default)]
pub struct GlobalQuotaCeiling {
    /// Maximum total `max_memory_bytes` across all databases. 0 = unlimited.
    pub max_memory_bytes: u64,
    /// Maximum total `max_storage_bytes` across all databases. 0 = unlimited.
    pub max_storage_bytes: u64,
    /// Maximum total `max_qps` across all databases. 0 = unlimited.
    pub max_qps: u64,
    /// Maximum total `max_connections` across all databases. 0 = unlimited.
    pub max_connections: u64,
}

impl SystemCatalog {
    // ── database_quotas ───────────────────────────────────────────────────────

    /// Retrieve the quota record for a database. Returns `None` if no explicit
    /// quota has been configured (callers should fall back to `QuotaRecord::DEFAULT`).
    pub fn get_database_quota(&self, db_id: DatabaseId) -> crate::Result<Option<QuotaRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("database_quotas read txn", e))?;
        let table = txn
            .open_table(DATABASE_QUOTAS)
            .map_err(|e| catalog_err("open database_quotas", e))?;
        match table
            .get(db_id.as_u64())
            .map_err(|e| catalog_err("get database_quotas", e))?
        {
            Some(v) => {
                let record: QuotaRecord = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser QuotaRecord", e))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Write a quota record for a database.
    ///
    /// If `ceiling` contains non-zero values, the sum of all existing database
    /// quotas (including this one) is checked against each ceiling dimension.
    /// Returns `NodeDbError` with code `QUOTA_OVERCOMMIT` on violation.
    pub fn put_database_quota(
        &self,
        db_id: DatabaseId,
        record: &QuotaRecord,
        ceiling: &GlobalQuotaCeiling,
    ) -> crate::Result<()> {
        // Validate the record itself.
        record.validate().map_err(|e| crate::Error::BadRequest {
            detail: e.to_string(),
        })?;

        // Check sum-of-all-database-quotas ≤ global ceiling, if configured.
        if ceiling.max_memory_bytes > 0
            || ceiling.max_storage_bytes > 0
            || ceiling.max_qps > 0
            || ceiling.max_connections > 0
        {
            self.check_database_quota_ceiling(db_id, record, ceiling)?;
        }

        let bytes =
            zerompk::to_msgpack_vec(record).map_err(|e| catalog_err("serialize QuotaRecord", e))?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("database_quotas write txn", e))?;
        {
            let mut table = txn
                .open_table(DATABASE_QUOTAS)
                .map_err(|e| catalog_err("open database_quotas write", e))?;
            table
                .insert(db_id.as_u64(), bytes.as_slice())
                .map_err(|e| catalog_err("insert database_quotas", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("database_quotas commit", e))
    }

    /// Remove the quota record for a database. Idempotent.
    pub fn delete_database_quota(&self, db_id: DatabaseId) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("database_quotas delete txn", e))?;
        {
            let mut table = txn
                .open_table(DATABASE_QUOTAS)
                .map_err(|e| catalog_err("open database_quotas delete", e))?;
            table
                .remove(db_id.as_u64())
                .map_err(|e| catalog_err("remove database_quotas", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("database_quotas delete commit", e))
    }

    /// List all database quota records.
    pub fn list_database_quotas(&self) -> crate::Result<Vec<(DatabaseId, QuotaRecord)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("list_database_quotas read txn", e))?;
        let table = txn
            .open_table(DATABASE_QUOTAS)
            .map_err(|e| catalog_err("open database_quotas list", e))?;
        let mut out = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| catalog_err("iter database_quotas", e))?;
        for row in iter {
            let (k, v) = row.map_err(|e| catalog_err("iter database_quotas row", e))?;
            let record: QuotaRecord = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("deser list_database_quotas row", e))?;
            out.push((DatabaseId::new(k.value()), record));
        }
        Ok(out)
    }

    // ── sum-of-quotas validation ──────────────────────────────────────────────

    /// Validate that the proposed quota for `db_id` does not push the cluster-wide
    /// sum past any non-zero ceiling dimension.
    fn check_database_quota_ceiling(
        &self,
        db_id: DatabaseId,
        proposed: &QuotaRecord,
        ceiling: &GlobalQuotaCeiling,
    ) -> crate::Result<()> {
        let all = self.list_database_quotas()?;

        // Sum existing records, excluding the row being overwritten (if any).
        let mut sum_memory: u64 = 0;
        let mut sum_storage: u64 = 0;
        let mut sum_qps: u64 = 0;
        let mut sum_connections: u64 = 0;

        for (id, rec) in &all {
            if *id == db_id {
                continue; // Will be replaced by `proposed`.
            }
            sum_memory = sum_memory.saturating_add(rec.max_memory_bytes);
            sum_storage = sum_storage.saturating_add(rec.max_storage_bytes);
            sum_qps = sum_qps.saturating_add(rec.max_qps as u64);
            sum_connections = sum_connections.saturating_add(rec.max_connections as u64);
        }

        // Add the proposed values.
        sum_memory = sum_memory.saturating_add(proposed.max_memory_bytes);
        sum_storage = sum_storage.saturating_add(proposed.max_storage_bytes);
        sum_qps = sum_qps.saturating_add(proposed.max_qps as u64);
        sum_connections = sum_connections.saturating_add(proposed.max_connections as u64);

        if ceiling.max_memory_bytes > 0 && sum_memory > ceiling.max_memory_bytes {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_memory_bytes".into(),
                detail: format!(
                    "total {sum_memory} exceeds global ceiling {}",
                    ceiling.max_memory_bytes
                ),
            });
        }
        if ceiling.max_storage_bytes > 0 && sum_storage > ceiling.max_storage_bytes {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_storage_bytes".into(),
                detail: format!(
                    "total {sum_storage} exceeds global ceiling {}",
                    ceiling.max_storage_bytes
                ),
            });
        }
        if ceiling.max_qps > 0 && sum_qps > ceiling.max_qps {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_qps".into(),
                detail: format!("total {sum_qps} exceeds global ceiling {}", ceiling.max_qps),
            });
        }
        if ceiling.max_connections > 0 && sum_connections > ceiling.max_connections {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_connections".into(),
                detail: format!(
                    "total {sum_connections} exceeds global ceiling {}",
                    ceiling.max_connections
                ),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::PriorityClass;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn no_ceiling() -> GlobalQuotaCeiling {
        GlobalQuotaCeiling::default()
    }

    fn sample_record() -> QuotaRecord {
        QuotaRecord {
            max_memory_bytes: 1073741824,
            max_storage_bytes: 10737418240,
            max_qps: 1000,
            max_connections: 100,
            cache_weight: 2,
            priority_class: PriorityClass::Standard,
            maintenance_cpu_pct: 25,
        }
    }

    #[test]
    fn get_missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert!(
            cat.get_database_quota(DatabaseId::DEFAULT)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn put_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        let r = sample_record();
        cat.put_database_quota(DatabaseId::DEFAULT, &r, &no_ceiling())
            .unwrap();
        let got = cat
            .get_database_quota(DatabaseId::DEFAULT)
            .unwrap()
            .unwrap();
        assert_eq!(got, r);
    }

    #[test]
    fn delete_is_idempotent() {
        let (_dir, cat) = open_catalog();
        cat.put_database_quota(DatabaseId::DEFAULT, &sample_record(), &no_ceiling())
            .unwrap();
        cat.delete_database_quota(DatabaseId::DEFAULT).unwrap();
        assert!(
            cat.get_database_quota(DatabaseId::DEFAULT)
                .unwrap()
                .is_none()
        );
        cat.delete_database_quota(DatabaseId::DEFAULT).unwrap(); // second delete is no-op
    }

    #[test]
    fn ceiling_overcommit_rejected() {
        let (_dir, cat) = open_catalog();
        let ceiling = GlobalQuotaCeiling {
            max_memory_bytes: 2_000_000_000,
            ..Default::default()
        };
        let r1 = QuotaRecord {
            max_memory_bytes: 1_500_000_000,
            ..QuotaRecord::DEFAULT
        };
        cat.put_database_quota(DatabaseId::new(1), &r1, &ceiling)
            .unwrap();

        // Second database would push total to 3 GB, exceeding 2 GB ceiling.
        let r2 = QuotaRecord {
            max_memory_bytes: 1_500_000_000,
            ..QuotaRecord::DEFAULT
        };
        let err = cat
            .put_database_quota(DatabaseId::new(2), &r2, &ceiling)
            .unwrap_err();
        assert!(matches!(err, crate::Error::QuotaOvercommit { .. }));
    }

    #[test]
    fn update_existing_does_not_double_count() {
        let (_dir, cat) = open_catalog();
        let ceiling = GlobalQuotaCeiling {
            max_memory_bytes: 2_000_000_000,
            ..Default::default()
        };
        let r = QuotaRecord {
            max_memory_bytes: 1_500_000_000,
            ..QuotaRecord::DEFAULT
        };
        cat.put_database_quota(DatabaseId::new(1), &r, &ceiling)
            .unwrap();
        // Updating the same database to 1.8 GB should succeed (replaces, not adds).
        let r2 = QuotaRecord {
            max_memory_bytes: 1_800_000_000,
            ..QuotaRecord::DEFAULT
        };
        cat.put_database_quota(DatabaseId::new(1), &r2, &ceiling)
            .unwrap();
    }
}
