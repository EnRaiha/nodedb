// SPDX-License-Identifier: BUSL-1.1

//! Catalog persistence for per-tenant resource quotas within a database.
//!
//! Quotas are stored in `_system.tenant_quotas` keyed by `(DatabaseId, TenantId)`.
//! At write time the sum of all tenant quotas within the database is checked
//! against that database's quota ceiling; if no database quota is configured,
//! the check is skipped.

use nodedb_types::{DatabaseId, QuotaRecord, TenantId};
use redb::{ReadableDatabase, ReadableTable};

use super::database_quotas::GlobalQuotaCeiling;
use super::types::{SystemCatalog, TENANT_QUOTAS, catalog_err};

impl SystemCatalog {
    // ── tenant_quotas ─────────────────────────────────────────────────────────

    /// Retrieve the quota record for a specific tenant in a database.
    /// Returns `None` if no explicit quota has been configured.
    pub fn get_tenant_quota(
        &self,
        db_id: DatabaseId,
        tenant_id: TenantId,
    ) -> crate::Result<Option<QuotaRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("tenant_quotas read txn", e))?;
        let table = txn
            .open_table(TENANT_QUOTAS)
            .map_err(|e| catalog_err("open tenant_quotas", e))?;
        let key = (db_id.as_u64(), tenant_id.as_u64());
        match table
            .get(key)
            .map_err(|e| catalog_err("get tenant_quotas", e))?
        {
            Some(v) => {
                let record: QuotaRecord = zerompk::from_msgpack(v.value())
                    .map_err(|e| catalog_err("deser tenant QuotaRecord", e))?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Write a quota record for a tenant within a database.
    ///
    /// Validates the record and checks that the sum of all tenant quotas in
    /// the database (including this one) does not exceed the database's own
    /// quota on any non-zero dimension. Returns `crate::Error::QuotaOvercommit`
    /// on violation.
    pub fn put_tenant_quota(
        &self,
        db_id: DatabaseId,
        tenant_id: TenantId,
        record: &QuotaRecord,
    ) -> crate::Result<()> {
        // Validate the record itself.
        record.validate().map_err(|e| crate::Error::BadRequest {
            detail: e.to_string(),
        })?;

        // Check sum-of-tenant-quotas ≤ database quota.
        self.check_tenant_quota_ceiling(db_id, tenant_id, record)?;

        let bytes = zerompk::to_msgpack_vec(record)
            .map_err(|e| catalog_err("serialize tenant QuotaRecord", e))?;
        let key = (db_id.as_u64(), tenant_id.as_u64());
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("tenant_quotas write txn", e))?;
        {
            let mut table = txn
                .open_table(TENANT_QUOTAS)
                .map_err(|e| catalog_err("open tenant_quotas write", e))?;
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| catalog_err("insert tenant_quotas", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("tenant_quotas commit", e))
    }

    /// Remove the quota record for a tenant within a database. Idempotent.
    pub fn delete_tenant_quota(&self, db_id: DatabaseId, tenant_id: TenantId) -> crate::Result<()> {
        let key = (db_id.as_u64(), tenant_id.as_u64());
        let txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("tenant_quotas delete txn", e))?;
        {
            let mut table = txn
                .open_table(TENANT_QUOTAS)
                .map_err(|e| catalog_err("open tenant_quotas delete", e))?;
            table
                .remove(key)
                .map_err(|e| catalog_err("remove tenant_quotas", e))?;
        }
        txn.commit()
            .map_err(|e| catalog_err("tenant_quotas delete commit", e))
    }

    /// List all tenant quota records for a specific database.
    pub fn list_tenant_quotas_for_database(
        &self,
        db_id: DatabaseId,
    ) -> crate::Result<Vec<(TenantId, QuotaRecord)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("list_tenant_quotas read txn", e))?;
        let table = txn
            .open_table(TENANT_QUOTAS)
            .map_err(|e| catalog_err("open tenant_quotas list", e))?;

        let low = (db_id.as_u64(), 0u64);
        let high = (db_id.as_u64(), u64::MAX);
        let range = table
            .range(low..=high)
            .map_err(|e| catalog_err("range tenant_quotas", e))?;

        let mut out = Vec::new();
        for row in range {
            let (k, v) = row.map_err(|e| catalog_err("iter tenant_quotas row", e))?;
            let (_, tid) = k.value();
            let record: QuotaRecord = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("deser list_tenant_quotas row", e))?;
            out.push((TenantId::new(tid), record));
        }
        Ok(out)
    }

    /// List all tenant quota records across all databases.
    pub fn list_all_tenant_quotas(
        &self,
    ) -> crate::Result<Vec<(DatabaseId, TenantId, QuotaRecord)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("list_all_tenant_quotas read txn", e))?;
        let table = txn
            .open_table(TENANT_QUOTAS)
            .map_err(|e| catalog_err("open tenant_quotas list_all", e))?;
        let iter = table
            .iter()
            .map_err(|e| catalog_err("iter tenant_quotas all", e))?;
        let mut out = Vec::new();
        for row in iter {
            let (k, v) = row.map_err(|e| catalog_err("iter tenant_quotas all row", e))?;
            let (db, tid) = k.value();
            let record: QuotaRecord = zerompk::from_msgpack(v.value())
                .map_err(|e| catalog_err("deser list_all_tenant_quotas row", e))?;
            out.push((DatabaseId::new(db), TenantId::new(tid), record));
        }
        Ok(out)
    }

    // ── sum-of-tenant-quotas validation ──────────────────────────────────────

    fn check_tenant_quota_ceiling(
        &self,
        db_id: DatabaseId,
        tenant_id: TenantId,
        proposed: &QuotaRecord,
    ) -> crate::Result<()> {
        // Load the database's own quota. If none is set, no ceiling to check.
        let db_quota = match self.get_database_quota(db_id)? {
            Some(q) => q,
            None => return Ok(()),
        };

        // Build a GlobalQuotaCeiling from the database quota's non-zero limits.
        let ceiling = GlobalQuotaCeiling {
            max_memory_bytes: db_quota.max_memory_bytes,
            max_storage_bytes: db_quota.max_storage_bytes,
            max_qps: db_quota.max_qps as u64,
            max_connections: db_quota.max_connections as u64,
        };

        // If all dimensions are zero, the database quota imposes no limits.
        if ceiling.max_memory_bytes == 0
            && ceiling.max_storage_bytes == 0
            && ceiling.max_qps == 0
            && ceiling.max_connections == 0
        {
            return Ok(());
        }

        let tenants = self.list_tenant_quotas_for_database(db_id)?;

        let mut sum_memory: u64 = 0;
        let mut sum_storage: u64 = 0;
        let mut sum_qps: u64 = 0;
        let mut sum_connections: u64 = 0;

        for (tid, rec) in &tenants {
            if *tid == tenant_id {
                continue; // Will be replaced by `proposed`.
            }
            sum_memory = sum_memory.saturating_add(rec.max_memory_bytes);
            sum_storage = sum_storage.saturating_add(rec.max_storage_bytes);
            sum_qps = sum_qps.saturating_add(rec.max_qps as u64);
            sum_connections = sum_connections.saturating_add(rec.max_connections as u64);
        }

        sum_memory = sum_memory.saturating_add(proposed.max_memory_bytes);
        sum_storage = sum_storage.saturating_add(proposed.max_storage_bytes);
        sum_qps = sum_qps.saturating_add(proposed.max_qps as u64);
        sum_connections = sum_connections.saturating_add(proposed.max_connections as u64);

        if ceiling.max_memory_bytes > 0 && sum_memory > ceiling.max_memory_bytes {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_memory_bytes".into(),
                detail: format!(
                    "tenant sum {sum_memory} exceeds database quota {}",
                    ceiling.max_memory_bytes
                ),
            });
        }
        if ceiling.max_storage_bytes > 0 && sum_storage > ceiling.max_storage_bytes {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_storage_bytes".into(),
                detail: format!(
                    "tenant sum {sum_storage} exceeds database quota {}",
                    ceiling.max_storage_bytes
                ),
            });
        }
        if ceiling.max_qps > 0 && sum_qps > ceiling.max_qps {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_qps".into(),
                detail: format!(
                    "tenant sum {sum_qps} exceeds database quota {}",
                    ceiling.max_qps
                ),
            });
        }
        if ceiling.max_connections > 0 && sum_connections > ceiling.max_connections {
            return Err(crate::Error::QuotaOvercommit {
                field: "max_connections".into(),
                detail: format!(
                    "tenant sum {sum_connections} exceeds database quota {}",
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

    use crate::control::security::catalog::database_quotas::GlobalQuotaCeiling;

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let cat = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, cat)
    }

    fn sample_record() -> QuotaRecord {
        QuotaRecord {
            max_memory_bytes: 536870912,
            max_storage_bytes: 1073741824,
            max_qps: 500,
            max_connections: 50,
            cache_weight: 1,
            priority_class: PriorityClass::Standard,
            maintenance_cpu_pct: 25,
        }
    }

    #[test]
    fn get_missing_returns_none() {
        let (_dir, cat) = open_catalog();
        assert!(
            cat.get_tenant_quota(DatabaseId::DEFAULT, TenantId::new(1))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn put_get_roundtrip() {
        let (_dir, cat) = open_catalog();
        let r = sample_record();
        cat.put_tenant_quota(DatabaseId::DEFAULT, TenantId::new(1), &r)
            .unwrap();
        let got = cat
            .get_tenant_quota(DatabaseId::DEFAULT, TenantId::new(1))
            .unwrap()
            .unwrap();
        assert_eq!(got, r);
    }

    #[test]
    fn list_for_database_scoped() {
        let (_dir, cat) = open_catalog();
        let r = sample_record();
        cat.put_tenant_quota(DatabaseId::new(1), TenantId::new(10), &r)
            .unwrap();
        cat.put_tenant_quota(DatabaseId::new(2), TenantId::new(20), &r)
            .unwrap();

        let list1 = cat
            .list_tenant_quotas_for_database(DatabaseId::new(1))
            .unwrap();
        assert_eq!(list1.len(), 1);
        assert_eq!(list1[0].0, TenantId::new(10));

        let list2 = cat
            .list_tenant_quotas_for_database(DatabaseId::new(2))
            .unwrap();
        assert_eq!(list2.len(), 1);
        assert_eq!(list2[0].0, TenantId::new(20));
    }

    #[test]
    fn tenant_overcommit_rejected_when_db_quota_set() {
        let (_dir, cat) = open_catalog();

        // Set a database quota of 1 GB memory.
        let db_quota = QuotaRecord {
            max_memory_bytes: 1_000_000_000,
            ..QuotaRecord::DEFAULT
        };
        cat.put_database_quota(
            DatabaseId::new(1),
            &db_quota,
            &GlobalQuotaCeiling::default(),
        )
        .unwrap();

        // First tenant gets 700 MB.
        let t1 = QuotaRecord {
            max_memory_bytes: 700_000_000,
            ..QuotaRecord::DEFAULT
        };
        cat.put_tenant_quota(DatabaseId::new(1), TenantId::new(1), &t1)
            .unwrap();

        // Second tenant tries 400 MB → total 1.1 GB > 1 GB.
        let t2 = QuotaRecord {
            max_memory_bytes: 400_000_000,
            ..QuotaRecord::DEFAULT
        };
        let err = cat
            .put_tenant_quota(DatabaseId::new(1), TenantId::new(2), &t2)
            .unwrap_err();
        assert!(matches!(err, crate::Error::QuotaOvercommit { .. }));
    }

    #[test]
    fn no_db_quota_skips_check() {
        let (_dir, cat) = open_catalog();
        // No database quota set → no ceiling → any tenant quota is accepted.
        let t = QuotaRecord {
            max_memory_bytes: u64::MAX,
            ..QuotaRecord::DEFAULT
        };
        cat.put_tenant_quota(DatabaseId::new(1), TenantId::new(1), &t)
            .unwrap();
    }
}
