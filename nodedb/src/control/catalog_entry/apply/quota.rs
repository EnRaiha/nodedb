// SPDX-License-Identifier: BUSL-1.1

//! Apply resource-quota catalog entries to `SystemCatalog` redb.
//!
//! Every function writes the raw record consensus already accepted. Record
//! validation and ceiling checks belong to the leader, before the propose.

use nodedb_types::{DatabaseId, QuotaRecord, TenantId};

use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::diag::{DATABASE_SCOPE, TENANT_SCOPE};

/// Apply a `PutDatabaseQuota` entry.
pub fn put_database(
    db_id: u64,
    record: &QuotaRecord,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .write_database_quota(DatabaseId::new(db_id), record)
        .map_err(|e| {
            crate::diag::quota_row_write_failed(&e, "write_database_quota", db_id, None);
            catalog_err(&format!("write_database_quota (database {db_id})"), e)
        })
}

/// Apply a `DeleteDatabaseQuota` entry.
pub fn delete_database(db_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_database_quota(DatabaseId::new(db_id))
        .map_err(|e| {
            crate::diag::quota_row_write_failed(&e, "delete_database_quota", db_id, None);
            catalog_err(&format!("delete_database_quota (database {db_id})"), e)
        })
}

/// Apply a `PutTenantQuota` entry.
pub fn put_tenant(
    db_id: u64,
    tenant_id: u64,
    record: &QuotaRecord,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .write_tenant_quota(DatabaseId::new(db_id), TenantId::new(tenant_id), record)
        .map_err(|e| {
            crate::diag::quota_row_write_failed(&e, "write_tenant_quota", db_id, Some(tenant_id));
            catalog_err(
                &format!("write_tenant_quota (database {db_id}, tenant {tenant_id})"),
                e,
            )
        })
}

/// Apply a `DeleteTenantQuota` entry.
pub fn delete_tenant(db_id: u64, tenant_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_tenant_quota(DatabaseId::new(db_id), TenantId::new(tenant_id))
        .map_err(|e| {
            crate::diag::quota_row_write_failed(&e, "delete_tenant_quota", db_id, Some(tenant_id));
            catalog_err(
                &format!("delete_tenant_quota (database {db_id}, tenant {tenant_id})"),
                e,
            )
        })
}

/// Delete the quota rows of a dropped database, its tenant rows included.
pub fn purge_database_scope(db_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    let rows = catalog
        .list_tenant_quotas_for_database(DatabaseId::new(db_id))
        .map_err(|e| {
            crate::diag::quota_scope_purge_incomplete(&e, DATABASE_SCOPE, Some(db_id), None);
            catalog_err(&format!("tenant quota scan (database {db_id})"), e)
        })?;
    for (tenant_id, _) in rows {
        delete_tenant(db_id, tenant_id.as_u64(), catalog)?;
    }
    delete_database(db_id, catalog)
}

/// Delete a dropped tenant's quota rows in every database.
pub fn purge_tenant_scope(tenant_id: u64, catalog: &SystemCatalog) -> crate::Result<()> {
    let rows = catalog.list_all_tenant_quotas().map_err(|e| {
        crate::diag::quota_scope_purge_incomplete(&e, TENANT_SCOPE, None, Some(tenant_id));
        catalog_err(&format!("tenant quota scan (tenant {tenant_id})"), e)
    })?;
    for (db_id, row_tenant, _) in rows {
        if row_tenant.as_u64() == tenant_id {
            delete_tenant(db_id.as_u64(), tenant_id, catalog)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{decode, encode};

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample_record() -> QuotaRecord {
        QuotaRecord {
            max_memory_bytes: 1_073_741_824,
            max_storage_bytes: 10_737_418_240,
            max_qps: 1000,
            max_connections: 100,
            cache_weight: 2,
            priority_class: nodedb_types::PriorityClass::Standard,
            maintenance_cpu_pct: 40,
        }
    }

    #[test]
    fn put_database_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::PutDatabaseQuota {
            db_id: 7,
            record: Box::new(sample_record()),
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutDatabaseQuota { db_id, record } => {
                assert_eq!(db_id, 7);
                assert_eq!(*record, sample_record());
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_database_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::DeleteDatabaseQuota { db_id: 7 };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        assert!(matches!(
            decoded,
            CatalogEntry::DeleteDatabaseQuota { db_id: 7 }
        ));
    }

    #[test]
    fn put_tenant_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::PutTenantQuota {
            db_id: 3,
            tenant_id: 9,
            record: Box::new(sample_record()),
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutTenantQuota {
                db_id,
                tenant_id,
                record,
            } => {
                assert_eq!((db_id, tenant_id), (3, 9));
                assert_eq!(*record, sample_record());
            }
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_tenant_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::DeleteTenantQuota {
            db_id: 3,
            tenant_id: 9,
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        assert!(matches!(
            decoded,
            CatalogEntry::DeleteTenantQuota {
                db_id: 3,
                tenant_id: 9
            }
        ));
    }

    #[test]
    fn apply_writes_database_quota_row() {
        let (_dir, catalog) = open_catalog();
        let entry = CatalogEntry::PutDatabaseQuota {
            db_id: 5,
            record: Box::new(sample_record()),
        };
        crate::control::catalog_entry::apply::apply_to(&entry, &catalog).unwrap();
        let stored = catalog
            .get_database_quota(DatabaseId::new(5))
            .unwrap()
            .unwrap();
        assert_eq!(stored, sample_record());

        crate::control::catalog_entry::apply::apply_to(
            &CatalogEntry::DeleteDatabaseQuota { db_id: 5 },
            &catalog,
        )
        .unwrap();
        assert!(
            catalog
                .get_database_quota(DatabaseId::new(5))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn apply_writes_tenant_quota_row() {
        let (_dir, catalog) = open_catalog();
        let entry = CatalogEntry::PutTenantQuota {
            db_id: 5,
            tenant_id: 11,
            record: Box::new(sample_record()),
        };
        crate::control::catalog_entry::apply::apply_to(&entry, &catalog).unwrap();
        let stored = catalog
            .get_tenant_quota(DatabaseId::new(5), TenantId::new(11))
            .unwrap()
            .unwrap();
        assert_eq!(stored, sample_record());

        crate::control::catalog_entry::apply::apply_to(
            &CatalogEntry::DeleteTenantQuota {
                db_id: 5,
                tenant_id: 11,
            },
            &catalog,
        )
        .unwrap();
        assert!(
            catalog
                .get_tenant_quota(DatabaseId::new(5), TenantId::new(11))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn purge_database_scope_removes_the_database_and_tenant_rows() {
        let (_dir, catalog) = open_catalog();
        let db = DatabaseId::new(9);
        let other = DatabaseId::new(10);
        catalog.write_database_quota(db, &sample_record()).unwrap();
        catalog
            .write_tenant_quota(db, TenantId::new(1), &sample_record())
            .unwrap();
        catalog
            .write_tenant_quota(other, TenantId::new(1), &sample_record())
            .unwrap();

        purge_database_scope(db.as_u64(), &catalog).expect("purge database scope");

        assert!(catalog.get_database_quota(db).unwrap().is_none());
        assert!(
            catalog
                .get_tenant_quota(db, TenantId::new(1))
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get_tenant_quota(other, TenantId::new(1))
                .unwrap()
                .is_some(),
            "another database keeps its tenant quota row"
        );
    }

    #[test]
    fn purge_database_scope_without_rows_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        let db = DatabaseId::new(12);

        purge_database_scope(db.as_u64(), &catalog).expect("purge database scope");

        assert!(catalog.get_database_quota(db).unwrap().is_none());
    }

    #[test]
    fn purge_tenant_scope_removes_that_tenant_in_every_database() {
        let (_dir, catalog) = open_catalog();
        let first = DatabaseId::new(1);
        let second = DatabaseId::new(2);
        let dropped = TenantId::new(7);
        let kept = TenantId::new(8);
        catalog
            .write_tenant_quota(first, dropped, &sample_record())
            .unwrap();
        catalog
            .write_tenant_quota(second, dropped, &sample_record())
            .unwrap();
        catalog
            .write_tenant_quota(first, kept, &sample_record())
            .unwrap();

        purge_tenant_scope(dropped.as_u64(), &catalog).expect("purge tenant scope");

        assert!(catalog.get_tenant_quota(first, dropped).unwrap().is_none());
        assert!(catalog.get_tenant_quota(second, dropped).unwrap().is_none());
        assert!(catalog.get_tenant_quota(first, kept).unwrap().is_some());
    }

    #[test]
    fn delete_database_entry_purges_the_quota_rows() {
        let (_dir, catalog) = open_catalog();
        let db = DatabaseId::new(4);
        catalog.write_database_quota(db, &sample_record()).unwrap();
        catalog
            .write_tenant_quota(db, TenantId::new(2), &sample_record())
            .unwrap();

        crate::control::catalog_entry::apply::apply_to(
            &CatalogEntry::DeleteDatabase { db_id: db.as_u64() },
            &catalog,
        )
        .unwrap();

        assert!(catalog.get_database_quota(db).unwrap().is_none());
        assert!(
            catalog
                .get_tenant_quota(db, TenantId::new(2))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn delete_tenant_entry_purges_the_quota_rows() {
        let (_dir, catalog) = open_catalog();
        let db = DatabaseId::new(4);
        let tenant = TenantId::new(2);
        catalog
            .write_tenant_quota(db, tenant, &sample_record())
            .unwrap();

        crate::control::catalog_entry::apply::apply_to(
            &CatalogEntry::DeleteTenant {
                tenant_id: tenant.as_u64(),
            },
            &catalog,
        )
        .unwrap();

        assert!(catalog.get_tenant_quota(db, tenant).unwrap().is_none());
    }

    #[test]
    fn apply_skips_validation_the_leader_already_ran() {
        let (_dir, catalog) = open_catalog();
        // cache_weight = 0 fails `QuotaRecord::validate`; apply must still write.
        let record = QuotaRecord {
            cache_weight: 0,
            ..sample_record()
        };
        put_database(6, &record, &catalog).expect("apply put_database quota");
        assert_eq!(
            catalog
                .get_database_quota(DatabaseId::new(6))
                .unwrap()
                .unwrap(),
            record
        );
    }
}
