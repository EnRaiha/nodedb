// SPDX-License-Identifier: BUSL-1.1

//! Apply per-scope token quota catalog entries to `SystemCatalog` redb.
//!
//! Writes only. The leader parses the enforcement mode and range-checks the
//! warning threshold before proposing, so apply carries no policy: a
//! rejection here would leave followers without a row the leader accepted.

use crate::control::security::catalog::auth_types::StoredScopeQuota;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutScopeQuota` entry.
pub fn put(stored: &StoredScopeQuota, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .put_scope_quota(stored)
        .map_err(|e| catalog_err(&format!("put_scope_quota '{}'", stored.scope_name), e))
}

/// Apply a `DeleteScopeQuota` entry. A missing row is not an error: the
/// entry is idempotent under replay.
pub fn delete(scope_name: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_scope_quota(scope_name)
        .map_err(|e| catalog_err(&format!("delete_scope_quota '{scope_name}'"), e))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::catalog_entry::{apply, decode, encode};

    fn open_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn sample() -> StoredScopeQuota {
        StoredScopeQuota {
            scope_name: "ops:all".to_string(),
            max_tokens: 1_000_000,
            period_secs: 2_592_000,
            enforcement: "hard".to_string(),
            warning_threshold: 0.75,
        }
    }

    #[test]
    fn put_scope_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::PutScopeQuota(Box::new(sample()));
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::PutScopeQuota(stored) => assert_eq!(*stored, sample()),
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn delete_scope_quota_roundtrips_through_codec() {
        let entry = CatalogEntry::DeleteScopeQuota {
            scope_name: "ops:all".to_string(),
        };
        let decoded = decode(&encode(&entry).unwrap()).unwrap();
        match decoded {
            CatalogEntry::DeleteScopeQuota { scope_name } => assert_eq!(scope_name, "ops:all"),
            other => panic!("unexpected variant: {}", other.kind()),
        }
    }

    #[test]
    fn apply_writes_and_removes_the_scope_quota_row() {
        let (_dir, catalog) = open_catalog();
        apply::apply_to(&CatalogEntry::PutScopeQuota(Box::new(sample())), &catalog).unwrap();
        let stored = catalog.load_all_scope_quotas().unwrap();
        assert_eq!(stored, vec![sample()]);

        apply::apply_to(
            &CatalogEntry::DeleteScopeQuota {
                scope_name: "ops:all".to_string(),
            },
            &catalog,
        )
        .unwrap();
        assert!(catalog.load_all_scope_quotas().unwrap().is_empty());
    }

    #[test]
    fn apply_skips_the_validation_the_leader_already_ran() {
        let (_dir, catalog) = open_catalog();
        // `hrad` fails `QuotaEnforcement::parse`; apply must still write it.
        let record = StoredScopeQuota {
            enforcement: "hrad".to_string(),
            ..sample()
        };
        put(&record, &catalog).expect("apply put_scope_quota");
        assert_eq!(catalog.load_all_scope_quotas().unwrap(), vec![record]);
    }

    #[test]
    fn deleting_an_absent_scope_quota_is_a_noop() {
        let (_dir, catalog) = open_catalog();
        delete("never-defined", &catalog).expect("delete absent scope quota");
    }
}
