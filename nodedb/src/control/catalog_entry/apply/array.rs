// SPDX-License-Identifier: BUSL-1.1

//! Apply array catalog entries to `_system.arrays`.

use nodedb_array::types::ArrayId;

use crate::control::array_catalog::{ArrayCatalogEntry, persist};
use crate::control::security::catalog::{SystemCatalog, catalog_err};
use crate::types::{DatabaseId, TenantId};

/// Apply `PutArray`: write the full stored definition.
pub fn put(entry: &ArrayCatalogEntry, catalog: &SystemCatalog) -> crate::Result<()> {
    persist::persist(catalog, entry).map_err(|e| {
        catalog_err(
            &format!(
                "put_array '{}' (database {}, tenant {})",
                entry.name,
                entry.array_id.database_id.as_u64(),
                entry.array_id.tenant_id.as_u64()
            ),
            e,
        )
    })
}

/// Apply `DeleteArray`: remove the row and its surrogate bindings in one
/// transaction. A move carries the bindings to `moved_to` instead.
pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    moved_to: Option<u64>,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    let array_id =
        ArrayId::in_database(TenantId::new(tenant_id), DatabaseId::new(database_id), name);
    let removed = match moved_to {
        Some(to) => persist::move_with_surrogates(catalog, &array_id, DatabaseId::new(to)),
        None => persist::remove_with_surrogates(catalog, &array_id),
    };
    removed.map_err(|e| {
        catalog_err(
            &format!("delete_array '{name}' (database {database_id}, tenant {tenant_id})"),
            e,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::{Hlc, HlcClock};

    use super::*;
    use crate::control::catalog_entry::CatalogEntry;
    use crate::control::catalog_entry::descriptor_stamp::{stamp, stamp_batch};
    use crate::control::catalog_entry::descriptor_validate::{ValidationOutcome, validate};
    use crate::control::security::credential::CredentialStore;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    fn array(name: &str, hlc: Hlc) -> ArrayCatalogEntry {
        ArrayCatalogEntry {
            array_id: ArrayId::in_database(TenantId::new(1), DatabaseId::DEFAULT, name),
            name: name.to_string(),
            schema_msgpack: vec![0x90],
            schema_hash: 7,
            created_at_ms: 0,
            prefix_bits: 8,
            audit_retain_ms: None,
            minimum_audit_retain_ms: None,
            modification_hlc: hlc,
            incarnation: nodedb_types::Hlc::ZERO,
        }
    }

    fn delete_entry(name: &str, target_hlc: Hlc) -> CatalogEntry {
        CatalogEntry::DeleteArray {
            database_id: DatabaseId::DEFAULT.as_u64(),
            tenant_id: 1,
            name: name.to_string(),
            target_hlc,
            moved_to: None,
        }
    }

    /// A `DeleteArray` replayed after a same-name recreate names the prior
    /// incarnation, so it must not remove the recreated array.
    #[test]
    fn replayed_delete_of_a_prior_incarnation_is_already_applied() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        put(&array("grid", Hlc::new(30, 0)), catalog).expect("seed recreated array");

        let replayed = delete_entry("grid", Hlc::new(10, 0));
        assert_eq!(
            validate(&replayed, catalog).expect("validate"),
            ValidationOutcome::AlreadyApplied
        );

        let current = delete_entry("grid", Hlc::new(30, 0));
        assert_eq!(
            validate(&current, catalog).expect("validate"),
            ValidationOutcome::Apply
        );
    }

    /// A `PutArray` replayed from before a drop and recreate must not
    /// overwrite the recreated definition.
    #[test]
    fn replayed_put_of_a_prior_incarnation_is_already_applied() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        put(&array("grid", Hlc::new(30, 0)), catalog).expect("seed recreated array");

        let replayed = CatalogEntry::PutArray(Box::new(array("grid", Hlc::new(10, 0))));
        assert_eq!(
            validate(&replayed, catalog).expect("validate"),
            ValidationOutcome::AlreadyApplied
        );
    }

    /// The proposer freezes a delete's target from the committed row, and a
    /// put orders after the row it replaces.
    #[test]
    fn stamps_target_the_committed_incarnation() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        let clock = HlcClock::new();
        let committed = Hlc::new(u64::MAX / 2, 0);
        put(&array("grid", committed), catalog).expect("seed array");

        let CatalogEntry::DeleteArray { target_hlc, .. } =
            stamp(delete_entry("grid", Hlc::ZERO), &clock, catalog).expect("stamp delete")
        else {
            unreachable!("stamp keeps the variant");
        };
        assert_eq!(target_hlc, committed);

        let CatalogEntry::PutArray(altered) = stamp(
            CatalogEntry::PutArray(Box::new(array("grid", Hlc::ZERO))),
            &clock,
            catalog,
        )
        .expect("stamp put") else {
            unreachable!("stamp keeps the variant");
        };
        assert!(altered.modification_hlc > committed);
    }

    /// `CREATE ARRAY g; DROP ARRAY g` in one batch: the drop targets the
    /// create stamped before it.
    #[test]
    fn batched_delete_targets_the_preceding_put() {
        let (store, _tmp) = make_catalog();
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutArray(Box::new(array("grid", Hlc::ZERO))),
                delete_entry("grid", Hlc::ZERO),
            ],
            &HlcClock::new(),
            store.catalog(),
        )
        .expect("stamp batch");
        let (CatalogEntry::PutArray(created), CatalogEntry::DeleteArray { target_hlc, .. }) =
            (&stamped[0], &stamped[1])
        else {
            unreachable!("stamp keeps the variants");
        };
        assert_eq!(*target_hlc, created.modification_hlc);
    }

    #[test]
    fn delete_removes_the_row() {
        let (store, _tmp) = make_catalog();
        let catalog = store.catalog();
        put(&array("grid", Hlc::new(5, 0)), catalog).expect("seed array");
        delete(DatabaseId::DEFAULT.as_u64(), 1, "grid", None, catalog).expect("delete");
        assert!(
            catalog
                .get_array_in_database(TenantId::new(1), DatabaseId::DEFAULT, "grid")
                .expect("read")
                .is_none()
        );
    }

    /// A moving delete survives the metadata log encoding with its target.
    #[test]
    fn moving_delete_roundtrips_through_the_log_codec() {
        let entry = CatalogEntry::DeleteArray {
            database_id: 3,
            tenant_id: 1,
            name: "grid".to_string(),
            target_hlc: Hlc::new(7, 1),
            moved_to: Some(crate::control::array_catalog::ArrayMove {
                target_db_id: 4,
                mover_tenant_id: 9,
            }),
        };
        let bytes = crate::control::catalog_entry::encode(&entry).expect("encode");
        let CatalogEntry::DeleteArray {
            target_hlc,
            moved_to,
            ..
        } = crate::control::catalog_entry::decode(&bytes).expect("decode")
        else {
            unreachable!("the codec keeps the variant");
        };
        assert_eq!(target_hlc, Hlc::new(7, 1));
        assert_eq!(
            moved_to,
            Some(crate::control::array_catalog::ArrayMove {
                target_db_id: 4,
                mover_tenant_id: 9,
            })
        );
    }
}
