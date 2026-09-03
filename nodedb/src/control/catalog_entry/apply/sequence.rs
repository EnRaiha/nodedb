// SPDX-License-Identifier: BUSL-1.1

//! Apply Sequence catalog entries to `SystemCatalog` redb.

use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};
use crate::control::security::catalog::{SystemCatalog, catalog_err};

pub fn put(stored: &StoredSequence, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_sequence(stored).map_err(|e| {
        catalog_err(
            &format!(
                "put_sequence '{}' (database {}, tenant {})",
                stored.name, stored.database_id, stored.tenant_id
            ),
            e,
        )
    })?;
    super::owner::put_parent_owner(
        object_type::SEQUENCE,
        stored.database_id,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    )
}

pub fn delete(
    database_id: u64,
    tenant_id: u64,
    name: &str,
    catalog: &SystemCatalog,
) -> crate::Result<()> {
    catalog
        .delete_sequence(database_id, tenant_id, name)
        .map_err(|e| {
            catalog_err(
                &format!("delete_sequence '{name}' (database {database_id}, tenant {tenant_id})"),
                e,
            )
        })?;
    super::owner::delete_parent_owner(object_type::SEQUENCE, database_id, tenant_id, name, catalog)
}

/// Persist the durable counter state. A lost write rewinds the sequence and
/// reissues identifiers already handed out, so the error propagates.
pub fn put_state(state: &SequenceState, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_sequence_state(state).map_err(|e| {
        catalog_err(
            &format!(
                "put_sequence_state '{}' (database {}, tenant {})",
                state.name, state.database_id, state.tenant_id
            ),
            e,
        )
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::control::catalog_entry::apply::apply_to;
    use crate::control::catalog_entry::codec::{decode, encode};
    use crate::control::catalog_entry::entry::CatalogEntry;
    use crate::control::security::credential::store::CredentialStore;

    /// Shared helper: open a fresh temp-dir-backed credential store
    /// and return it alongside the TempDir (kept alive for the test).
    fn open_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(
            CredentialStore::open(&tmp.path().join("system.redb")).expect("open credential store"),
        );
        (store, tmp)
    }

    #[test]
    fn roundtrip_put_sequence() {
        let seq = StoredSequence::new(3, 1, "counter".into(), "bob".into());
        let entry = CatalogEntry::PutSequence(Box::new(seq));
        let bytes = encode(&entry).unwrap();
        match decode(&bytes).unwrap() {
            CatalogEntry::PutSequence(s) => {
                assert_eq!(s.database_id, 3);
                assert_eq!(s.tenant_id, 1);
                assert_eq!(s.name, "counter");
                assert_eq!(s.owner, "bob");
            }
            other => panic!("expected PutSequence, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_delete_sequence() {
        let entry = CatalogEntry::DeleteSequence {
            database_id: 3,
            tenant_id: 42,
            name: "gone".into(),
        };
        let bytes = encode(&entry).unwrap();
        match decode(&bytes).unwrap() {
            CatalogEntry::DeleteSequence {
                database_id,
                tenant_id,
                name,
            } => {
                assert_eq!(database_id, 3);
                assert_eq!(tenant_id, 42);
                assert_eq!(name, "gone");
            }
            other => panic!("expected DeleteSequence, got {other:?}"),
        }
    }

    #[test]
    fn apply_put_then_delete_sequence() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();

        let seq = StoredSequence::new(3, 1, "orders_id_seq".into(), "alice".into());
        apply_to(&CatalogEntry::PutSequence(Box::new(seq)), catalog).expect("apply put_sequence");

        let loaded = catalog
            .get_sequence(3, 1, "orders_id_seq")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders_id_seq");

        apply_to(
            &CatalogEntry::DeleteSequence {
                database_id: 3,
                tenant_id: 1,
                name: "orders_id_seq".into(),
            },
            catalog,
        )
        .expect("apply delete_sequence");

        assert!(
            catalog
                .get_sequence(3, 1, "orders_id_seq")
                .unwrap()
                .is_none()
        );
    }

    /// Owner rows are keyed by database. Two sequences sharing a name
    /// in different databases own separate rows, and dropping one must
    /// leave the other intact.
    #[test]
    fn an_owner_row_of_one_database_survives_a_drop_in_another() {
        let (credentials, _tmp) = open_catalog();
        let catalog = credentials.catalog();

        for (database_id, owner) in [(1u64, "alice"), (2u64, "bob")] {
            let seq = StoredSequence::new(database_id, 7, "shared_seq".into(), owner.into());
            apply_to(&CatalogEntry::PutSequence(Box::new(seq)), catalog)
                .expect("apply put_sequence");
        }

        let owner_in = |database_id: u64| -> Option<String> {
            catalog
                .load_all_owners()
                .expect("load owners")
                .into_iter()
                .find(|o| {
                    o.object_type == object_type::SEQUENCE
                        && o.database_id == database_id
                        && o.tenant_id == 7
                        && o.object_name == "shared_seq"
                })
                .map(|o| o.owner_username)
        };

        assert_eq!(owner_in(1).as_deref(), Some("alice"));
        assert_eq!(owner_in(2).as_deref(), Some("bob"));

        apply_to(
            &CatalogEntry::DeleteSequence {
                database_id: 1,
                tenant_id: 7,
                name: "shared_seq".into(),
            },
            catalog,
        )
        .expect("apply delete_sequence");

        assert_eq!(
            owner_in(1),
            None,
            "the dropped database's owner row must be gone"
        );
        assert_eq!(
            owner_in(2).as_deref(),
            Some("bob"),
            "another database's owner row must survive"
        );
    }
}
