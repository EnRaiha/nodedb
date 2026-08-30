// SPDX-License-Identifier: BUSL-1.1

//! Apply Sequence catalog entries to `SystemCatalog` redb.

use tracing::warn;

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::object_type;
use crate::control::security::catalog::sequence_types::{SequenceState, StoredSequence};

pub fn put(stored: &StoredSequence, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_sequence(stored) {
        warn!(
            sequence = %stored.name,
            tenant = stored.tenant_id,
            error = %e,
            "catalog_entry: put_sequence failed"
        );
    }
    super::owner::put_parent_owner(
        object_type::SEQUENCE,
        stored.tenant_id,
        &stored.name,
        &stored.owner,
        catalog,
    );
}

pub fn delete(tenant_id: u64, name: &str, catalog: &SystemCatalog) {
    if let Err(e) = catalog.delete_sequence(tenant_id, name) {
        warn!(
            sequence = %name,
            tenant = tenant_id,
            error = %e,
            "catalog_entry: delete_sequence failed"
        );
    }
    super::owner::delete_parent_owner(object_type::SEQUENCE, tenant_id, name, catalog);
}

pub fn put_state(state: &SequenceState, catalog: &SystemCatalog) {
    if let Err(e) = catalog.put_sequence_state(state) {
        warn!(
            sequence = %state.name,
            tenant = state.tenant_id,
            error = %e,
            "catalog_entry: put_sequence_state failed"
        );
    }
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
        let seq = StoredSequence::new(1, "counter".into(), "bob".into());
        let entry = CatalogEntry::PutSequence(Box::new(seq));
        let bytes = encode(&entry).unwrap();
        match decode(&bytes).unwrap() {
            CatalogEntry::PutSequence(s) => {
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
            tenant_id: 42,
            name: "gone".into(),
        };
        let bytes = encode(&entry).unwrap();
        match decode(&bytes).unwrap() {
            CatalogEntry::DeleteSequence { tenant_id, name } => {
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

        let seq = StoredSequence::new(1, "orders_id_seq".into(), "alice".into());
        apply_to(&CatalogEntry::PutSequence(Box::new(seq)), catalog).expect("apply put_sequence");

        let loaded = catalog
            .get_sequence(1, "orders_id_seq")
            .unwrap()
            .expect("present");
        assert_eq!(loaded.name, "orders_id_seq");

        apply_to(
            &CatalogEntry::DeleteSequence {
                tenant_id: 1,
                name: "orders_id_seq".into(),
            },
            catalog,
        )
        .expect("apply delete_sequence");

        assert!(catalog.get_sequence(1, "orders_id_seq").unwrap().is_none());
    }
}
