// SPDX-License-Identifier: BUSL-1.1

//! Descriptor versioning stamp helpers.
//!
//! Called by the metadata commit applier right before any `Put*`
//! `CatalogEntry` is written to `SystemCatalog` redb. Reads the prior
//! persisted record, increments `descriptor_version` by one (or
//! assigns `1` on create), and stamps `modification_hlc` from the
//! node-local [`HlcClock`]. Returns the entry with stamped fields
//! so the applier calls `apply_to` with the stamped value.
//!
//! The stamp is a pure function of the prior state, the clock, and
//! the incoming entry — no global side effects beyond advancing the
//! local HLC. This makes it safe to call on every tick of every node
//! inside the raft apply path.
//!
//! ## Rolling upgrade contract
//!
//! In mixed-version clusters, stamping is gated by
//! [`crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION`].
//! When the cluster is in compat mode the applier must skip this
//! helper entirely — the gate check lives at the call site so this
//! module is oblivious to it.
//!
//! ## Variants without descriptor fields
//!
//! Not every `CatalogEntry` variant carries descriptor version/HLC.
//! `PutUser`, `PutRole`, `PutPermission`, `PutOwner`, `PutTenant`,
//! `PutApiKey`, `PutRlsPolicy`, `PutSchedule`, `PutChangeStream`,
//! `PutSequenceState`, and the `Delete*` / `Deactivate*` variants
//! are returned unchanged. The helper is exhaustive on
//! [`CatalogEntry`] so adding a new variant is a compile-time
//! error here — the compiler forces you to make a conscious
//! decision about whether it needs a version stamp.

use nodedb_types::HlcClock;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

/// Read the prior persisted descriptor (if any), assign
/// `descriptor_version = prior + 1` (or `1` on create), stamp
/// `modification_hlc = clock.now()`, and return the entry.
///
/// Infallible by design: if a redb read fails (unlikely — the
/// applier already holds the only writer and the read txn can't
/// race), we log at debug level and stamp as if the record was
/// absent (version `1`). Version `0` is never emitted by this
/// function — it is strictly the "pre-stamping compat mode"
/// sentinel.
pub fn stamp(entry: CatalogEntry, clock: &HlcClock, catalog: &SystemCatalog) -> CatalogEntry {
    let hlc = clock.now();
    match entry {
        CatalogEntry::PutCollection(mut stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutCollection(stored)
        }
        CatalogEntry::PutMaterializedView(mut stored) => {
            let prior = catalog
                .get_materialized_view(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|v| v.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutMaterializedView(stored)
        }
        CatalogEntry::PutFunction(mut stored) => {
            let prior = catalog
                .get_function(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|f| f.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutFunction(stored)
        }
        CatalogEntry::PutProcedure(mut stored) => {
            let prior = catalog
                .get_procedure(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|p| p.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutProcedure(stored)
        }
        CatalogEntry::PutTrigger(mut stored) => {
            let prior = catalog
                .get_trigger(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|t| t.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutTrigger(stored)
        }
        CatalogEntry::PutSequence(mut stored) => {
            let prior = catalog
                .get_sequence(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|s| s.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutSequence(stored)
        }
        CatalogEntry::PutContinuousAggregate(mut stored) => {
            let prior = catalog
                .get_continuous_aggregate(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            stored.descriptor_version = prior.saturating_add(1);
            stored.modification_hlc = hlc;
            CatalogEntry::PutContinuousAggregate(stored)
        }
        // Variants without descriptor versioning pass through
        // unchanged. Exhaustive match forces explicit handling of
        // any future variant added to `CatalogEntry`.
        entry @ (CatalogEntry::DeactivateCollection { .. }
        | CatalogEntry::PurgeCollection { .. }
        | CatalogEntry::DeleteFunction { .. }
        | CatalogEntry::DeleteProcedure { .. }
        | CatalogEntry::DeleteTrigger { .. }
        | CatalogEntry::DeleteMaterializedView { .. }
        | CatalogEntry::DeleteContinuousAggregate { .. }
        | CatalogEntry::DeleteSequence { .. }
        | CatalogEntry::PutSequenceState(_)
        | CatalogEntry::PutSchedule(_)
        | CatalogEntry::DeleteSchedule { .. }
        | CatalogEntry::PutChangeStream(_)
        | CatalogEntry::DeleteChangeStream { .. }
        | CatalogEntry::PutUser(_)
        | CatalogEntry::DropUser { .. }
        | CatalogEntry::PutRole(_)
        | CatalogEntry::DeleteRole { .. }
        | CatalogEntry::PutApiKey(_)
        | CatalogEntry::RevokeApiKey { .. }
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        | CatalogEntry::PutOwner(_)
        | CatalogEntry::DeleteOwner { .. }
        | CatalogEntry::PutSynonymGroup(_)
        | CatalogEntry::DeleteSynonymGroup { .. }
        | CatalogEntry::PutCustomType(_)
        | CatalogEntry::DeleteCustomType { .. }
        | CatalogEntry::PutDatabase(_)
        | CatalogEntry::DeleteDatabase { .. }
        | CatalogEntry::PutDatabaseGrant { .. }
        | CatalogEntry::DeleteDatabaseGrant { .. }
        | CatalogEntry::PutOidcProvider(_)
        | CatalogEntry::DeleteOidcProvider { .. }
        | CatalogEntry::CloneDatabase { .. }
        | CatalogEntry::MoveTenantCutover { .. }) => entry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::StoredCollection;
    use crate::control::security::credential::CredentialStore;
    use nodedb_types::DatabaseId;
    use std::sync::Arc;

    fn make_catalog() -> (Arc<CredentialStore>, tempfile::TempDir) {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let store = Arc::new(CredentialStore::open(&tmp.path().join("system.redb")).expect("open"));
        (store, tmp)
    }

    #[test]
    fn stamp_on_create_assigns_version_one() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog().as_ref().expect("catalog");
        let stored = StoredCollection::new(1, "orders", "tester");
        let entry = CatalogEntry::PutCollection(Box::new(stored));

        let stamped = stamp(entry, &clock, catalog);
        let CatalogEntry::PutCollection(boxed) = stamped else {
            panic!("expected PutCollection");
        };
        assert_eq!(boxed.descriptor_version, 1);
        assert!(boxed.modification_hlc > nodedb_types::Hlc::ZERO);
    }

    #[test]
    fn stamp_monotonic_across_updates() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog().as_ref().expect("catalog");

        let mut prior_hlc = nodedb_types::Hlc::ZERO;
        for expected in 1u64..=5 {
            let stored = StoredCollection::new(1, "orders", "tester");
            let entry = CatalogEntry::PutCollection(Box::new(stored));
            let stamped = stamp(entry, &clock, catalog);
            let CatalogEntry::PutCollection(boxed) = stamped else {
                panic!("expected PutCollection");
            };
            assert_eq!(boxed.descriptor_version, expected);
            assert!(boxed.modification_hlc > prior_hlc);
            prior_hlc = boxed.modification_hlc;
            // Persist so the next iteration reads this as prior.
            catalog
                .put_collection(DatabaseId::DEFAULT, &boxed)
                .expect("put_collection");
        }
    }

    #[test]
    fn stamp_ignores_deletes() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog().as_ref().expect("catalog");
        let entry = CatalogEntry::DeactivateCollection {
            tenant_id: 1,
            name: "orders".into(),
        };
        let stamped = stamp(entry, &clock, catalog);
        assert!(matches!(stamped, CatalogEntry::DeactivateCollection { .. }));
    }
}
