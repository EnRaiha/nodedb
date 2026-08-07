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
//! `PutApiKey`, `PutAuthUser`, `PutRlsPolicy`, `PutSchedule`, `PutChangeStream`,
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
    let mut hlc = clock.now();
    match entry {
        CatalogEntry::PutCollection(mut stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
            if let Some(prior_hlc) = prior.as_ref().map(|c| c.modification_hlc)
                && prior_hlc >= hlc
            {
                hlc = clock.update(prior_hlc);
            }
            let prior_descriptor = prior.as_ref().map(|c| c.descriptor_version).unwrap_or(0);
            stored.descriptor_version = prior_descriptor.saturating_add(1);
            // Constraint version bumps ONLY when the derived constraint set
            // actually changes, so an unrelated ALTER never advances the
            // apply-time fence key and never transiently rejects in-flight
            // CRDT deltas. `Constraint: Eq` + name-sorted translator make the
            // set comparison exact and order-stable.
            let prior_constraint_version =
                prior.as_ref().map(|c| c.constraint_version).unwrap_or(0);
            let prior_set = prior
                .as_ref()
                .map(crate::control::security::catalog::collection_constraints)
                .unwrap_or_default();
            let new_set = crate::control::security::catalog::collection_constraints(&stored);
            stored.constraint_version = if new_set != prior_set {
                prior_constraint_version.saturating_add(1)
            } else {
                prior_constraint_version
            };
            stored.modification_hlc = hlc;
            CatalogEntry::PutCollection(stored)
        }
        CatalogEntry::PutCollectionIfAbsent(mut stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten();
            // Existing is a semantic no-op. Freeze the exact persisted record
            // rather than manufacturing an unpersisted next version; this also
            // makes later full-log replay payload-identical and lets a following
            // real mutation in the same batch advance from the true prior.
            if let Some(prior) = prior {
                return CatalogEntry::PutCollectionIfAbsent(Box::new(prior));
            }
            stored.descriptor_version = 1;
            let new_set = crate::control::security::catalog::collection_constraints(&stored);
            stored.constraint_version = u64::from(!new_set.is_empty());
            stored.modification_hlc = hlc;
            CatalogEntry::PutCollectionIfAbsent(stored)
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
                .get_function_in_database(stored.database_id, stored.tenant_id, &stored.name)
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
                .get_procedure_in_database(stored.database_id, stored.tenant_id, &stored.name)
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
                .get_trigger_in_database(stored.database_id, stored.tenant_id, &stored.name)
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
        | CatalogEntry::PutStreamingMaterializedView(_)
        | CatalogEntry::DeleteStreamingMaterializedView { .. }
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
        // Auth-user records carry no descriptor version.
        | CatalogEntry::PutAuthUser(_)
        | CatalogEntry::PutTenant(_)
        | CatalogEntry::PutTenantWithAdmin { .. }
        | CatalogEntry::DeleteTenant { .. }
        | CatalogEntry::PutRlsPolicy(_)
        | CatalogEntry::DeleteRlsPolicy { .. }
        // Redaction policies carry no descriptor version, same as RLS.
        | CatalogEntry::PutRedactionPolicy(_)
        | CatalogEntry::DeleteRedactionPolicy { .. }
        | CatalogEntry::PutPermission(_)
        | CatalogEntry::DeletePermission { .. }
        // Scope grants carry no descriptor version, same as permissions.
        | CatalogEntry::PutScopeGrant(_)
        | CatalogEntry::DeleteScopeGrant { .. }
        | CatalogEntry::PutIndexRecord(_)
        | CatalogEntry::DeleteIndexRecord { .. }
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
        | CatalogEntry::RecordWalTombstone { .. }
        | CatalogEntry::CloneDatabase { .. }
        | CatalogEntry::MoveTenantCutover { .. }) => entry,
    }
}

/// Stamp a transactional DDL batch in statement order. Persisted catalog
/// state seeds the first mutation of each descriptor; a prior mutation of the
/// same descriptor in this batch seeds the next one.
pub fn stamp_batch(
    entries: Vec<CatalogEntry>,
    clock: &HlcClock,
    catalog: &SystemCatalog,
) -> Vec<CatalogEntry> {
    let mut stamped_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let mut stamped = stamp(entry, clock, catalog);
        if let Some(prior) = stamped_entries
            .iter()
            .rev()
            .find(|prior| same_descriptor(prior, &stamped))
        {
            stamped = advance_after(prior, stamped);
        }
        stamped_entries.push(stamped);
    }
    stamped_entries
}

fn same_descriptor(prior: &CatalogEntry, current: &CatalogEntry) -> bool {
    match (prior, current) {
        (
            CatalogEntry::PutCollection(a) | CatalogEntry::PutCollectionIfAbsent(a),
            CatalogEntry::PutCollection(b) | CatalogEntry::PutCollectionIfAbsent(b),
        ) => a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name,
        (CatalogEntry::PutMaterializedView(a), CatalogEntry::PutMaterializedView(b)) => {
            a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutFunction(a), CatalogEntry::PutFunction(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutProcedure(a), CatalogEntry::PutProcedure(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutTrigger(a), CatalogEntry::PutTrigger(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutSequence(a), CatalogEntry::PutSequence(b)) => {
            a.tenant_id == b.tenant_id && a.name == b.name
        }
        (CatalogEntry::PutContinuousAggregate(a), CatalogEntry::PutContinuousAggregate(b)) => {
            a.database_id == b.database_id && a.tenant_id == b.tenant_id && a.name == b.name
        }
        _ => false,
    }
}

fn advance_after(prior: &CatalogEntry, current: CatalogEntry) -> CatalogEntry {
    match (prior, current) {
        (
            CatalogEntry::PutCollection(prior) | CatalogEntry::PutCollectionIfAbsent(prior),
            CatalogEntry::PutCollection(mut current),
        ) => {
            advance_collection(prior, &mut current);
            CatalogEntry::PutCollection(current)
        }
        (
            CatalogEntry::PutCollection(prior) | CatalogEntry::PutCollectionIfAbsent(prior),
            CatalogEntry::PutCollectionIfAbsent(mut current),
        ) => {
            advance_collection(prior, &mut current);
            CatalogEntry::PutCollectionIfAbsent(current)
        }
        (
            CatalogEntry::PutMaterializedView(prior),
            CatalogEntry::PutMaterializedView(mut current),
        ) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutMaterializedView(current)
        }
        (CatalogEntry::PutFunction(prior), CatalogEntry::PutFunction(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutFunction(current)
        }
        (CatalogEntry::PutProcedure(prior), CatalogEntry::PutProcedure(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutProcedure(current)
        }
        (CatalogEntry::PutTrigger(prior), CatalogEntry::PutTrigger(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutTrigger(current)
        }
        (CatalogEntry::PutSequence(prior), CatalogEntry::PutSequence(mut current)) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutSequence(current)
        }
        (
            CatalogEntry::PutContinuousAggregate(prior),
            CatalogEntry::PutContinuousAggregate(mut current),
        ) => {
            current.descriptor_version = prior.descriptor_version.saturating_add(1);
            CatalogEntry::PutContinuousAggregate(current)
        }
        (_, current) => current,
    }
}

fn advance_collection(
    prior: &crate::control::security::catalog::StoredCollection,
    current: &mut crate::control::security::catalog::StoredCollection,
) {
    current.descriptor_version = prior.descriptor_version.saturating_add(1);
    let prior_set = crate::control::security::catalog::collection_constraints(prior);
    let current_set = crate::control::security::catalog::collection_constraints(current);
    current.constraint_version = if prior_set == current_set {
        prior.constraint_version
    } else {
        prior.constraint_version.saturating_add(1)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::{
        ParamDirection, ProcedureParam, ProcedureRoutability,
    };
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::security::catalog::{
        FunctionLanguage, FunctionSecurity, FunctionVolatility, StoredCollection, StoredFunction,
        StoredProcedure, StoredSequence, StoredTrigger,
    };
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
        let catalog = store.catalog();
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
        let catalog = store.catalog();

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
    fn stamp_batch_advances_repeated_collection_mutations() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let entries = vec![
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "orders", "tester"))),
            CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "orders", "tester"))),
        ];
        let stamped = stamp_batch(entries, &clock, store.catalog());
        let CatalogEntry::PutCollection(first) = &stamped[0] else {
            panic!("expected first collection");
        };
        let CatalogEntry::PutCollection(second) = &stamped[1] else {
            panic!("expected second collection");
        };
        assert_eq!(first.descriptor_version, 1);
        assert_eq!(second.descriptor_version, 2);
    }

    /// Persist a collection at `version` so the next stamp reads it as prior.
    fn seed_prior(catalog: &SystemCatalog, name: &str, version: u64) {
        let mut stored = StoredCollection::new(1, name, "tester");
        stored.descriptor_version = version;
        catalog
            .put_collection(DatabaseId::DEFAULT, &stored)
            .expect("put_collection");
    }

    fn function(database_id: DatabaseId) -> StoredFunction {
        StoredFunction {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![],
            return_type: "INT".into(),
            body_sql: "1".into(),
            compiled_body_sql: None,
            volatility: FunctionVolatility::Immutable,
            security: FunctionSecurity::Invoker,
            language: FunctionLanguage::Sql,
            wasm_hash: None,
            wasm_module: None,
            dependencies: vec![],
            wasm_fuel: 1_000_000,
            wasm_memory: 16 * 1024 * 1024,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn procedure(database_id: DatabaseId) -> StoredProcedure {
        StoredProcedure {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            parameters: vec![ProcedureParam {
                name: "input".into(),
                data_type: "INT".into(),
                direction: ParamDirection::In,
            }],
            body_sql: "BEGIN END".into(),
            max_iterations: 1_000_000,
            timeout_secs: 60,
            routability: ProcedureRoutability::MultiCollection,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    fn trigger(database_id: DatabaseId) -> StoredTrigger {
        StoredTrigger {
            tenant_id: 1,
            database_id,
            name: "same_name".into(),
            collection: "orders".into(),
            timing: TriggerTiming::After,
            events: TriggerEvents {
                on_insert: true,
                on_update: false,
                on_delete: false,
            },
            granularity: TriggerGranularity::Row,
            when_condition: None,
            body_sql: "BEGIN END".into(),
            priority: 0,
            enabled: true,
            execution_mode: TriggerExecutionMode::Async,
            security: TriggerSecurity::Invoker,
            batch_mode: TriggerBatchMode::BatchSafe,
            owner: "tester".into(),
            created_at: 0,
            descriptor_version: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[test]
    fn same_named_cross_database_routines_do_not_coalesce() {
        let database_a = DatabaseId::new(11);
        let database_b = DatabaseId::new(12);
        let pairs = [
            (
                CatalogEntry::PutFunction(Box::new(function(database_a))),
                CatalogEntry::PutFunction(Box::new(function(database_b))),
            ),
            (
                CatalogEntry::PutProcedure(Box::new(procedure(database_a))),
                CatalogEntry::PutProcedure(Box::new(procedure(database_b))),
            ),
            (
                CatalogEntry::PutTrigger(Box::new(trigger(database_a))),
                CatalogEntry::PutTrigger(Box::new(trigger(database_b))),
            ),
        ];

        for (first, second) in pairs {
            assert!(!same_descriptor(&first, &second));
        }
    }

    #[test]
    fn stamp_batch_existing_if_absent_does_not_consume_a_version() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        seed_prior(store.catalog(), "orders", 1);
        let mut announcement = StoredCollection::new(1, "orders", "remote");
        announcement.descriptor_version = 0;
        let update = StoredCollection::new(1, "orders", "updated");
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutCollectionIfAbsent(Box::new(announcement)),
                CatalogEntry::PutCollection(Box::new(update)),
            ],
            &clock,
            store.catalog(),
        );
        let CatalogEntry::PutCollectionIfAbsent(noop) = &stamped[0] else {
            panic!("expected create-only entry");
        };
        let CatalogEntry::PutCollection(update) = &stamped[1] else {
            panic!("expected real update");
        };
        assert_eq!(noop.descriptor_version, 1);
        assert_eq!(noop.owner, "tester");
        assert_eq!(update.descriptor_version, 2);
    }

    #[test]
    fn stamp_batch_advances_repeated_sequence_mutations() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let sequence = StoredSequence::new(1, "invoice_seq".into(), "tester".into());
        let stamped = stamp_batch(
            vec![
                CatalogEntry::PutSequence(Box::new(sequence.clone())),
                CatalogEntry::PutSequence(Box::new(sequence)),
            ],
            &clock,
            store.catalog(),
        );
        let CatalogEntry::PutSequence(first) = &stamped[0] else {
            panic!("expected first sequence");
        };
        let CatalogEntry::PutSequence(second) = &stamped[1] else {
            panic!("expected second sequence");
        };
        assert_eq!(first.descriptor_version, 1);
        assert_eq!(second.descriptor_version, 2);
    }

    #[test]
    fn stamp_ignores_deletes() {
        let (store, _tmp) = make_catalog();
        let clock = HlcClock::new();
        let catalog = store.catalog();
        let entry = CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 1,
            name: "orders".into(),
        };
        let stamped = stamp(entry, &clock, catalog);
        assert!(matches!(stamped, CatalogEntry::DeactivateCollection { .. }));
    }
}
