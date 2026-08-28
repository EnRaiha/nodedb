// SPDX-License-Identifier: BUSL-1.1

//! Derive a `DescriptorId` (and, where relevant, prior persisted version)
//! from a `CatalogEntry` — the drain proposer's and the metadata applier's
//! shared entry point into "which descriptor does this DDL touch".

use nodedb_types::DatabaseId;

use nodedb_cluster::{DescriptorId, DescriptorKind};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

/// For a `Put*` entry that carries `descriptor_version`, return
/// the `DescriptorId` whose drain should be implicitly cleared
/// after the entry applies. Returns `None` for variants without
/// descriptor versioning (auth, schedules, change streams, etc.).
///
/// Called from `MetadataCommitApplier::apply_host_side_effects`
/// on every node — after the `apply_to` succeeds, the applier
/// looks up the drained id via this helper and calls
/// `shared.lease_drain.install_end` on it. This is how drain
/// clears implicitly on the happy path without a second raft
/// round-trip.
pub fn descriptor_id_for_implicit_clear(entry: &CatalogEntry) -> Option<DescriptorId> {
    match entry {
        CatalogEntry::PutCollection(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        )),
        CatalogEntry::PutCollectionIfAbsent(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Collection,
            stored.name.clone(),
        )),
        CatalogEntry::PutMaterializedView(stored) => Some(DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            stored.tenant_id,
            DescriptorKind::MaterializedView,
            stored.name.clone(),
        )),
        CatalogEntry::PutFunction(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Function,
            stored.name.clone(),
        )),
        CatalogEntry::PutProcedure(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Procedure,
            stored.name.clone(),
        )),
        CatalogEntry::PutTrigger(stored) => Some(DescriptorId::new(
            stored.database_id.as_u64(),
            stored.tenant_id,
            DescriptorKind::Trigger,
            stored.name.clone(),
        )),

        CatalogEntry::PutSequence(stored) => Some(DescriptorId::new(
            DatabaseId::DEFAULT.as_u64(),
            stored.tenant_id,
            DescriptorKind::Sequence,
            stored.name.clone(),
        )),
        _ => None,
    }
}

/// For a `Put*` entry that carries `descriptor_version`, return
/// `(descriptor_id, prior_persisted_version)` so the proposer can
/// decide whether to run drain. `prior_persisted_version` is `0`
/// on create (no prior record) and causes `drain_for_ddl` to
/// return immediately.
///
/// Called from `metadata_proposer::propose_catalog_entry_with_timeout`
/// BEFORE the raft propose path. Reads from `SystemCatalog` under
/// a short read txn — the read is consistent with the subsequent
/// propose because the stamp logic in the applier increments
/// from the same prior value under its own write txn.
pub fn descriptor_id_and_prior_version(
    entry: &CatalogEntry,
    shared: &SharedState,
) -> Option<(DescriptorId, u64)> {
    let catalog = shared.credentials.catalog();
    match entry {
        CatalogEntry::PutCollection(stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Collection,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutCollectionIfAbsent(stored) => {
            let prior = catalog
                .get_collection(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|c| c.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Collection,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutMaterializedView(stored) => {
            let prior = catalog
                .get_materialized_view(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|v| v.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    DatabaseId::DEFAULT.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::MaterializedView,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutFunction(stored) => {
            let prior = catalog
                .get_function_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|f| f.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Function,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutProcedure(stored) => {
            let prior = catalog
                .get_procedure_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|p| p.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Procedure,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutTrigger(stored) => {
            let prior = catalog
                .get_trigger_in_database(stored.database_id, stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|t| t.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    stored.database_id.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Trigger,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        CatalogEntry::PutSequence(stored) => {
            let prior = catalog
                .get_sequence(stored.tenant_id, &stored.name)
                .ok()
                .flatten()
                .map(|s| s.descriptor_version)
                .unwrap_or(0);
            Some((
                DescriptorId::new(
                    DatabaseId::DEFAULT.as_u64(),
                    stored.tenant_id,
                    DescriptorKind::Sequence,
                    stored.name.clone(),
                ),
                prior,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::procedure_types::ProcedureRoutability;
    use crate::control::security::catalog::trigger_types::{
        TriggerBatchMode, TriggerEvents, TriggerExecutionMode, TriggerGranularity, TriggerSecurity,
        TriggerTiming,
    };
    use crate::control::security::catalog::{
        FunctionLanguage, FunctionSecurity, FunctionVolatility, StoredCollection, StoredFunction,
        StoredProcedure, StoredTrigger,
    };

    fn function(database_id: DatabaseId) -> StoredFunction {
        StoredFunction {
            tenant_id: 41,
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
            tenant_id: 41,
            database_id,
            name: "same_name".into(),
            parameters: vec![],
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
            tenant_id: 41,
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
    fn routine_descriptor_ids_preserve_selected_database() {
        let database_id = DatabaseId::new(73);
        let entries = [
            CatalogEntry::PutFunction(Box::new(function(database_id))),
            CatalogEntry::PutProcedure(Box::new(procedure(database_id))),
            CatalogEntry::PutTrigger(Box::new(trigger(database_id))),
        ];

        for entry in entries {
            let id = descriptor_id_for_implicit_clear(&entry).expect("routine descriptor id");
            assert_eq!(id.database_id, database_id.as_u64());
            assert_eq!(id.tenant_id, 41);
            assert_eq!(id.name, "same_name");
        }
    }

    #[test]
    fn implicit_clear_collection_id_preserves_non_default_database() {
        let mut stored = StoredCollection::new(41, "orders", "owner");
        stored.database_id = DatabaseId::new(73);
        let entry = CatalogEntry::PutCollection(Box::new(stored));

        let id = descriptor_id_for_implicit_clear(&entry).expect("collection descriptor id");
        assert_eq!(id.database_id, 73);
        assert_eq!(id.tenant_id, 41);
        assert_eq!(id.kind, DescriptorKind::Collection);
        assert_eq!(id.name, "orders");
    }
}
