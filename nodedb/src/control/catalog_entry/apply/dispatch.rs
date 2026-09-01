// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive dispatch of a [`CatalogEntry`] to its per-family apply function.

use tracing::debug;

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::SystemCatalog;

use super::{
    api_key, auth_user, change_stream, collection, continuous_aggregate, custom_type, database,
    function, index_registry, materialized_view, oidc_provider, owner, permission, procedure,
    quota, redaction, retention_policy, rls, role, schedule, scope_grant, scope_quota, sequence,
    streaming_materialized_view, synonym_group, tenant, trigger, user, wal_tombstone,
};

/// Apply `entry` to `catalog`.
///
/// A failed catalog write raises: skipping a committed metadata entry
/// diverges this node from the quorum. `Ok(false)` reports that the entry
/// wrote nothing, which still concludes its DDL. Debug builds verify
/// referential integrity after every apply — release-gated because a full
/// rescan would wedge `raft_tick_loop` on a node with a pre-existing orphan.
pub fn apply_to(entry: &CatalogEntry, catalog: &SystemCatalog) -> Result<bool, crate::Error> {
    let applied = match entry {
        CatalogEntry::PutTenantWithAdmin { tenant, admin } => {
            tenant::put_with_admin(tenant, admin, catalog)?
        }
        _ => {
            apply_to_inner(entry, catalog)?;
            true
        }
    };
    if !applied {
        return Ok(false);
    }
    #[cfg(debug_assertions)]
    {
        // Narrow to OrphanRow (primary row without owner row, or vice versa).
        // DanglingReference is test-fixture hygiene / legitimate startup
        // state — leave those to the full startup-time verifier.
        use crate::control::cluster::recovery_check::divergence::DivergenceKind;
        let orphans: Vec<_> =
            crate::control::cluster::recovery_check::integrity::verify_redb_integrity(catalog)
                .into_iter()
                .filter(|d| matches!(d.kind, DivergenceKind::OrphanRow { .. }))
                .collect();
        if let Some(first) = orphans.first() {
            let DivergenceKind::OrphanRow { kind, .. } = &first.kind else {
                unreachable!("filtered to OrphanRow above");
            };
            crate::diag::catalog_apply_orphan_row(entry.kind(), kind, orphans.len());
            return Err(crate::Error::CatalogIntegrityViolation {
                entry_kind: entry.kind().to_string(),
                detail: format!(
                    "every parent-replicated Put* variant must write both the primary row \
                     and the StoredOwner row; orphan violations: {orphans:?}"
                ),
            });
        }
    }
    Ok(true)
}

fn apply_to_inner(entry: &CatalogEntry, catalog: &SystemCatalog) -> crate::Result<()> {
    match entry {
        CatalogEntry::PutCollection(stored) => collection::put(stored, catalog),
        CatalogEntry::PutCollectionIfAbsent(stored) => collection::put_if_absent(stored, catalog),
        CatalogEntry::DeactivateCollection {
            database_id,
            tenant_id,
            name,
            descriptor_version,
            modification_hlc,
        } => collection::deactivate(
            *database_id,
            *tenant_id,
            name,
            collection::DeactivateStamp {
                descriptor_version: *descriptor_version,
                modification_hlc: *modification_hlc,
            },
            catalog,
        ),
        CatalogEntry::PurgeCollection {
            database_id,
            tenant_id,
            name,
        } => {
            // Preserve an inactive row until post-apply storage reclaim
            // succeeds — the restart-durable same-name lifecycle barrier.
            match collection::prepare_purge(*database_id, *tenant_id, name, catalog) {
                // A node that never held the row has nothing to fence.
                // Interactive purge paths use `prepare_purge_checked` instead.
                Ok(found) => {
                    debug!(
                        collection = %name,
                        tenant = *tenant_id,
                        found,
                        "catalog_entry: purge preparation"
                    );
                    Ok(())
                }
                Err(error) => panic!("collection catalog purge preparation failed: {error}"),
            }
        }
        CatalogEntry::PutSequence(stored) => sequence::put(stored, catalog),
        CatalogEntry::DeleteSequence { tenant_id, name } => {
            sequence::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutSequenceState(state) => sequence::put_state(state, catalog),
        CatalogEntry::PutTrigger(stored) => trigger::put(stored, catalog),
        CatalogEntry::DeleteTrigger {
            database_id,
            tenant_id,
            name,
        } => trigger::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutFunction(stored) => function::put(stored, catalog),
        CatalogEntry::DeleteFunction {
            database_id,
            tenant_id,
            name,
        } => function::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutProcedure(stored) => procedure::put(stored, catalog),
        CatalogEntry::DeleteProcedure {
            database_id,
            tenant_id,
            name,
        } => procedure::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutSchedule(stored) => schedule::put(stored, catalog),
        CatalogEntry::DeleteSchedule {
            database_id,
            tenant_id,
            name,
        } => schedule::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutChangeStream(stored) => change_stream::put(stored, catalog),
        CatalogEntry::DeleteChangeStream {
            database_id,
            tenant_id,
            name,
        } => change_stream::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutUser(stored) => user::put(stored, catalog),
        CatalogEntry::DropUser { username } => user::delete(username, catalog),
        CatalogEntry::PutRole(stored) => role::put(stored, catalog),
        CatalogEntry::DeleteRole { name } => role::delete(name, catalog),
        CatalogEntry::PutApiKey(stored) => api_key::put(stored, catalog),
        CatalogEntry::RevokeApiKey { key_id } => api_key::revoke(key_id, catalog),
        CatalogEntry::PutAuthUser(stored) => auth_user::put(stored, catalog),
        CatalogEntry::PutMaterializedView(stored) => materialized_view::put(stored, catalog),
        CatalogEntry::DeleteMaterializedView { tenant_id, name } => {
            match materialized_view::delete(*tenant_id, name, catalog) {
                Ok(()) => Ok(()),
                Err(error) => panic!("materialized-view catalog deletion failed: {error}"),
            }
        }
        CatalogEntry::PutStreamingMaterializedView(definition) => {
            streaming_materialized_view::put(definition, catalog)
        }
        CatalogEntry::DeleteStreamingMaterializedView {
            database_id,
            tenant_id,
            name,
        } => match streaming_materialized_view::delete(*database_id, *tenant_id, name, catalog) {
            Ok(()) => Ok(()),
            Err(error) => panic!("streaming materialized-view catalog deletion failed: {error}"),
        },
        CatalogEntry::PutContinuousAggregate(stored) => continuous_aggregate::put(stored, catalog),
        CatalogEntry::DeleteContinuousAggregate {
            database_id,
            tenant_id,
            name,
        } => continuous_aggregate::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutTenant(stored) => tenant::put(stored, catalog),
        // Applied by `apply_to` so its commit outcome can suppress post-apply.
        CatalogEntry::PutTenantWithAdmin { .. } => Ok(()),
        CatalogEntry::DeleteTenant { tenant_id } => tenant::delete(*tenant_id, catalog),
        CatalogEntry::PutRlsPolicy(stored) => rls::put(stored, catalog),
        CatalogEntry::DeleteRlsPolicy {
            tenant_id,
            collection,
            name,
        } => rls::delete(*tenant_id, collection, name, catalog),
        CatalogEntry::PutRedactionPolicy(stored) => redaction::put(stored, catalog),
        CatalogEntry::DeleteRedactionPolicy {
            tenant_id,
            collection,
            for_role,
        } => redaction::delete(*tenant_id, collection, for_role, catalog),
        CatalogEntry::PutPermission(stored) => permission::put(stored, catalog),
        CatalogEntry::DeletePermission {
            target,
            grantee,
            permission: perm,
        } => permission::delete(target, grantee, perm, catalog),
        CatalogEntry::PutScopeGrant(stored) => scope_grant::put(stored, catalog),
        CatalogEntry::DeleteScopeGrant {
            scope_name,
            grantee_type,
            grantee_id,
        } => scope_grant::delete(scope_name, grantee_type, grantee_id, catalog),
        CatalogEntry::PutIndexRecord(record) => index_registry::put(record, catalog),
        CatalogEntry::DeleteIndexRecord {
            database_id,
            tenant_id,
            name,
            ..
        } => index_registry::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::PutOwner(stored) => owner::put(stored, catalog),
        CatalogEntry::DeleteOwner {
            object_type,
            database_id,
            tenant_id,
            object_name,
        } => owner::delete(object_type, *database_id, *tenant_id, object_name, catalog),
        CatalogEntry::PutSynonymGroup(stored) => synonym_group::put(stored, catalog),
        CatalogEntry::DeleteSynonymGroup { tenant_id, name } => {
            synonym_group::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutCustomType(stored) => custom_type::put(stored, catalog),
        CatalogEntry::DeleteCustomType { tenant_id, name } => {
            custom_type::delete(*tenant_id, name, catalog)
        }
        CatalogEntry::PutDatabase(descriptor) => database::put(descriptor, catalog),
        CatalogEntry::DeleteDatabase { db_id } => database::delete(*db_id, catalog),
        CatalogEntry::PutDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => database::put_grant(*db_id, *user_id, privilege, catalog),
        CatalogEntry::DeleteDatabaseGrant {
            db_id,
            user_id,
            privilege,
        } => database::delete_grant(*db_id, *user_id, privilege, catalog),
        CatalogEntry::CloneDatabase {
            target_descriptor,
            source_db_id,
        } => database::clone_apply(target_descriptor, *source_db_id, catalog),
        CatalogEntry::PutOidcProvider(provider) => oidc_provider::put(provider, catalog),
        CatalogEntry::DeleteOidcProvider { name } => oidc_provider::delete(name, catalog),
        CatalogEntry::RecordWalTombstone {
            database_id,
            tenant_id,
            collection,
            purge_lsn,
        } => wal_tombstone::record(*database_id, *tenant_id, collection, *purge_lsn, catalog),
        CatalogEntry::PutDatabaseQuota { db_id, record } => {
            quota::put_database(*db_id, record, catalog)
        }
        CatalogEntry::DeleteDatabaseQuota { db_id } => quota::delete_database(*db_id, catalog),
        CatalogEntry::PutTenantQuota {
            db_id,
            tenant_id,
            record,
        } => quota::put_tenant(*db_id, *tenant_id, record, catalog),
        CatalogEntry::DeleteTenantQuota { db_id, tenant_id } => {
            quota::delete_tenant(*db_id, *tenant_id, catalog)
        }
        CatalogEntry::PutScopeQuota(stored) => scope_quota::put(stored, catalog),
        CatalogEntry::DeleteScopeQuota { scope_name } => scope_quota::delete(scope_name, catalog),
        CatalogEntry::PutRetentionPolicy(def) => retention_policy::put(def, catalog),
        CatalogEntry::DeleteRetentionPolicy {
            database_id,
            tenant_id,
            name,
            ..
        } => retention_policy::delete(*database_id, *tenant_id, name, catalog),
        CatalogEntry::MoveTenantCutover {
            tenant_id,
            source_db_id,
            target_db_id,
            collections,
        } => tenant::move_cutover(
            *tenant_id,
            *source_db_id,
            *target_db_id,
            collections,
            catalog,
        ),
    }
}
