// SPDX-License-Identifier: BUSL-1.1

//! Stable `kind()` label for every [`CatalogEntry`] variant.
//!
//! Kept out of `entry.rs` so the enum definition stays a pure type
//! declaration: the label table grows one line per variant and would
//! otherwise push the definition file past its size budget.

use super::entry::CatalogEntry;

impl CatalogEntry {
    /// Short, human-readable descriptor of this entry — used in
    /// trace / metric labels.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::PutCollection(_) => "put_collection",
            Self::PutCollectionIfAbsent(_) => "put_collection_if_absent",
            Self::DeactivateCollection { .. } => "deactivate_collection",
            Self::PurgeCollection { .. } => "purge_collection",
            Self::PutSequence(_) => "put_sequence",
            Self::DeleteSequence { .. } => "delete_sequence",
            Self::PutSequenceState(_) => "put_sequence_state",
            Self::PutTrigger(_) => "put_trigger",
            Self::DeleteTrigger { .. } => "delete_trigger",
            Self::PutFunction(_) => "put_function",
            Self::DeleteFunction { .. } => "delete_function",
            Self::PutProcedure(_) => "put_procedure",
            Self::DeleteProcedure { .. } => "delete_procedure",
            Self::PutSchedule(_) => "put_schedule",
            Self::DeleteSchedule { .. } => "delete_schedule",
            Self::PutChangeStream(_) => "put_change_stream",
            Self::DeleteChangeStream { .. } => "delete_change_stream",
            Self::PutUser(_) => "put_user",
            Self::DropUser { .. } => "drop_user",
            Self::PutRole(_) => "put_role",
            Self::DeleteRole { .. } => "delete_role",
            Self::PutApiKey(_) => "put_api_key",
            Self::RevokeApiKey { .. } => "revoke_api_key",
            Self::PutAuthUser(_) => "put_auth_user",
            Self::PutMaterializedView(_) => "put_materialized_view",
            Self::DeleteMaterializedView { .. } => "delete_materialized_view",
            Self::PutStreamingMaterializedView(_) => "put_streaming_materialized_view",
            Self::DeleteStreamingMaterializedView { .. } => "delete_streaming_materialized_view",
            Self::PutContinuousAggregate(_) => "put_continuous_aggregate",
            Self::DeleteContinuousAggregate { .. } => "delete_continuous_aggregate",
            Self::PutTenant(_) => "put_tenant",
            Self::PutTenantWithAdmin { .. } => "put_tenant_with_admin",
            Self::DeleteTenant { .. } => "delete_tenant",
            Self::PutRlsPolicy(_) => "put_rls_policy",
            Self::DeleteRlsPolicy { .. } => "delete_rls_policy",
            Self::PutRedactionPolicy(_) => "put_redaction_policy",
            Self::DeleteRedactionPolicy { .. } => "delete_redaction_policy",
            Self::PutPermission(_) => "put_permission",
            Self::DeletePermission { .. } => "delete_permission",
            Self::PutScopeGrant(_) => "put_scope_grant",
            Self::DeleteScopeGrant { .. } => "delete_scope_grant",
            Self::PutIndexRecord(_) => "put_index_record",
            Self::DeleteIndexRecord { .. } => "delete_index_record",
            Self::PutOwner(_) => "put_owner",
            Self::DeleteOwner { .. } => "delete_owner",
            Self::PutDatabase(_) => "put_database",
            Self::DeleteDatabase { .. } => "delete_database",
            Self::PutDatabaseGrant { .. } => "put_database_grant",
            Self::DeleteDatabaseGrant { .. } => "delete_database_grant",
            Self::PutSynonymGroup(_) => "put_synonym_group",
            Self::DeleteSynonymGroup { .. } => "delete_synonym_group",
            Self::PutCustomType(_) => "put_custom_type",
            Self::DeleteCustomType { .. } => "delete_custom_type",
            Self::PutOidcProvider(_) => "put_oidc_provider",
            Self::DeleteOidcProvider { .. } => "delete_oidc_provider",
            Self::RecordWalTombstone { .. } => "record_wal_tombstone",
            Self::MoveTenantCutover { .. } => "move_tenant_cutover",
            Self::CloneDatabase { .. } => "clone_database",
        }
    }
}
