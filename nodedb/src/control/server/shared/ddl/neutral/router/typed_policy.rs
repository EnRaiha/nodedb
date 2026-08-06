// SPDX-License-Identifier: BUSL-1.1

//! Typed DDL arms for `PolicyStmt`: conflict policies, RLS policies, custom
//! types, retention policies, and synonym groups.

use nodedb_sql::ddl_ast::statement::{NodedbStatement, PolicyStmt};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::conflict_policy;
use super::super::custom_type;
use super::super::redaction::{self, CreateRedactionPolicyRequest};
use super::super::retention_policy;
use super::super::rls::{self, CreateRlsPolicyRequest};
use super::super::synonym_group;

pub(super) async fn try_typed(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    _sql: &str,
    database_id: DatabaseId,
    stmt: &NodedbStatement,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    match stmt {
        // `SHOW CONFLICT POLICY ON <collection>`. Parses into a typed
        // `PolicyStmt::ShowConflictPolicy` and was dispatched from the pgwire
        // typed-AST async router. The Data Plane `GetPolicy` read is preserved
        // verbatim in `conflict_policy`.
        NodedbStatement::Policy(PolicyStmt::ShowConflictPolicy { collection }) => Some(
            conflict_policy::show_conflict_policy(state, identity, database_id, collection).await,
        ),

        NodedbStatement::Policy(PolicyStmt::CreateRlsPolicy {
            name,
            collection,
            policy_type,
            predicate_raw,
            is_restrictive,
            on_deny_raw,
            tenant_id_override,
        }) => Some(rls::create_rls_policy(
            state,
            identity,
            &CreateRlsPolicyRequest {
                name,
                collection,
                policy_type_raw: policy_type,
                predicate_raw,
                is_restrictive: *is_restrictive,
                on_deny_raw: on_deny_raw.as_deref(),
                tenant_id_override: *tenant_id_override,
            },
        )),

        NodedbStatement::Policy(PolicyStmt::DropRlsPolicy {
            name,
            collection,
            if_exists,
            tenant_id_override,
        }) => Some(rls::drop_rls_policy(
            state,
            identity,
            name,
            collection,
            *if_exists,
            *tenant_id_override,
        )),

        NodedbStatement::Policy(PolicyStmt::ShowRlsPolicies {
            collection,
            tenant_id_override,
        }) => Some(rls::show_rls_policies(
            state,
            identity,
            collection.as_deref(),
            *tenant_id_override,
        )),

        NodedbStatement::Policy(PolicyStmt::CreateRedactionPolicy {
            name,
            collection,
            for_role,
            rules,
            if_not_exists,
            tenant_id_override,
        }) => Some(redaction::create_redaction_policy(
            state,
            identity,
            &CreateRedactionPolicyRequest {
                name,
                collection,
                for_role,
                rules,
                if_not_exists: *if_not_exists,
                tenant_id_override: *tenant_id_override,
            },
        )),

        NodedbStatement::Policy(PolicyStmt::DropRedactionPolicy {
            collection,
            for_role,
            if_exists,
            tenant_id_override,
        }) => Some(redaction::drop_redaction_policy(
            state,
            identity,
            collection,
            for_role,
            *if_exists,
            *tenant_id_override,
        )),

        NodedbStatement::Policy(PolicyStmt::ShowRedactionPolicies {
            collection,
            tenant_id_override,
        }) => Some(redaction::show_redaction_policies(
            state,
            identity,
            collection.as_deref(),
            *tenant_id_override,
        )),

        NodedbStatement::Policy(PolicyStmt::CreateEnumType { name, labels }) => {
            Some(custom_type::create_enum_type(state, identity, name, labels))
        }

        NodedbStatement::Policy(PolicyStmt::CreateCompositeType { name, fields }) => Some(
            custom_type::create_composite_type(state, identity, name, fields),
        ),

        NodedbStatement::Policy(PolicyStmt::DropType { name, if_exists }) => {
            Some(custom_type::drop_type(state, identity, name, *if_exists))
        }

        NodedbStatement::Policy(PolicyStmt::AlterTypeAddValue { type_name, label }) => Some(
            custom_type::alter_type_add_value(state, identity, type_name, label),
        ),

        NodedbStatement::Policy(PolicyStmt::ShowTypes) => {
            Some(custom_type::show_types(state, identity))
        }

        NodedbStatement::Policy(PolicyStmt::CreateRetentionPolicy {
            name,
            collection,
            body_raw,
            eval_interval_raw,
        }) => Some(
            retention_policy::create_retention_policy(
                state,
                identity,
                database_id,
                name,
                collection,
                body_raw,
                eval_interval_raw.as_deref(),
            )
            .await,
        ),

        NodedbStatement::Policy(PolicyStmt::AlterRetentionPolicy {
            name,
            action,
            set_key,
            set_value,
        }) => Some(retention_policy::alter_retention_policy(
            state,
            identity,
            database_id,
            name,
            action,
            set_key.as_deref(),
            set_value.as_deref(),
        )),

        NodedbStatement::Policy(PolicyStmt::DropRetentionPolicy { name, if_exists }) => {
            // IF EXISTS short-circuit folded from the pgwire guard: a DROP of a
            // non-existing retention policy returns the tag before the handler
            // runs (and before the tenant-admin gate). The existence check reads
            // the in-memory registry for the identity tenant scoped to the
            // selected database.
            let tid = identity.tenant_id.as_u64();
            if *if_exists
                && state
                    .retention_policy_registry
                    .get(database_id.as_u64(), tid, name)
                    .is_none()
            {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP RETENTION POLICY".to_string(),
                    rows_affected: None,
                }]));
            }
            Some(retention_policy::drop_retention_policy(state, identity, database_id, name).await)
        }

        NodedbStatement::Policy(PolicyStmt::CreateSynonymGroup { name, terms }) => Some(
            synonym_group::create_synonym_group(state, identity, database_id, name, terms).await,
        ),

        NodedbStatement::Policy(PolicyStmt::DropSynonymGroup { name, if_exists }) => Some(
            synonym_group::drop_synonym_group(state, identity, database_id, name, *if_exists).await,
        ),

        NodedbStatement::Policy(PolicyStmt::ShowSynonymGroups) => {
            Some(synonym_group::show_synonym_groups(state, identity))
        }

        _ => None,
    }
}
