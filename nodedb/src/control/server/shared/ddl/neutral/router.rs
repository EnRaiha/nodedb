// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL router.
//!
//! [`try_dispatch`] recognizes the migrated families and routes to them; every
//! other statement returns `None` so the transitional pgwire delegation in the
//! parent [`super::super::dispatch`] handles it.

use nodedb_sql::ddl_ast::statement::{AuthStmt, CollectionStmt, NodedbStatement, PolicyStmt};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::result::{DdlError, DdlResult};
use super::oidc;
use super::rls::{self, CreateRlsPolicyRequest};
use super::sequence::{self, CreateSequenceRequest};

/// Try to handle `sql` with a migrated protocol-neutral DDL family handler.
///
/// Returns `Some(result)` when a migrated family owns the statement, `None`
/// otherwise (non-migrated family, parse error, or a sub-case that today falls
/// through to the SQL planner) so the caller can fall back to the transitional
/// pgwire delegation.
pub async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    _database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Parse errors / non-DDL / non-migrated families → let the pgwire path run,
    // which re-parses and reproduces the exact error handling for those inputs.
    let stmt = match nodedb_sql::ddl_ast::parse(sql) {
        Some(Ok(stmt)) => stmt,
        _ => return None,
    };

    match &stmt {
        NodedbStatement::Collection(CollectionStmt::CreateSequence {
            name,
            if_not_exists,
            start,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            format_template_raw,
            reset_period_raw,
            gap_free,
            scope,
        }) => {
            // IF NOT EXISTS on a non-existing sequence falls through to the
            // planner today (the pgwire guard returned None and no create arm
            // matched `if_not_exists: true`). Replicate by returning None.
            let tenant_id = identity.tenant_id.as_u64();
            if *if_not_exists && !state.sequence_registry.exists(tenant_id, name) {
                return None;
            }
            Some(sequence::create_sequence(
                state,
                identity,
                &CreateSequenceRequest {
                    name,
                    if_not_exists: *if_not_exists,
                    start: *start,
                    increment: *increment,
                    min_value: *min_value,
                    max_value: *max_value,
                    cycle: *cycle,
                    cache: *cache,
                    format_template_raw: format_template_raw.as_deref(),
                    reset_period_raw: reset_period_raw.as_deref(),
                    gap_free: *gap_free,
                    scope: scope.as_deref(),
                },
            ))
        }

        NodedbStatement::Collection(CollectionStmt::AlterSequence {
            name,
            action,
            with_value,
        }) => Some(sequence::alter_sequence(
            state,
            identity,
            name,
            action,
            with_value.as_deref(),
        )),

        NodedbStatement::Collection(CollectionStmt::DropSequence { name, if_exists }) => {
            Some(sequence::drop_sequence(state, identity, name, *if_exists))
        }

        NodedbStatement::Collection(CollectionStmt::ShowSequences) => {
            Some(sequence::show_sequences(state, identity))
        }

        NodedbStatement::Collection(CollectionStmt::DescribeSequence { name }) => {
            Some(sequence::describe_sequence(state, identity, name))
        }

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
        }) => {
            // IF EXISTS on a non-existing policy short-circuits to the tag,
            // folded from the pgwire guard (which checks existence against the
            // identity tenant). The existing case and the non-IF-EXISTS case
            // fall through to the token-based handler, which re-derives the name
            // / collection / TENANT override from `parts` exactly as the pgwire
            // string dispatch did.
            let tid = identity.tenant_id.as_u64();
            if *if_exists && !state.rls.policy_exists(tid, collection, name) {
                return Some(Ok(vec![DdlResult::Status {
                    command: "DROP RLS POLICY".to_string(),
                    rows_affected: None,
                }]));
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(rls::drop_rls_policy(state, identity, &parts))
        }

        NodedbStatement::Policy(PolicyStmt::ShowRlsPolicies { .. }) => {
            // The AST recognizes the broader `SHOW RLS POLI…` prefix, but the
            // pgwire string dispatch only handled `SHOW RLS POLICIES` /
            // `SHOW RLS POLICY`; narrower inputs fell through to the planner.
            // Replicate that exact prefix guard by returning None otherwise.
            let upper = sql.to_uppercase();
            if !(upper.starts_with("SHOW RLS POLICIES") || upper.starts_with("SHOW RLS POLICY")) {
                return None;
            }
            let parts: Vec<&str> = sql.split_whitespace().collect();
            Some(rls::show_rls_policies(state, identity, &parts))
        }

        NodedbStatement::Auth(AuthStmt::CreateOidcProvider {
            name,
            issuer,
            jwks_uri,
            audience,
            claim_mappings,
        }) => Some(oidc::create_oidc_provider(
            state,
            identity,
            name,
            issuer,
            jwks_uri,
            audience.as_deref(),
            claim_mappings,
        )),

        NodedbStatement::Auth(AuthStmt::AlterOidcProviderClaimMapping {
            name,
            claim_mappings,
        }) => Some(oidc::alter_oidc_provider_claim_mapping(
            state,
            identity,
            name,
            claim_mappings,
        )),

        NodedbStatement::Auth(AuthStmt::DropOidcProvider { name, if_exists }) => {
            Some(oidc::drop_oidc_provider(state, identity, name, *if_exists))
        }

        NodedbStatement::Auth(AuthStmt::ShowOidcProviders) => {
            Some(oidc::show_oidc_providers(state, identity))
        }

        _ => None,
    }
}
