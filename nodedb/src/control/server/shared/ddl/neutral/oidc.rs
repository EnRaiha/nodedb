// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral OIDC provider DDL — CREATE / ALTER / DROP / SHOW.
//!
//! Ported from the pgwire `ddl::oidc` handlers. All non-return logic
//! (superuser gate + its denial audit, empty-field validation, duplicate
//! name / issuer pre-checks, `StoredClaimMappingRule` build, catalog proposes,
//! local single-node fallbacks, `audit_record`) is preserved verbatim; only the
//! result construction changed from pgwire `Response` / `PgWireError` to the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! OIDC providers are system-scoped (superuser-only) and backed by the
//! `_system.oidc_providers` catalog table.

use serde_json::{Map, Value as JsonValue};

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredOidcProvider;
use crate::control::security::catalog::oidc_providers::StoredClaimMappingRule;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use nodedb_sql::ddl_ast::statement::OidcClaimMappingClause;

use super::super::result::{DdlError, DdlResult};

/// Build a single-tag status result.
fn status(command: &str) -> Vec<DdlResult> {
    vec![DdlResult::Status {
        command: command.to_string(),
        rows_affected: None,
    }]
}

/// Superuser gate, folded in verbatim from the pgwire `require_superuser`
/// helper: on denial it emits `AuditEvent::PermissionDenied` (database-less
/// scope, matching the `None` `db_id` the pgwire handlers passed) and returns
/// SQLSTATE 42501, preserving both the side effect and the wire error.
fn require_superuser(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    action: &str,
) -> Result<(), DdlError> {
    if identity.is_superuser {
        Ok(())
    } else {
        state.audit_record_with_db(
            AuditEvent::PermissionDenied,
            Some(identity.tenant_id),
            None,
            &identity.username,
            action,
        );
        Err(DdlError {
            sqlstate: "42501".to_string(),
            message: format!("permission denied: {action} requires superuser"),
        })
    }
}

/// Handle `CREATE OIDC PROVIDER <name> ISSUER '<iss>' JWKS_URI '<uri>' ...`.
pub fn create_oidc_provider(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    issuer: &str,
    jwks_uri: &str,
    audience: Option<&str>,
    claim_mappings: &[OidcClaimMappingClause],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "create OIDC providers")?;

    if issuer.is_empty() {
        return Err(DdlError {
            sqlstate: "22023".to_string(),
            message: "ISSUER must not be empty".to_string(),
        });
    }
    if jwks_uri.is_empty() {
        return Err(DdlError {
            sqlstate: "22023".to_string(),
            message: "JWKS_URI must not be empty".to_string(),
        });
    }

    let catalog = state.credentials.catalog().as_ref().ok_or(DdlError {
        sqlstate: "XX000".to_string(),
        message: "system catalog not available".to_string(),
    })?;

    // Check for duplicate by provider name.
    match catalog.get_oidc_provider(name) {
        Ok(Some(_)) => {
            return Err(DdlError {
                sqlstate: "42710".to_string(),
                message: format!("OIDC provider '{name}' already exists"),
            });
        }
        Ok(None) => {}
        Err(e) => {
            return Err(DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog read: {e}"),
            });
        }
    }

    // Check for duplicate issuer (one issuer → one provider).
    match catalog.list_oidc_providers() {
        Ok(providers) => {
            if providers.iter().any(|p| p.issuer == issuer) {
                return Err(DdlError {
                    sqlstate: "42710".to_string(),
                    message: format!("OIDC provider with issuer '{issuer}' already exists"),
                });
            }
        }
        Err(e) => {
            return Err(DdlError {
                sqlstate: "XX000".to_string(),
                message: format!("catalog list: {e}"),
            });
        }
    }

    let stored_mappings: Vec<StoredClaimMappingRule> = claim_mappings
        .iter()
        .map(|cm| StoredClaimMappingRule {
            claim_name: cm.claim_name.clone(),
            claim_value: cm.claim_value.clone(),
            default_database: cm.default_database,
            add_databases: cm.add_databases.clone(),
            add_roles: cm.add_roles.clone(),
        })
        .collect();

    let provider = StoredOidcProvider {
        provider_name: name.to_string(),
        issuer: issuer.to_string(),
        jwks_uri: jwks_uri.to_string(),
        audience: audience.map(str::to_string),
        claim_mapping: stored_mappings,
        created_at_lsn: 0,
    };

    let entry = CatalogEntry::PutOidcProvider(Box::new(provider.clone()));
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.put_oidc_provider(&provider).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog write: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE OIDC PROVIDER {name} issuer={issuer}"),
    );

    Ok(status("CREATE OIDC PROVIDER"))
}

/// Handle `ALTER OIDC PROVIDER <name> SET CLAIM MAPPING WHEN ...`.
///
/// Replaces the entire claim-mapping list for the named provider.
pub fn alter_oidc_provider_claim_mapping(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    claim_mappings: &[OidcClaimMappingClause],
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "alter OIDC providers")?;

    let catalog = state.credentials.catalog().as_ref().ok_or(DdlError {
        sqlstate: "XX000".to_string(),
        message: "system catalog not available".to_string(),
    })?;

    let mut provider = catalog
        .get_oidc_provider(name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .ok_or(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("OIDC provider '{name}' does not exist"),
        })?;

    let stored_mappings: Vec<StoredClaimMappingRule> = claim_mappings
        .iter()
        .map(|cm| StoredClaimMappingRule {
            claim_name: cm.claim_name.clone(),
            claim_value: cm.claim_value.clone(),
            default_database: cm.default_database,
            add_databases: cm.add_databases.clone(),
            add_roles: cm.add_roles.clone(),
        })
        .collect();

    provider.claim_mapping = stored_mappings;

    let entry = CatalogEntry::PutOidcProvider(Box::new(provider.clone()));
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.put_oidc_provider(&provider).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog write: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!(
            "ALTER OIDC PROVIDER {name} SET CLAIM MAPPING ({} rules)",
            provider.claim_mapping.len()
        ),
    );

    Ok(status("ALTER OIDC PROVIDER"))
}

/// Handle `DROP OIDC PROVIDER [IF EXISTS] <name>`.
pub fn drop_oidc_provider(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    if_exists: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "drop OIDC providers")?;

    let catalog = state.credentials.catalog().as_ref().ok_or(DdlError {
        sqlstate: "XX000".to_string(),
        message: "system catalog not available".to_string(),
    })?;

    if catalog
        .get_oidc_provider(name)
        .map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog read: {e}"),
        })?
        .is_none()
    {
        if if_exists {
            return Ok(status("DROP OIDC PROVIDER"));
        }
        return Err(DdlError {
            sqlstate: "42704".to_string(),
            message: format!("OIDC provider '{name}' does not exist"),
        });
    }

    let entry = CatalogEntry::DeleteOidcProvider {
        name: name.to_string(),
    };
    let log_index = propose_catalog_entry(state, &entry).map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("metadata propose: {e}"),
    })?;
    if log_index == 0 {
        catalog.delete_oidc_provider(name).map_err(|e| DdlError {
            sqlstate: "XX000".to_string(),
            message: format!("catalog delete: {e}"),
        })?;
    }

    state.audit_record(
        AuditEvent::OidcProviderChanged,
        Some(identity.tenant_id),
        &identity.username,
        &format!("DROP OIDC PROVIDER {name}"),
    );

    Ok(status("DROP OIDC PROVIDER"))
}

/// Handle `SHOW OIDC PROVIDERS`.
pub fn show_oidc_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(state, identity, "show OIDC providers")?;

    let catalog = state.credentials.catalog().as_ref().ok_or(DdlError {
        sqlstate: "XX000".to_string(),
        message: "system catalog not available".to_string(),
    })?;

    let providers = catalog.list_oidc_providers().map_err(|e| DdlError {
        sqlstate: "XX000".to_string(),
        message: format!("catalog list: {e}"),
    })?;

    let columns = vec![
        "name".to_string(),
        "issuer".to_string(),
        "jwks_uri".to_string(),
        "audience".to_string(),
        "claim_mapping_rules".to_string(),
    ];

    let mut rows = Vec::with_capacity(providers.len());
    for p in &providers {
        let mut row = Map::new();
        row.insert(
            "name".to_string(),
            JsonValue::String(p.provider_name.clone()),
        );
        row.insert("issuer".to_string(), JsonValue::String(p.issuer.clone()));
        row.insert(
            "jwks_uri".to_string(),
            JsonValue::String(p.jwks_uri.clone()),
        );
        let aud = p.audience.as_deref().unwrap_or("");
        row.insert("audience".to_string(), JsonValue::String(aud.to_string()));
        let rule_count = p.claim_mapping.len().to_string();
        row.insert(
            "claim_mapping_rules".to_string(),
            JsonValue::String(rule_count),
        );
        rows.push(row);
    }

    let column_types = ShapedRows::text_types(columns.len());
    Ok(vec![DdlResult::Rows(ShapedRows {
        columns,
        column_types,
        rows,
        notice: None,
    })])
}
