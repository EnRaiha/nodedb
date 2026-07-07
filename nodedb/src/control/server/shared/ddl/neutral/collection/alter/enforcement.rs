// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION ... SET {RETENTION,LEGAL_HOLD,APPEND_ONLY,LAST_VALUE_CACHE}`
//! — non-schema enforcement knobs propagated through `CatalogEntry::PutCollection`.
//!
//! Ported verbatim from the pgwire `ddl::collection::alter::enforcement`
//! handlers; only the result type changed to the protocol-neutral
//! [`DdlResult`] / [`DdlError`]. The retention-period validation, legal-hold
//! add/remove bookkeeping, append-only / last-value-cache guards, the
//! `PutCollection` propose, and the `schema_version` bump are unchanged, as is
//! the `ALTER COLLECTION` command tag.

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::{err, status};

/// ALTER COLLECTION <name> SET RETENTION = '<value>'
pub(super) fn alter_collection_set_retention(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    value: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, name)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    crate::data::executor::enforcement::retention::parse_retention_period(value)
        .map_err(|e| err("22023", e.to_string()))?;

    coll.retention_period = Some(value.to_string());
    persist_and_bump(state, &coll)
}

/// ALTER COLLECTION <name> SET LEGAL_HOLD = TRUE|FALSE TAG '<tag>'
pub(super) fn alter_collection_set_legal_hold(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    enabled: bool,
    tag: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, name)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    if enabled {
        if coll.legal_holds.iter().any(|h| h.tag == tag) {
            return Err(err(
                "23505",
                format!("legal hold tag '{tag}' already exists on {name}"),
            ));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        coll.legal_holds
            .push(crate::control::security::catalog::LegalHold {
                tag: tag.to_string(),
                created_at: now,
                created_by: identity.username.clone(),
            });
    } else {
        let before = coll.legal_holds.len();
        coll.legal_holds.retain(|h| h.tag != tag);
        if coll.legal_holds.len() == before {
            return Err(err(
                "42704",
                format!("legal hold tag '{tag}' not found on {name}"),
            ));
        }
    }

    persist_and_bump(state, &coll)
}

/// ALTER COLLECTION <name> SET APPEND_ONLY
pub(super) fn alter_collection_set_append_only(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, name)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    if coll.append_only {
        return Err(err(
            "42710",
            format!("collection '{name}' is already append-only"),
        ));
    }
    coll.append_only = true;
    persist_and_bump(state, &coll)
}

/// ALTER COLLECTION <name> SET LAST_VALUE_CACHE = TRUE|FALSE
pub(super) fn alter_collection_set_last_value_cache(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    name: &str,
    enabled: bool,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let catalog = state.credentials.catalog();
    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, name)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| err("42P01", format!("collection '{name}' not found")))?;

    if !coll.collection_type.is_timeseries() {
        return Err(err(
            "42809",
            format!("'{name}' is not a timeseries collection"),
        ));
    }
    coll.lvc_enabled = enabled;
    persist_and_bump(state, &coll)
}

fn persist_and_bump(
    state: &SharedState,
    coll: &crate::control::security::catalog::StoredCollection,
) -> Result<Vec<DdlResult>, DdlError> {
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    super::support::propose_and_apply(state, &entry)?;
    state.schema_version.bump();
    Ok(status("ALTER COLLECTION"))
}
