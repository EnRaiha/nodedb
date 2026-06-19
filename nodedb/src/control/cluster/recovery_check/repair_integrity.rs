// SPDX-License-Identifier: BUSL-1.1

//! Self-heal pass for `OrphanRow` integrity violations.
//!
//! The startup catalog sanity check used to treat every orphan row
//! as fatal — a single CREATE DDL that landed the primary row
//! without its companion `StoredOwner` (the original
//! single-node bypass bug) would brick boot at the
//! `CatalogSanityCheck` phase with no operator recourse short of
//! wiping `system.redb`.
//!
//! Every parent-replicated `Stored<T>` carries the creator's
//! username in-band (`StoredCollection.owner`,
//! `StoredFunction.owner`, etc. — kept in sync by every applier
//! including `DeactivateCollection`). That is total reconstruction
//! information for the missing `StoredOwner` row, so an orphan is
//! always recoverable without operator action.
//!
//! [`heal_orphan_rows`] runs after `verify_redb_integrity` and
//! before the report is assembled. For every `OrphanRow` whose
//! parent kind is `owner`, it reads the surviving primary row, lifts
//! its `owner` field, and writes the missing `StoredOwner` back to
//! the `OWNERS` table. Healed divergences are dropped from the
//! returned list; anything we can't repair (the primary row really
//! is gone, or the catalog write fails) stays in
//! `integrity_violations` and still aborts startup.

use nodedb_types::DatabaseId;
use tracing::{info, warn};

use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::auth_types::{StoredOwner, object_type};

use super::divergence::{Divergence, DivergenceKind};

/// Heal every `OrphanRow { expected_parent_kind: "owner", .. }`
/// violation in `violations` by reconstructing the missing
/// `StoredOwner` row from the primary row's in-band `owner` field.
///
/// Returns `(remaining, healed)`. `remaining` contains every
/// divergence we did not repair (different kinds, primary row
/// missing, write failure); `healed` is the count of orphan rows
/// successfully repaired — surfaced in the `VerifyReport` so the
/// `CatalogSanityCheck` log line shows `repaired=N` instead of the
/// pre-fix `repaired=0`.
pub fn heal_orphan_rows(
    catalog: &SystemCatalog,
    violations: Vec<Divergence>,
) -> (Vec<Divergence>, usize) {
    let mut remaining = Vec::with_capacity(violations.len());
    let mut healed = 0usize;
    for d in violations {
        if let DivergenceKind::OrphanRow {
            kind,
            key,
            expected_parent_kind: "owner",
        } = &d.kind
            && let Some((tenant_id, name)) = parse_key(key)
            && let Some(owner_username) = primary_row_owner(catalog, kind, tenant_id, &name)
        {
            let stored = StoredOwner {
                object_type: (*kind).to_string(),
                object_name: name.clone(),
                tenant_id,
                owner_username: owner_username.clone(),
            };
            match catalog.put_owner(&stored) {
                Ok(()) => {
                    info!(
                        kind,
                        tenant_id,
                        object = %name,
                        owner = %owner_username,
                        "catalog sanity check: healed orphan row by \
                         reconstructing StoredOwner from primary row's \
                         in-band owner field"
                    );
                    healed += 1;
                    continue;
                }
                Err(e) => warn!(
                    kind,
                    tenant_id,
                    object = %name,
                    error = %e,
                    "catalog sanity check: could not heal orphan row — \
                     leaving divergence in integrity_violations"
                ),
            }
        }
        remaining.push(d);
    }
    (remaining, healed)
}

/// Parse the `OrphanRow.key` format `"{tenant_id}:{name}"` written
/// by `verify_redb_integrity`. Returns `None` if the key isn't in
/// that shape (e.g. a future divergence kind reuses the field with
/// different semantics) — the caller then leaves the divergence
/// unrepaired rather than guessing.
fn parse_key(key: &str) -> Option<(u64, String)> {
    let (tenant, name) = key.split_once(':')?;
    let tenant_id = tenant.parse().ok()?;
    Some((tenant_id, name.to_string()))
}

/// Look up the surviving primary row for a parent-replicated DDL
/// object and return its in-band `owner` field. `None` means the
/// primary row is genuinely missing — at which point the orphan
/// cannot be auto-repaired and startup should still abort, because
/// `verify_redb_integrity` should not have flagged this divergence
/// in the first place if the primary were gone.
fn primary_row_owner(
    catalog: &SystemCatalog,
    kind: &str,
    tenant_id: u64,
    name: &str,
) -> Option<String> {
    match kind {
        object_type::COLLECTION => catalog
            .get_collection(DatabaseId::DEFAULT, tenant_id, name)
            .ok()
            .flatten()
            .map(|c| c.owner),
        object_type::FUNCTION => catalog
            .get_function(tenant_id, name)
            .ok()
            .flatten()
            .map(|f| f.owner),
        object_type::PROCEDURE => catalog
            .get_procedure(tenant_id, name)
            .ok()
            .flatten()
            .map(|p| p.owner),
        object_type::TRIGGER => catalog
            .get_trigger(tenant_id, name)
            .ok()
            .flatten()
            .map(|t| t.owner),
        object_type::MATERIALIZED_VIEW => catalog
            .get_materialized_view(tenant_id, name)
            .ok()
            .flatten()
            .map(|m| m.owner),
        object_type::SEQUENCE => catalog
            .get_sequence(tenant_id, name)
            .ok()
            .flatten()
            .map(|s| s.owner),
        object_type::SCHEDULE => catalog
            .load_all_schedules()
            .ok()
            .and_then(|all| {
                all.into_iter()
                    .find(|s| s.tenant_id == tenant_id && s.name == name)
            })
            .map(|s| s.owner),
        object_type::CHANGE_STREAM => catalog
            .get_change_stream(tenant_id, name)
            .ok()
            .flatten()
            .map(|c| c.owner),
        object_type::CONTINUOUS_AGGREGATE => catalog
            // This recovery-repair owner lookup is keyed only by
            // (tenant_id, name); like the COLLECTION arm above it resolves
            // within the default database. Cross-database owner repair is a
            // separate verifier initiative.
            .get_continuous_aggregate(DatabaseId::DEFAULT.as_u64(), tenant_id, name)
            .ok()
            .flatten()
            .map(|c| c.owner),
        // Unknown kinds shouldn't appear in OrphanRow today (the
        // verifier only flags the parent-replicated types) but
        // surface here as "couldn't repair" rather than crashing.
        _ => None,
    }
}
