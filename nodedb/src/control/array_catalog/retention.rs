// SPDX-License-Identifier: BUSL-1.1

//! The compliance floor of an ALTER ARRAY retention change.
//!
//! Lowering checks it for an early refusal. The catalog apply checks it again
//! against the committed entry, so a plan lowered before a concurrent or
//! later ALTER (a cached plan included) cannot shorten retention below the
//! floor.

use nodedb_types::config::retention::BitemporalRetention;

use super::ArrayCatalogEntry;

/// Refuse an ALTER of `name` whose resulting `audit_retain_ms` falls below
/// its resulting `minimum_audit_retain_ms`.
///
/// Both changes use ALTER's double-`Option`: `None` keeps the field of
/// `current`, `Some(None)` clears it, `Some(Some(v))` sets it.
pub(crate) fn check_retention_floor(
    name: &str,
    current: &ArrayCatalogEntry,
    audit_retain_ms: Option<Option<i64>>,
    minimum_audit_retain_ms: Option<Option<u64>>,
) -> crate::Result<()> {
    let floor = match minimum_audit_retain_ms {
        Some(changed) => changed.unwrap_or(0),
        None => current.minimum_audit_retain_ms.unwrap_or(0),
    };
    let retain = audit_retain_ms.unwrap_or(current.audit_retain_ms);
    let Some(retain_ms) = retain else {
        return Ok(());
    };
    let retention = BitemporalRetention {
        data_retain_ms: 0,
        audit_retain_ms: retain_ms as u64,
        minimum_audit_retain_ms: floor,
    };
    retention.validate().map_err(|e| crate::Error::PlanError {
        detail: format!("ALTER ARRAY {name}: {e}"),
    })
}
