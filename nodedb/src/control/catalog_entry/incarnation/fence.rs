// SPDX-License-Identifier: BUSL-1.1

//! Apply-time fence decisions for entries that carry an incarnation clock.

use nodedb_types::Hlc;
use std::cmp::Ordering;

use super::target::{carried_target, delete_key, written_row};
use crate::control::catalog_entry::CatalogEntry;
use crate::control::catalog_entry::descriptor_validate::ValidationOutcome;
use crate::control::security::catalog::SystemCatalog;

/// Decide a fenced delete against the row it names.
///
/// - row absent: apply. A follower that never held the row has nothing to fence.
/// - row clock above the target: a later incarnation. Acknowledge. An
///   unstamped target (`Hlc::ZERO`) was proposed against an absent row, so any
///   stamped row is later than it.
/// - row clock equal to the target: the targeted incarnation. Apply. This
///   covers an unstamped target against a row that predates HLC stamping.
/// - row clock below the target: this node missed a mutation the proposer saw.
///
/// A live delete never meets a later incarnation: the proposer stamps while
/// it holds the replicated DDL preparation lease, after its own node applied
/// the lease acquire, and a `DdlPrepared` entry applies only under the token
/// that owns the lease. Acknowledgement is reached by log replay alone.
pub fn check_delete(
    entry: &CatalogEntry,
    catalog: &SystemCatalog,
) -> crate::Result<ValidationOutcome> {
    let (Some(key), Some(target)) = (delete_key(entry), carried_target(entry)) else {
        return Ok(ValidationOutcome::Apply);
    };
    let Some(current) = key.read(catalog)? else {
        return Ok(ValidationOutcome::Apply);
    };
    match current.hlc.cmp(&target.hlc) {
        Ordering::Greater => Ok(ValidationOutcome::AlreadyApplied),
        Ordering::Equal => Ok(ValidationOutcome::Apply),
        Ordering::Less => Err(crate::Error::DescriptorVersionAnomaly {
            descriptor: key.name().to_string(),
            carried: target.descriptor_version,
            prior: current.descriptor_version,
        }),
    }
}

/// Acknowledge a soft delete or an unversioned put whose row a later
/// incarnation already holds. The entry's own clock is newer than the row it
/// was stamped against, so a row clock at or below it applies.
pub fn check_superseded(
    entry: &CatalogEntry,
    catalog: &SystemCatalog,
) -> crate::Result<ValidationOutcome> {
    let Some((key, incoming)) = written_row(entry) else {
        return Ok(ValidationOutcome::Apply);
    };
    if incoming.hlc == Hlc::ZERO {
        return Ok(ValidationOutcome::Apply);
    }
    Ok(match key.read(catalog)? {
        Some(current) if current.hlc > incoming.hlc => ValidationOutcome::AlreadyApplied,
        _ => ValidationOutcome::Apply,
    })
}
