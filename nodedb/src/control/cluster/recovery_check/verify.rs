// SPDX-License-Identifier: BUSL-1.1

//! Top-level pipeline invoked at the `CatalogSanityCheck`
//! startup phase.
//!
//! Runs the three sub-checks in order:
//!
//! 1. Applied-index gate — local `MetadataCache.applied_index`
//!    against the current `AppliedIndexWatcher` watermark.
//! 2. Registry ⇔ redb verifier — re-load every in-memory
//!    registry and swap in fresh on any divergence.
//! 3. redb cross-table integrity check — referential
//!    invariants inside redb. Unrepairable — any violation
//!    fails the sanity check.
//!
//! Returns a [`VerifyReport`] with per-phase outcomes. The
//! caller (main.rs) checks `report.is_acceptable()` and
//! either advances the phase or calls
//! `shared.startup.fail()` + aborts startup.

use std::time::Instant;

use crate::control::state::SharedState;

use super::applied_index::check_applied_index;
use super::integrity::verify_redb_integrity;
use super::registry_verify::verify_registries;
use super::repair_integrity::heal_orphan_rows;
use super::report::VerifyReport;

/// Run the full catalog sanity check pipeline against the
/// shared state. Never panics, never writes to redb.
/// Repairs in-memory registries in place.
pub async fn verify_and_repair(shared: &SharedState) -> crate::Result<VerifyReport> {
    let start = Instant::now();

    // ── 1. Applied-index gate ──────────────────────────
    let gate = check_applied_index(shared);
    if !gate.is_ok() {
        tracing::error!(
            cache_applied = gate.cache_applied,
            watcher_current = gate.watcher_current,
            gap = gate.gap,
            "catalog sanity check: applied_index gap — metadata replay incomplete"
        );
    }

    // ── 2. Registry ⇔ redb verification + repair ───────
    //
    // Single-node / no-catalog mode: `credentials.catalog()`
    // returns `None` because the `SystemCatalog` is
    // in-memory only. Nothing to verify against — skip both
    // the registry verifier AND the integrity walker.
    let (registry_outcome, integrity, integrity_healed) = {
        let catalog = shared.credentials.catalog();

        let reg = verify_registries(shared, catalog)?;
        let raw = verify_redb_integrity(catalog);
        // Self-heal the orphan-row class: reconstruct every
        // missing `StoredOwner` from the primary row's in-band
        // `owner` field. Anything still in `remaining` is a real
        // integrity bug (primary row gone, catalog write failed,
        // or a future divergence kind we don't know how to
        // repair) and must still fail the startup gate.
        let (remaining, healed) = heal_orphan_rows(catalog, raw);
        if healed > 0 {
            tracing::info!(
                healed,
                remaining = remaining.len(),
                "catalog sanity check: integrity self-heal pass repaired \
                     orphan rows by reconstructing StoredOwner entries from \
                     primary rows' in-band owner fields"
            );
        }
        (Some(reg), remaining, healed)
    };

    // ── 3. Assemble report ─────────────────────────────
    let (registry_divergences, all_repairs_ok) = match registry_outcome {
        Some(o) => {
            // Emit labeled metrics: one observation per registry.
            if let Some(metrics) = shared.system_metrics.as_deref() {
                for (registry, count) in &o.counts {
                    let outcome = if count.detected == 0 {
                        "ok"
                    } else if count.repaired == count.detected {
                        "warning"
                    } else {
                        "error"
                    };
                    metrics.record_catalog_sanity_check(registry, outcome);
                }
            }
            (o.counts, o.all_repairs_ok)
        }
        None => (Default::default(), true),
    };

    Ok(VerifyReport {
        applied_index_ok: gate.is_ok(),
        applied_index_gap: gate.gap,
        integrity_violations: integrity,
        integrity_repaired: integrity_healed,
        registry_divergences,
        all_repairs_ok,
        elapsed: start.elapsed(),
    })
}
