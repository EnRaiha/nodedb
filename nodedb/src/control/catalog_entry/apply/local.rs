// SPDX-License-Identifier: BUSL-1.1

//! Single-node DDL catch-up shim.
//!
//! When `metadata_proposer::propose_catalog_entry` reports
//! [`ProposeOutcome::LocalOnly`] — single node or rolling-upgrade compat
//! mode — no Raft applier will run on this node, and the originating
//! handler is solely responsible for landing the catalog write. A
//! `Buffered` entry belongs to an open transaction and applies nothing. Earlier code did this with an ad-hoc
//! `if log_index == 0 { catalog.put_<type>(...)?; }` block in every
//! handler, which silently forgot the companion
//! `owner::put_parent_owner` write. That orphaned every newly-created
//! parent-replicated object on disk and bricked the next clean
//! restart at `CatalogSanityCheck`.
//!
//! [`apply_locally_if_needed`] is the one and only place that
//! short-circuit happens now. It routes through [`apply_to`], whose
//! per-family appliers already pair every primary write with the
//! matching owner write, so the orphan-row class is unrepresentable
//! by construction.

use tracing::warn;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::apply_to;

/// Apply `entry` locally only for [`ProposeOutcome::LocalOnly`], so the
/// originating node's redb catalog reflects the DDL. No-op for
/// `Replicated` (the Raft applier has run, or will) and for `Buffered`
/// (the open transaction owns the entry until COMMIT).
///
/// Always returns, whether the apply succeeded or not — family
/// handlers warn-and-continue on per-table redb errors to match the
/// Raft applier's "best effort, replay on restart" semantics, and a
/// release-mode catalog write failure is caught at the next startup
/// by the integrity repair pass in `recovery_check::verify_and_repair`.
/// A debug-mode orphan-row violation from [`apply_to`] is logged
/// here rather than propagated, for the same reason; `apply_to`
/// already files a `faultbox` report at the point of detection, so
/// the failure is not silently lost even though this caller does
/// not raise it.
pub fn apply_locally_if_needed(state: &SharedState, entry: &CatalogEntry, outcome: ProposeOutcome) {
    if !outcome.needs_local_apply() {
        return;
    }
    let catalog = state.credentials.catalog();
    if let Err(e) = apply_to(entry, catalog) {
        warn!(
            kind = entry.kind(),
            error = %e,
            "catalog_entry: apply_locally_if_needed: apply_to failed"
        );
    }
}
