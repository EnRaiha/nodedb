// SPDX-License-Identifier: BUSL-1.1

//! Single-node DDL catch-up shim.
//!
//! When `metadata_proposer::propose_catalog_entry` reports
//! [`ProposeOutcome::LocalOnly`] — single node or rolling-upgrade compat
//! mode — no Raft applier will run on this node, and the originating
//! handler is solely responsible for landing the catalog write. A
//! `Buffered` entry belongs to an open transaction and applies nothing.
//!
//! [`apply_locally_if_needed`] is the only place that short-circuit
//! happens. It routes through [`apply_to`], whose per-family appliers
//! pair every primary write with the matching owner write, so the
//! orphan-row class is unrepresentable by construction.

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
/// Always returns, whether the apply succeeded or not. Family handlers
/// raise on a redb error; this local-only caller is the one that logs and
/// continues, because the startup integrity repair in
/// `recovery_check::verify_and_repair` reconciles the row on the next boot.
/// A debug-mode orphan-row violation from [`apply_to`] is logged here for
/// the same reason, and `apply_to` files its `faultbox` report at the point
/// of detection, so the failure is never silently lost.
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
