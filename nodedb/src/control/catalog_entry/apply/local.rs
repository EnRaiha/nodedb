// SPDX-License-Identifier: BUSL-1.1

//! Single-node DDL catch-up shim.
//!
//! When `metadata_proposer::propose_catalog_entry` returns
//! `Ok(0)` — single-node, rolling-upgrade compat mode, or inside a
//! DDL-transaction buffer — no Raft applier will run on this node,
//! and the originating handler is solely responsible for landing the
//! catalog write. Earlier code did this with an ad-hoc
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

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

use super::apply_to;

/// When `log_index == 0`, apply `entry` locally so the originating
/// node's redb catalog reflects the DDL. No-op when `log_index > 0`
/// (the Raft applier has already run, or will).
///
/// Returns `Ok(())` whether the apply succeeded or not — family
/// handlers warn-and-continue on per-table redb errors to match the
/// Raft applier's "best effort, replay on restart" semantics. A
/// release-mode catalog write failure is logged; a debug-mode one
/// trips the orphan-row `debug_assert!` inside [`apply_to`]. Both
/// are caught at the next startup by the integrity repair pass in
/// `recovery_check::verify_and_repair`.
pub fn apply_locally_if_needed(state: &SharedState, entry: &CatalogEntry, log_index: u64) {
    if log_index != 0 {
        return;
    }
    let catalog = state.credentials.catalog();
    apply_to(entry, catalog);
}
