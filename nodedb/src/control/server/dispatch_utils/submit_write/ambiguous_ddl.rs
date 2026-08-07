// SPDX-License-Identifier: BUSL-1.1

//! The ambiguous-outcome guard for an enqueued Array DDL, split out of
//! `funnel.rs` because it is a self-contained decision with no ordering
//! dependency on the guard/append/enqueue/await/durability sequence — it only
//! runs from the funnel's post-enqueue error branches.

use crate::control::state::SharedState;

/// An enqueued Array CREATE/ALTER has an ambiguous outcome if the response
/// times out, closes, or overflows collection. Its Data-Plane mutation may
/// already exist, so rolling back the catalog would manufacture a ghost engine.
/// Preserve the transition and stop the node through the canonical watch; boot
/// recovery will reconcile from the durable catalog and WAL rather than serving
/// a potentially split Control/Data-plane view.
pub(super) fn preserve_ambiguous_array_ddl(
    shared: &SharedState,
    transition: &crate::control::array_catalog::ddl::AuthorizedDdlTransition,
) {
    if !transition.preserves_on_ambiguous_apply() {
        return;
    }
    if let Err(error) = transition.finalize(shared) {
        tracing::error!(error = %error, "array DDL ambiguous after enqueue; finalization failed before fail-stop");
    }
    shared.shutdown.signal();
}
