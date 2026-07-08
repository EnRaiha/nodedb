// SPDX-License-Identifier: BUSL-1.1

//! Sidecar value type for Calvin apply results (RETURNING rows).

use crate::bridge::envelope::Response;

/// The applied Data-Plane result for one completed Calvin transaction, carried
/// via [`SharedState::calvin_apply_results`] from the per-vShard scheduler to
/// the coordinator's completion path.
///
/// Under collection-level sharding a single Calvin transaction has exactly one
/// RETURNING-bearing participant (a predicate DELETE/UPDATE resolves to one data
/// shard; dual-homed edges never carry RETURNING, and cross-shard writes inside
/// an explicit transaction are rejected). If a second participant ever deposits
/// for the same `TxnId`, the sidecar records [`Conflict`](Self::Conflict) instead
/// of silently keeping one shard's rows: the coordinator then fails the statement
/// loudly rather than returning a partial cross-shard union. That arm is
/// unreachable today but safe — no silent partial can escape.
///
/// [`SharedState::calvin_apply_results`]: crate::control::state::SharedState::calvin_apply_results
pub enum CalvinApplyResult {
    /// The sole RETURNING-bearing participant's applied response.
    Single(Response),
    /// Two or more participants deposited for one `TxnId` — a cross-shard
    /// RETURNING union, which is unsupported. Drained as a typed error.
    Conflict,
}
