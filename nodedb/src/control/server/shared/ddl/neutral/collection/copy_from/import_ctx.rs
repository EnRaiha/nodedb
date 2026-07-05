// SPDX-License-Identifier: BUSL-1.1

//! Shared context for the `COPY FROM` per-format row-import helpers.
//!
//! Bundles the invariant dependencies (`state`, `identity`, `tenant_id`,
//! `database_id`, transaction scope) that every import helper forwards to
//! `plan_and_dispatch`, so each helper takes a single `&ImportCtx` plus its
//! format-specific arguments instead of a long positional list.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;

/// Invariant dependencies threaded into each row-import helper.
pub(super) struct ImportCtx<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: nodedb_types::TenantId,
    pub database_id: nodedb_types::DatabaseId,
    pub txn_ctx: &'a DmlTxnCtx<'a>,
}
