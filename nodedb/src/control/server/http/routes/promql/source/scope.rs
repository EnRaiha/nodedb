// SPDX-License-Identifier: BUSL-1.1

//! Who a PromQL read runs as, and against which database.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// The principal, database and deadline every scan of one PromQL request
/// runs under. The database is the request's resolved database, never the
/// identity's default.
pub(crate) struct ReadScope<'a> {
    pub state: &'a SharedState,
    pub identity: &'a AuthenticatedIdentity,
    pub database_id: DatabaseId,
    /// One deadline for the whole request, shared by every scan it runs.
    pub deadline: StatementDeadline,
}

impl<'a> ReadScope<'a> {
    pub(crate) fn new(
        state: &'a SharedState,
        identity: &'a AuthenticatedIdentity,
        database_id: DatabaseId,
    ) -> Self {
        Self {
            state,
            identity,
            database_id,
            deadline: StatementDeadline::of_statement(state),
        }
    }
}
