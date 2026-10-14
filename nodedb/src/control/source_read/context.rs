// SPDX-License-Identifier: BUSL-1.1

//! Who reads a write's source relation, and where.

use nodedb_types::{DatabaseId, TenantId};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::TxnId;

/// The scope one source read runs in.
pub(crate) struct SourceReadContext<'a> {
    pub state: &'a SharedState,
    /// The statement's identity. Catalog and continuous-aggregate rows are
    /// produced for it.
    pub identity: &'a AuthenticatedIdentity,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    /// The session transaction the statement runs in. `Some` folds its
    /// staged writes into the read. `None` reads committed rows only.
    pub txn_id: Option<TxnId>,
}
