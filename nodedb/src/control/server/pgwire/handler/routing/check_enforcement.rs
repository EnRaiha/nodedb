// SPDX-License-Identifier: BUSL-1.1

//! CHECK constraint and enum-label enforcement for the pgwire SQL path.
//!
//! The enforcement is protocol-neutral and lives in
//! `shared::check_constraint::statement`. These wrappers map its verdict to a
//! pgwire error.

use nodedb_types::DatabaseId;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::security::auth_context::AuthContext;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::check_constraint::{
    enforce_statement_checks, enforce_statement_enum_labels,
};
use crate::control::server::shared::ddl::result::DdlError;
use crate::types::TenantId;

use super::super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Enforce general CHECK constraints before planning INSERT or UPDATE SQL.
    /// `txn_id` names the session's open transaction: an UPDATE's current row
    /// is read as it left it.
    pub(super) async fn enforce_check_constraints_if_needed(
        &self,
        sql: &str,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
        database_id: DatabaseId,
        auth: &AuthContext,
        txn_id: Option<crate::types::TxnId>,
    ) -> PgWireResult<()> {
        enforce_statement_checks(
            &self.state,
            identity,
            tenant_id,
            database_id,
            auth,
            txn_id,
            sql,
        )
        .await
        .map_err(pgwire_err)
    }

    /// Validate enum-typed column values against the custom type registry.
    pub(super) fn enforce_enum_labels_if_needed(
        &self,
        sql: &str,
        tenant_id: TenantId,
        database_id: DatabaseId,
    ) -> PgWireResult<()> {
        enforce_statement_enum_labels(&self.state, tenant_id, database_id, sql).map_err(pgwire_err)
    }
}

fn pgwire_err(error: DdlError) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        error.sqlstate,
        error.message,
    )))
}
