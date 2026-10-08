// SPDX-License-Identifier: BUSL-1.1

//! Simple-query and prepared-statement entry points for planned SQL execution.

use pgwire::api::portal::Format;
use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::SessionId;
use crate::types::TenantId;

use super::super::core::NodeDbPgHandler;
use super::result_shaping::ResultShaping;

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block, writes are buffered until COMMIT while
    /// reads execute immediately. Results honour `requested`, the client's
    /// result formats: text for the simple-query protocol.
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        requested: &Format,
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(
            identity,
            sql,
            tenant_id,
            session_id,
            &[],
            ResultShaping {
                projection: None,
                formats: requested,
            },
        )
        .await
    }

    /// Execute planned SQL with bound parameters (prepared statement path).
    pub(in crate::control::server::pgwire::handler) async fn execute_planned_sql_with_params(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        session_id: SessionId,
        params: &[nodedb_sql::ParamValue],
        shaping: ResultShaping<'_>,
    ) -> PgWireResult<Vec<Response>> {
        self.execute_planned_sql_inner(identity, sql, tenant_id, session_id, params, shaping)
            .await
    }
}
