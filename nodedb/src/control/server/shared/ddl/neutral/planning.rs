// SPDX-License-Identifier: BUSL-1.1

//! RLS-aware planning and authorization for neutral DDL readback queries.

use std::sync::Arc;

use nodedb_types::DatabaseId;

use crate::control::planner::context::{PlanSecurityContext, PlanSqlWithRlsParams, QueryContext};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::shared::authorization::{AuthorizedTaskSet, authorize_task_set};
use crate::control::server::shared::ddl::result::DdlError;
use crate::control::state::SharedState;

/// Plan a neutral DDL readback query with RLS and authorize every resulting task.
///
/// Neutral DDL handlers reconstruct a small number of internal scans.  They must
/// use the same RLS-aware planning and final task authorization boundary as
/// external query transports before dispatching those scans to the Data Plane.
pub async fn plan_authorized_sql(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Result<(AuthorizedTaskSet, OutputSchema), DdlError> {
    let auth = crate::control::server::session_auth::build_auth_context(identity);
    let permission_cache = state.permission_cache.read().await;
    let sec = PlanSecurityContext {
        identity,
        auth: &auth,
        rls_store: &state.rls,
        permissions: &state.permissions,
        roles: &state.roles,
        permission_cache: Some(&*permission_cache),
    };
    let query_ctx = QueryContext::for_state(state);
    let (tasks, output_schema) = query_ctx
        .plan_sql_with_rls(PlanSqlWithRlsParams {
            sql,
            tenant_id: identity.tenant_id,
            database_id,
            sec: &sec,
        })
        .await
        .map_err(|error| DdlError {
            sqlstate: "42601".to_string(),
            message: format!("query planning failed: {error}"),
        })?;

    let emitter = ArcAuditEmitter(Arc::clone(&state.audit));
    let authorized =
        authorize_task_set(identity, &tasks, &state.permissions, &state.roles, &emitter).map_err(
            |error| DdlError {
                sqlstate: nodedb_types::error::sqlstate::INSUFFICIENT_PRIVILEGE.to_string(),
                message: error.resource().to_string(),
            },
        )?;

    Ok((authorized, output_schema))
}
