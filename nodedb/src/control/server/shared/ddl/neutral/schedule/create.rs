// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE SCHEDULE` DDL handler.
//!
//! Ported from the pgwire `ddl::schedule::create` handler. The catalog path
//! (`propose_and_apply` + `log_index == 0` local registry refresh, the
//! `_schedules` CRDT-sync delta enqueue, and the `audit_record` call) is
//! preserved verbatim; only the result construction changed from pgwire
//! `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].
//!
//! Syntax:
//! ```sql
//! CREATE SCHEDULE <name> CRON '<expr>' [SCOPE LOCAL]
//!   [WITH (ALLOW_OVERLAP = false, MISSED = SKIP|CATCH_UP|QUEUE)]
//!   AS <sql_body>
//! ```

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::scheduler::cron::CronExpr;
use crate::event::scheduler::types::{MissedPolicy, ScheduleDef, ScheduleScope};

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// Parsed `CREATE SCHEDULE` request.
///
/// `scope` is "NORMAL" or "LOCAL", `missed_policy` is "SKIP", "CATCH_UP", or "QUEUE".
#[derive(Clone, Copy)]
pub struct CreateScheduleRequest<'a> {
    pub name: &'a str,
    pub cron_expr: &'a str,
    pub body_sql: &'a str,
    pub scope: &'a str,
    pub missed_policy: &'a str,
    pub allow_overlap: bool,
}

/// Handle `CREATE SCHEDULE`.
pub fn create_schedule(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: crate::types::DatabaseId,
    req: &CreateScheduleRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateScheduleRequest {
        name,
        cron_expr,
        body_sql,
        scope,
        missed_policy,
        allow_overlap,
    } = *req;
    require_tenant_admin(identity, "create schedules")?;

    let tenant_id = identity.tenant_id.as_u64();

    // Validate cron expression.
    CronExpr::parse(cron_expr).map_err(|e| DdlError {
        sqlstate: "42601".to_string(),
        message: format!("invalid cron expression: {e}"),
    })?;

    // Validate SQL body parses.
    crate::control::planner::procedural::parse_block(body_sql).map_err(|e| DdlError {
        sqlstate: "42601".to_string(),
        message: format!("schedule body parse error: {e}"),
    })?;

    // Check for duplicate.
    if state
        .schedule_registry
        .get(database_id, tenant_id, name)
        .is_some()
    {
        return Err(DdlError {
            sqlstate: "42710".to_string(),
            message: format!("schedule '{name}' already exists"),
        });
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| DdlError {
            sqlstate: "XX000".to_string(),
            message: "system clock error".to_string(),
        })?
        .as_secs();

    let target_collection = extract_target_collection(body_sql);

    let scope_enum = if scope.eq_ignore_ascii_case("LOCAL") {
        ScheduleScope::Local
    } else {
        ScheduleScope::Normal
    };

    let missed_policy_enum =
        MissedPolicy::from_str_opt(missed_policy).unwrap_or(MissedPolicy::Skip);

    let def = ScheduleDef {
        database_id: database_id.as_u64(),
        tenant_id,
        name: name.to_string(),
        cron_expr: cron_expr.to_string(),
        body_sql: body_sql.to_string(),
        scope: scope_enum,
        missed_policy: missed_policy_enum,
        allow_overlap,
        enabled: true,
        target_collection,
        owner: identity.username.clone(),
        created_at: now,
    };

    let entry = crate::control::catalog_entry::CatalogEntry::PutSchedule(Box::new(def.clone()));
    let log_index = super::super::super::catalog::propose_and_apply(state, &entry)?;
    if log_index == 0 {
        state.schedule_registry.register(def.clone());
    }

    {
        let delta_payload = zerompk::to_msgpack_vec(&def).unwrap_or_default();
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            database_id,
            collection: "_schedules".into(),
            document_id: format!("{}:{}", database_id.as_u64(), def.name),
            payload: delta_payload,
            op: crate::event::crdt_sync::types::DeltaOp::Upsert,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(delta);
    }

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE SCHEDULE {name}"),
    );

    Ok(status("CREATE SCHEDULE"))
}

/// Extract the target collection from a schedule's SQL body.
fn extract_target_collection(body_sql: &str) -> Option<String> {
    let tokens: Vec<&str> = body_sql.split_whitespace().collect();
    let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_uppercase()).collect();

    const TARGET_KEYWORDS: &[&str] = &["FROM", "INTO", "UPDATE", "ON"];
    const SKIP_AFTER: &[&str] = &[
        "BEGIN",
        "END",
        "EACH",
        "ROW",
        "STATEMENT",
        "RETURN",
        "IF",
        "THEN",
        "ELSE",
        "LOOP",
        "WHILE",
        "DECLARE",
        "SET",
        "WHERE",
        "AND",
        "OR",
        "NOT",
        "NULL",
        "TRUE",
        "FALSE",
    ];

    for (i, token) in upper_tokens.iter().enumerate() {
        if TARGET_KEYWORDS.contains(&token.as_str()) && i + 1 < tokens.len() {
            let candidate = tokens[i + 1]
                .trim_end_matches([';', '(', ')'])
                .to_lowercase();
            if !candidate.is_empty()
                && !SKIP_AFTER.contains(&candidate.to_uppercase().as_str())
                && !candidate.starts_with('$')
                && !candidate.starts_with('\'')
            {
                return Some(candidate);
            }
        }
    }
    None
}
