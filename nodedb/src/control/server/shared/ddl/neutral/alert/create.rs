// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `CREATE ALERT` DDL handler.
//!
//! Ported from the pgwire `ddl::alert::create` handler. The catalog path
//! (DIRECT `catalog.put_alert_rule(&def)` write), the `_alert_rules` CRDT-sync
//! delta enqueue, the in-memory registry registration, and the `audit_record`
//! call are preserved verbatim; only the result construction changed from
//! pgwire `Response` / `PgWireError` to the protocol-neutral [`DdlResult`] /
//! [`DdlError`].

use nodedb_types::DatabaseId;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::event::alert::types::{AlertCondition, AlertDef, CompareOp, NotifyTarget};

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::{require_tenant_admin, status};

/// CRDT collection name for alert rule sync between Origin and Lite.
const ALERT_RULES_CRDT_COLLECTION: &str = "_alert_rules";

fn err(sqlstate: &str, message: String) -> DdlError {
    DdlError {
        sqlstate: sqlstate.to_string(),
        message,
    }
}

/// Parsed `CREATE ALERT` request — fields extracted by the nodedb-sql parser.
///
/// `condition_raw` is the raw condition text (e.g. `"AVG(temperature) > 90.0"`).
/// `notify_targets_raw` is the raw NOTIFY section text.
#[derive(Clone, Copy)]
pub struct CreateAlertRequest<'a> {
    pub name: &'a str,
    pub collection: &'a str,
    pub where_filter: Option<&'a str>,
    pub condition_raw: &'a str,
    pub group_by: &'a [String],
    pub window_raw: &'a str,
    pub fire_after: u32,
    pub recover_after: u32,
    pub severity: &'a str,
    pub notify_targets_raw: &'a str,
    /// Session database the alert is created in. Scopes catalog lookups,
    /// the in-memory registry, and background eval-loop dispatch routing.
    pub database_id: DatabaseId,
}

/// Handle `CREATE ALERT`. Converts raw strings to `AlertCondition` and `Vec<NotifyTarget>`.
pub fn create_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &CreateAlertRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let CreateAlertRequest {
        name,
        collection,
        where_filter,
        condition_raw,
        group_by,
        window_raw,
        fire_after,
        recover_after,
        severity,
        notify_targets_raw,
        database_id,
    } = *req;
    require_tenant_admin(identity, "create alerts")?;

    let tenant_id = identity.tenant_id.as_u64();

    // Validate collection exists.
    if state
        .credentials
        .catalog()
        .get_collection(database_id, tenant_id, collection)
        .ok()
        .flatten()
        .is_none()
    {
        return Err(err(
            "42P01",
            format!("collection '{collection}' does not exist"),
        ));
    }

    // Check for duplicate alert name.
    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, name)
        .is_some()
    {
        return Err(err("42710", format!("alert '{name}' already exists")));
    }

    // Parse condition from raw string.
    let condition = parse_condition_raw(condition_raw)?;

    // Parse WINDOW duration.
    let window_ms = nodedb_types::kv_parsing::parse_interval_to_ms(window_raw)
        .map_err(|e| err("42601", format!("invalid window duration: {e}")))?
        as u64;

    // Parse NOTIFY targets.
    let notify_targets = parse_notify_targets_raw(notify_targets_raw)?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| err("XX000", "system clock error".to_string()))?
        .as_secs();

    let def = AlertDef {
        database_id: database_id.as_u64(),
        tenant_id,
        name: name.to_string(),
        collection: collection.to_string(),
        where_filter: where_filter.map(|s| s.to_string()),
        condition,
        group_by: group_by.to_vec(),
        window_ms,
        fire_after,
        recover_after,
        severity: severity.to_string(),
        notify_targets,
        enabled: true,
        owner: identity.username.clone(),
        created_at: now,
    };

    // Persist to catalog.
    let catalog = state.credentials.catalog();

    catalog
        .put_alert_rule(&def)
        .map_err(|e| err("XX000", format!("catalog write: {e}")))?;

    // Emit CRDT sync delta for Lite visibility.
    {
        let delta_payload = zerompk::to_msgpack_vec(&def).unwrap_or_default();
        let delta = crate::event::crdt_sync::types::OutboundDelta {
            database_id,
            collection: ALERT_RULES_CRDT_COLLECTION.into(),
            document_id: def.name.clone(),
            payload: delta_payload,
            op: crate::event::crdt_sync::types::DeltaOp::Upsert,
            lsn: 0,
            tenant_id,
            peer_id: state.node_id,
            sequence: 0,
        };
        state.crdt_sync_delivery.enqueue(delta);
    }

    state.alert_registry.register(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("CREATE ALERT {name}"),
    );

    tracing::info!(name, collection, "alert rule created");

    Ok(status("CREATE ALERT"))
}

/// Parse `agg_func(column) op threshold` from raw condition string.
fn parse_condition_raw(raw: &str) -> Result<AlertCondition, DdlError> {
    let s = raw.trim();
    let open = s.find('(').ok_or_else(|| {
        err(
            "42601",
            "expected agg_func(column) in CONDITION".to_string(),
        )
    })?;
    let close = s
        .find(')')
        .ok_or_else(|| err("42601", "missing ')' in CONDITION".to_string()))?;

    let agg_func = s[..open].trim().to_lowercase();
    let column = s[open + 1..close].trim().to_lowercase();
    let remainder = s[close + 1..].trim();

    let (op_str, rest) = if remainder.starts_with(">=")
        || remainder.starts_with("<=")
        || remainder.starts_with("!=")
        || remainder.starts_with("<>")
    {
        (&remainder[..2], remainder[2..].trim())
    } else if remainder.starts_with('>') || remainder.starts_with('<') || remainder.starts_with('=')
    {
        (&remainder[..1], remainder[1..].trim())
    } else {
        return Err(err(
            "42601",
            format!("expected comparison operator in CONDITION: {remainder}"),
        ));
    };

    let op = CompareOp::parse(op_str)
        .ok_or_else(|| err("42601", format!("unknown operator: {op_str}")))?;

    let threshold: f64 = rest
        .parse()
        .map_err(|_| err("42601", format!("expected numeric threshold: {rest}")))?;

    Ok(AlertCondition {
        agg_func,
        column,
        op,
        threshold,
    })
}

/// Parse NOTIFY targets from raw NOTIFY section text.
fn parse_notify_targets_raw(raw: &str) -> Result<Vec<NotifyTarget>, DdlError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let mut targets = Vec::new();
    for part in split_top_level_commas(raw) {
        let part = part.trim().trim_end_matches(';').trim();
        if part.is_empty() {
            continue;
        }
        if part
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("TOPIC "))
        {
            let name = extract_inner_quoted(part, 6)?;
            targets.push(NotifyTarget::Topic { name });
        } else if part
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("WEBHOOK "))
        {
            let url = extract_inner_quoted(part, 8)?;
            targets.push(NotifyTarget::Webhook { url });
        } else if part
            .get(..12)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("INSERT INTO "))
        {
            let after_insert = part.get(12..).ok_or_else(|| DdlError {
                sqlstate: "42601".to_string(),
                message: "expected INSERT INTO target".to_string(),
            })?;
            let (table, columns) = parse_insert_target(after_insert)?;
            targets.push(NotifyTarget::InsertInto { table, columns });
        }
    }
    Ok(targets)
}

fn split_top_level_commas(s: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut depth = 0usize;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                results.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < s.len() {
        results.push(&s[start..]);
    }
    results
}

fn extract_inner_quoted(s: &str, offset: usize) -> Result<String, DdlError> {
    let after = s[offset..].trim_start();
    let start = after
        .find('\'')
        .ok_or_else(|| err("42601", "expected quoted value".to_string()))?;
    let end = after[start + 1..]
        .find('\'')
        .ok_or_else(|| err("42601", "missing closing quote".to_string()))?;
    Ok(after[start + 1..start + 1 + end].to_string())
}

fn parse_insert_target(s: &str) -> Result<(String, Vec<String>), DdlError> {
    let s = s.trim();
    if let Some(paren_start) = s.find('(') {
        let table = s[..paren_start].trim().to_lowercase();
        let paren_end = s
            .rfind(')')
            .ok_or_else(|| err("42601", "missing ')' in INSERT INTO target".to_string()))?;
        if !s[paren_end + 1..].trim().is_empty() {
            return Err(err(
                "42601",
                "unexpected text after INSERT INTO column list".to_string(),
            ));
        }
        let cols: Vec<String> = s[paren_start + 1..paren_end]
            .split(',')
            .map(|c| c.trim().to_lowercase())
            .filter(|c| !c.is_empty())
            .collect();
        if table.is_empty() || cols.len() != 7 {
            return Err(err(
                "42601",
                "alert INSERT INTO target requires a table and exactly seven columns".to_string(),
            ));
        }
        Ok((table, cols))
    } else {
        let table = s.split_whitespace().next().unwrap_or(s).to_lowercase();
        if table.is_empty() {
            return Err(err(
                "42601",
                "alert INSERT INTO target requires a table".to_string(),
            ));
        }
        Ok((table, Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::parse_insert_target;

    #[test]
    fn insert_target_requires_exact_alert_history_arity() {
        assert!(parse_insert_target("history").is_ok());
        assert!(parse_insert_target("history (a,b,c,d,e,f,g)").is_ok());
        assert!(parse_insert_target("history (a,b)").is_err());
        assert!(parse_insert_target("history (a,b,c,d,e,f,g) trailing").is_err());
    }
}
