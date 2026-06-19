// SPDX-License-Identifier: BUSL-1.1

//! `SHOW ALERTS` and `SHOW ALERT STATUS ON <name>` DDL handlers.

use std::sync::Arc;

use futures::stream;
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{int8_field, sqlstate_error, text_field};

/// SHOW ALERTS — list all alert rules for the tenant.
pub fn show_alerts(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();
    let alerts = state
        .alert_registry
        .list_for_tenant_in_database(database_id.as_u64(), tenant_id);

    let schema = Arc::new(vec![
        text_field("name"),
        text_field("collection"),
        text_field("condition"),
        text_field("group_by"),
        text_field("window"),
        int8_field("fire_after"),
        int8_field("recover_after"),
        text_field("severity"),
        text_field("enabled"),
        int8_field("notify_count"),
    ]);

    let mut rows = Vec::new();
    for alert in &alerts {
        let condition_str = format!(
            "{}({}) {} {}",
            alert.condition.agg_func,
            alert.condition.column,
            alert.condition.op.as_sql(),
            alert.condition.threshold,
        );
        let group_by_str = if alert.group_by.is_empty() {
            "-".to_string()
        } else {
            alert.group_by.join(", ")
        };
        let window_str = format_duration_ms(alert.window_ms);

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(&alert.name)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&alert.collection)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&condition_str)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&group_by_str)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&window_str)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(alert.fire_after as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(alert.recover_after as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&alert.severity)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&alert.enabled.to_string())
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(alert.notify_targets.len() as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

/// SHOW ALERT STATUS ON <name> — per-group active/cleared state.
pub fn show_alert_status(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: nodedb_types::DatabaseId,
    name: &str,
) -> PgWireResult<Vec<Response>> {
    let tenant_id = identity.tenant_id.as_u64();
    let name = name.to_lowercase();

    // Verify alert exists.
    if state
        .alert_registry
        .get(database_id.as_u64(), tenant_id, &name)
        .is_none()
    {
        return Err(sqlstate_error(
            "42704",
            &format!("alert '{name}' does not exist"),
        ));
    }

    let states = state.alert_hysteresis.list_states(tenant_id, &name);

    let schema = Arc::new(vec![
        text_field("group_key"),
        text_field("status"),
        int8_field("consecutive_fire"),
        int8_field("consecutive_recover"),
        text_field("last_value"),
        int8_field("fired_at"),
        int8_field("cleared_at"),
    ]);

    let mut rows = Vec::new();
    for (group_key, group_state) in &states {
        let status_str = match group_state.status {
            crate::event::alert::AlertStatus::Active => "ACTIVE",
            crate::event::alert::AlertStatus::Cleared => "CLEARED",
        };
        let last_value = group_state
            .last_value
            .map(|v| format!("{v:.4}"))
            .unwrap_or_else(|| "-".to_string());

        let mut encoder = DataRowEncoder::new(schema.clone());
        encoder
            .encode_field(group_key)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&status_str)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(group_state.consecutive_fire as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&(group_state.consecutive_recover as i64))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&last_value)
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&group_state.fired_at.map(|t| t as i64).unwrap_or(0))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        encoder
            .encode_field(&group_state.cleared_at.map(|t| t as i64).unwrap_or(0))
            .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
        rows.push(Ok(encoder.take_row()));
    }

    Ok(vec![Response::Query(QueryResponse::new(
        schema,
        stream::iter(rows),
    ))])
}

fn format_duration_ms(ms: u64) -> String {
    const MINUTE: u64 = 60_000;
    const HOUR: u64 = 3_600_000;
    const DAY: u64 = 86_400_000;

    if ms.is_multiple_of(DAY) {
        format!("{}d", ms / DAY)
    } else if ms.is_multiple_of(HOUR) {
        format!("{}h", ms / HOUR)
    } else if ms.is_multiple_of(MINUTE) {
        format!("{}m", ms / MINUTE)
    } else if ms.is_multiple_of(1_000) {
        format!("{}s", ms / 1_000)
    } else {
        format!("{ms}ms")
    }
}
