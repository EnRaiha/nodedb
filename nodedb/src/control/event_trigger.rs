// SPDX-License-Identifier: BUSL-1.1

//! Background event trigger processor.
//!
//! Subscribes to the ChangeStream and evaluates DEFINE EVENT triggers
//! for each write. When a WHEN condition matches, the THEN action is
//! executed as a SQL statement via the Control Plane query path.

use nodedb_types::DatabaseId;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::control::change_stream::{ChangeEvent, ChangeOperation};
use crate::control::planner::context::QueryContext;
use crate::control::state::SharedState;
use crate::types::TraceId;

/// Spawn the event trigger processor as a background tokio task
/// registered with the shutdown loop registry. The processor
/// exits on shutdown signal or when the change-stream
/// broadcast channel closes.
pub fn spawn_event_trigger_processor(shared: Arc<SharedState>) {
    let mut subscription = shared.change_stream.subscribe(None, None);
    let registry = Arc::clone(&shared.loop_registry);
    let watch = Arc::clone(&shared.shutdown);

    crate::control::shutdown::spawn_loop(
        &registry,
        &watch,
        "event_trigger_processor",
        move |mut shutdown| async move {
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => {
                        debug!("event trigger processor shutting down");
                        break;
                    }
                    msg = subscription.recv_filtered() => match msg {
                        Ok(event) => process_event(&shared, &event).await,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!(
                                lagged = n,
                                "event trigger processor fell behind; skipped {n} events"
                            );
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            debug!("change stream closed; event trigger processor stopping");
                            break;
                        }
                    },
                }
            }
        },
    );
}

/// Process a single change event against all matching EventDefinitions.
async fn process_event(shared: &SharedState, event: &ChangeEvent) {
    let catalog = shared.credentials.catalog();

    let coll = match catalog.get_collection(
        DatabaseId::DEFAULT,
        event.tenant_id.as_u64(),
        &event.collection,
    ) {
        Ok(Some(c)) => c,
        _ => return,
    };

    if coll.event_defs.is_empty() {
        return;
    }

    let op_str = event.operation.as_str();

    for event_def in &coll.event_defs {
        let when_upper = event_def.when_condition.to_uppercase();
        let matches = match when_upper.as_str() {
            "INSERT" => event.operation == ChangeOperation::Insert,
            "UPDATE" => event.operation == ChangeOperation::Update,
            "DELETE" => event.operation == ChangeOperation::Delete,
            "ANY" | "*" | "TRUE" => true,
            compound => compound.contains(op_str),
        };

        if !matches {
            continue;
        }

        debug!(
            event = event_def.name,
            collection = event.collection,
            document_id = event.document_id,
            operation = op_str,
            action = event_def.then_action,
            "event trigger fired"
        );

        // Execute the THEN action as SQL.
        execute_then_action(shared, event, &event_def.then_action, &event_def.name).await;

        shared.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(event.tenant_id),
            "event_trigger",
            &format!(
                "event '{}' on '{}': doc={}, op={}, action={}",
                event_def.name, event.collection, event.document_id, op_str, event_def.then_action
            ),
        );
    }
}

fn contains_trigger_placeholder(text: &str) -> bool {
    ["$document_id", "$collection", "$operation"]
        .iter()
        .any(|placeholder| text.contains(placeholder))
}

fn quoted_region_end(sql: &str, start: usize, quote: u8, backslash_escapes: bool) -> Option<usize> {
    let bytes = sql.as_bytes();
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        if backslash_escapes && bytes[cursor] == b'\\' {
            cursor = cursor.checked_add(2)?;
            continue;
        }
        if bytes[cursor] == quote {
            if bytes.get(cursor + 1) == Some(&quote) {
                cursor += 2;
                continue;
            }
            return Some(cursor + 1);
        }
        cursor += 1;
    }
    None
}

fn dollar_delimiter(sql: &str, start: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'$') {
        return None;
    }
    let mut cursor = start + 1;
    while bytes
        .get(cursor)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        cursor += 1;
    }
    (bytes.get(cursor) == Some(&b'$')).then(|| &sql[start..=cursor])
}

fn canonical_trigger_template_sql(fragment: &str) -> &str {
    fragment
}

fn render_then_action_sql(action: &str, event: &ChangeEvent) -> Result<String, &'static str> {
    let mut rendered = String::with_capacity(action.len());
    let mut cursor = 0;
    while cursor < action.len() {
        let rest = &action[cursor..];
        if rest.starts_with("--") {
            let end = rest
                .find('\n')
                .map_or(action.len(), |offset| cursor + offset);
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with("/*") {
            let bytes = action.as_bytes();
            let mut end = cursor + 2;
            let mut depth = 1usize;
            while end < bytes.len() && depth > 0 {
                if bytes[end..].starts_with(b"/*") {
                    depth += 1;
                    end += 2;
                } else if bytes[end..].starts_with(b"*/") {
                    depth -= 1;
                    end += 2;
                } else {
                    end += 1;
                }
            }
            if depth != 0 {
                return Err("unterminated block comment in event trigger action");
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with('\'') || rest.starts_with('"') {
            let quote = action.as_bytes()[cursor];
            let backslash_escapes = quote == b'\''
                && cursor > 0
                && matches!(action.as_bytes()[cursor - 1], b'E' | b'e')
                && (cursor == 1
                    || !matches!(
                        action.as_bytes()[cursor - 2],
                        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$'
                    ));
            let end = quoted_region_end(action, cursor, quote, backslash_escapes)
                .ok_or("unterminated quoted region in event trigger action")?;
            if contains_trigger_placeholder(&action[cursor..end]) {
                return Err("event trigger placeholders must not be manually quoted");
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if let Some(delimiter) = dollar_delimiter(action, cursor) {
            let body_start = cursor + delimiter.len();
            let relative_end = action[body_start..]
                .find(delimiter)
                .ok_or("unterminated dollar quote in event trigger action")?;
            let end = body_start + relative_end + delimiter.len();
            if contains_trigger_placeholder(&action[cursor..end]) {
                return Err("event trigger placeholders must not be manually quoted");
            }
            rendered.push_str(canonical_trigger_template_sql(&action[cursor..end]));
            cursor = end;
            continue;
        }
        if rest.starts_with("$document_id") {
            rendered.push_str(&::nodedb_types::quote_literal(&event.document_id));
            cursor += "$document_id".len();
        } else if rest.starts_with("$collection") {
            rendered.push_str(&::nodedb_types::quote_ident(&event.collection));
            cursor += "$collection".len();
        } else if rest.starts_with("$operation") {
            let operation = event.operation.as_str();
            rendered.push_str(&::nodedb_types::quote_literal(operation));
            cursor += "$operation".len();
        } else {
            let ch = rest
                .chars()
                .next()
                .ok_or("invalid UTF-8 boundary in event trigger action")?;
            rendered.push(ch);
            cursor += ch.len_utf8();
        }
    }
    Ok(rendered)
}

/// Execute a THEN action string as SQL.
///
/// Template variables are substituted as complete canonical SQL tokens before
/// execution and therefore must not be manually quoted:
/// - `$document_id` → a string literal containing the affected document ID
/// - `$collection` → a quoted collection identifier
/// - `$operation` → an `INSERT`, `UPDATE`, or `DELETE` string literal
async fn execute_then_action(
    shared: &SharedState,
    event: &ChangeEvent,
    action: &str,
    trigger_name: &str,
) {
    let sql = match render_then_action_sql(action, event) {
        Ok(sql) => sql,
        Err(detail) => {
            warn!(
                trigger = trigger_name,
                error = detail,
                "event trigger action rejected"
            );
            return;
        }
    };

    let query_ctx = QueryContext::for_state(shared);

    match query_ctx
        .plan_sql(&sql, event.tenant_id, crate::types::DatabaseId::DEFAULT)
        .await
    {
        Ok((tasks, _output_schema)) => {
            for task in tasks {
                match crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    shared,
                    task.tenant_id,
                    task.database_id,
                    task.vshard_id,
                    task.plan,
                    TraceId::ZERO,
                )
                .await
                {
                    Ok(_) => {
                        info!(
                            trigger = trigger_name,
                            sql = sql,
                            "event trigger action executed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            trigger = trigger_name,
                            sql = sql,
                            error = %e,
                            "event trigger action dispatch failed"
                        );
                    }
                }
            }
        }
        Err(e) => {
            warn!(
                trigger = trigger_name,
                sql = sql,
                error = %e,
                "event trigger action plan failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::render_then_action_sql;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation};
    use crate::types::{Lsn, TenantId};

    fn hostile_event() -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "odd\"; DROP TABLE audit; --".into(),
            document_id: "doc'; DELETE FROM audit; --".into(),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    #[test]
    fn trigger_placeholders_render_as_canonical_sql_tokens() {
        let sql = render_then_action_sql(
            "INSERT INTO $collection (id, op) VALUES ($document_id, $operation)",
            &hostile_event(),
        )
        .expect("render trigger SQL");
        assert_eq!(
            sql,
            "INSERT INTO \"odd\"\"; DROP TABLE audit; --\" (id, op) VALUES ('doc''; DELETE FROM audit; --', 'INSERT')"
        );
    }

    #[test]
    fn trigger_placeholders_reject_manual_quoting_and_preserve_opaque_comments() {
        assert!(render_then_action_sql("SELECT '$document_id'", &hostile_event()).is_err());
        assert_eq!(
            render_then_action_sql("SELECT 1 -- $document_id\n", &hostile_event())
                .expect("comment is opaque"),
            "SELECT 1 -- $document_id\n"
        );
    }

    #[test]
    fn trigger_renderer_preserves_escape_string_literal_boundaries() {
        assert_eq!(
            render_then_action_sql(r"SELECT E'escaped \' quote'", &hostile_event())
                .expect("escape string is opaque"),
            r"SELECT E'escaped \' quote'"
        );
        assert_eq!(
            render_then_action_sql(r"SELECT e'escaped \' $document_id'", &hostile_event()),
            Err("event trigger placeholders must not be manually quoted")
        );
    }
}
