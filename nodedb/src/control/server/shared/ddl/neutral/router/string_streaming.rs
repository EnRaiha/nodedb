// SPDX-License-Identifier: BUSL-1.1

//! String-recognized streaming DDL arms: schedule/alert SHOW, change streams,
//! consumer groups, topics, stream/topic consumption, and pub/sub subscribe.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::alert;
use super::super::change_stream;
use super::super::consumer_group;
use super::super::schedule;
use super::super::stream_select;
use super::super::topic;
use super::super::topic_subscribe;

pub(super) async fn try_string(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // Schedule SHOW. `SHOW SCHEDULE HISTORY <name>` parses into a typed
    // `AutomationStmt::ShowScheduleHistory` and `SHOW SCHEDULES` into
    // `AutomationStmt::ShowSchedules`, but the pgwire router dispatched both from
    // the raw token slice by string prefix (the `SHOW SCHEDULE` prefix also
    // captures the bare-singular `SHOW SCHEDULE` input, which parses into no
    // typed variant). Replicate that exactly here, before the parse gate, so the
    // prefix recognition and `parts.get(3)` name extraction stay byte-identical.
    if upper.starts_with("SHOW SCHEDULE HISTORY ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(3).copied().unwrap_or("");
        return Some(schedule::show_schedule_history(
            state,
            identity,
            database_id,
            name,
        ));
    }
    if upper.starts_with("SHOW SCHEDULE") {
        return Some(schedule::show_schedules(state, identity, database_id));
    }

    // Alert SHOW. `SHOW ALERT STATUS <name>` parses into a typed
    // `AutomationStmt::ShowAlertStatus` and `SHOW ALERTS` into
    // `AutomationStmt::ShowAlerts`, but the pgwire admin router dispatched both
    // from the raw token slice by string prefix (the `SHOW ALERT` prefix also
    // captures the bare-singular `SHOW ALERT` input, which parses into
    // `ShowAlerts`). Replicate that exactly here, before the parse gate, so the
    // prefix recognition (STATUS checked first) and the `parts.get(4)` name
    // extraction (name after `ON`) stay byte-identical.
    if upper.starts_with("SHOW ALERT STATUS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let name = parts.get(4).copied().unwrap_or("");
        return Some(alert::show_alert_status(state, identity, database_id, name));
    }
    if upper.starts_with("SHOW ALERT") {
        return Some(alert::show_alerts(state, identity, database_id));
    }

    // Change streams: `SHOW CHANGE STREAM(S)`. This parses into a typed
    // `StreamViewStmt::ShowChangeStreams`, but the pgwire router dispatched it
    // from the raw SQL by string prefix (the `SHOW CHANGE STREAM` prefix, which
    // captures both the plural `SHOW CHANGE STREAMS` and the bare-singular
    // input). Replicate that exactly here, before the parse gate, so the prefix
    // recognition stays byte-identical.
    if upper.starts_with("SHOW CHANGE STREAM") {
        return Some(change_stream::show_change_streams(
            state,
            identity,
            database_id,
        ));
    }

    // Consumer groups: `SHOW CONSUMER GROUPS ON <stream>`, `SHOW PARTITIONS ON
    // <stream>`, and `COMMIT OFFSET(S) …`. The pgwire streaming router dispatched
    // all four by string prefix from the raw token slice. `SHOW CONSUMER GROUPS`
    // parses into a typed `StreamViewStmt::ShowConsumerGroups`, but the pgwire
    // string dispatch claimed it before any typed arm ran; `SHOW PARTITIONS` and
    // `COMMIT OFFSET(S)` parse into no typed variant at all. Replicate that
    // exactly here, before the parse gate, so the prefix recognition and the
    // `parts`-based syntax messages stay byte-identical. (`SHOW PARTITIONS ` also
    // shadows the timeseries `show_partitions` handler exactly as the pgwire
    // streaming router — which ran before engine_ops — did.)
    if upper.starts_with("SHOW CONSUMER GROUPS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::show_consumer_groups(
            state,
            identity,
            database_id,
            &parts,
        ));
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::show_partitions(
            state,
            identity,
            database_id,
            &parts,
        ));
    }
    if upper.starts_with("COMMIT OFFSET ") || upper.starts_with("COMMIT OFFSETS ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(consumer_group::commit_offset(state, identity, database_id, &parts).await);
    }

    // Topics: `CREATE TOPIC`, `DROP TOPIC`, `SHOW TOPIC(S)`, and `PUBLISH TO`.
    // None of these parse into any typed AST variant — the pgwire streaming
    // router dispatched all four by string prefix from the raw token slice /
    // SQL. Replicate that exactly here, before the parse gate, so the prefix
    // recognition (including the trailing-space-less `SHOW TOPIC`, which
    // captures both `SHOW TOPICS` and the bare-singular input) and the
    // `parts`-based syntax messages stay byte-identical.
    if upper.starts_with("CREATE TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::create_topic(state, identity, database_id, &parts, sql).await);
    }
    if upper.starts_with("DROP TOPIC ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic::drop_topic(state, identity, database_id, &parts).await);
    }
    if upper.starts_with("SHOW TOPIC") {
        return Some(topic::show_topics(state, identity, database_id));
    }
    if upper.starts_with("PUBLISH TO ") {
        return Some(topic::handle_publish(state, identity, database_id, sql).await);
    }

    // Stream consumption: `SELECT * FROM STREAM <name> CONSUMER GROUP <group>
    // [PARTITION <p>] [LIMIT <n>]`. Parses into no typed AST variant — the
    // pgwire streaming router recognized it by string prefix from the raw
    // token slice. Replicate that exactly here, before the parse gate, so the
    // prefix recognition and the `parts`-based extraction stay byte-identical.
    if upper.starts_with("SELECT ")
        && upper.contains("FROM STREAM ")
        && upper.contains("CONSUMER GROUP")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(stream_select::select_from_stream(state, identity, database_id, &parts).await);
    }

    // Stream/Topic consumption: `SELECT * FROM TOPIC <name> CONSUMER GROUP
    // <group> [LIMIT <n>]`. Topics use "topic:<name>" buffer keys; the pgwire
    // streaming router rewrote the token slice (TOPIC → STREAM, name →
    // "topic:<name>") and delegated to the stream-consume handler. Replicate
    // that rewrite exactly here, before the parse gate.
    if upper.starts_with("SELECT ")
        && upper.contains("FROM TOPIC ")
        && upper.contains("CONSUMER GROUP")
    {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 8
            || !parts[3].eq_ignore_ascii_case("TOPIC")
            || !parts[5].eq_ignore_ascii_case("CONSUMER")
            || !parts[6].eq_ignore_ascii_case("GROUP")
        {
            return Some(Err(DdlError {
                sqlstate: "42601".to_string(),
                message: "expected SELECT * FROM TOPIC <topic> CONSUMER GROUP <group>".to_string(),
            }));
        }
        let prefixed_name = format!("topic:{}", parts[4].to_lowercase());
        let stream_keyword = "STREAM";
        let mut rewritten = Vec::with_capacity(parts.len());
        for (i, &p) in parts.iter().enumerate() {
            match i {
                3 => rewritten.push(stream_keyword),
                4 => rewritten.push(prefixed_name.as_str()),
                _ => rewritten.push(p),
            }
        }
        return Some(
            stream_select::select_from_stream(state, identity, database_id, &rewritten).await,
        );
    }

    // Durable topics: `SUBSCRIBE TO <topic> [GROUP <group>] [SINCE <seq>]`.
    // Parses into no typed AST variant, so route it before the parse gate.
    if upper.starts_with("SUBSCRIBE TO ") {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        return Some(topic_subscribe::subscribe_to(
            state,
            identity,
            database_id,
            sql,
            &parts,
        ));
    }

    None
}
