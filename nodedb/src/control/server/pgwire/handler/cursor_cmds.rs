// SPDX-License-Identifier: BUSL-1.1

//! FETCH and MOVE command handlers for server-side cursors.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use super::super::types::text_field;
use crate::control::server::shared::session::SessionId;

use super::core::NodeDbPgHandler;

impl NodeDbPgHandler {
    /// Handle FETCH [ALL | FORWARD n | BACKWARD n | n] FROM cursor_name.
    pub(super) fn handle_fetch(
        &self,
        session_id: SessionId,
        sql: &str,
        upper: &str,
    ) -> PgWireResult<Vec<Response>> {
        let parts: Vec<&str> = sql.split_whitespace().collect();

        // Parse fetch direction and count.
        let (direction, count, cursor_name) = parse_fetch(upper, &parts)?;

        let rows = match direction {
            FetchDirection::Forward => {
                let (rows, _) = self
                    .sessions
                    .fetch_cursor(session_id, &cursor_name, count)
                    .map_err(|e| cursor_error(&e.to_string()))?;
                rows
            }
            FetchDirection::All => self
                .sessions
                .fetch_cursor_all(session_id, &cursor_name)
                .map_err(|e| cursor_error(&e.to_string()))?,
            FetchDirection::Backward => self
                .sessions
                .fetch_cursor_backward(session_id, &cursor_name, count)
                .map_err(|e| cursor_error(&e.to_string()))?,
        };

        let schema = Arc::new(vec![text_field("result")]);
        let mut encoded_rows = Vec::with_capacity(rows.len());
        for row_json in &rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            let _ = encoder.encode_field(row_json);
            encoded_rows.push(Ok(encoder.take_row()));
        }
        Ok(vec![Response::Query(QueryResponse::new(
            schema,
            futures::stream::iter(encoded_rows),
        ))])
    }

    /// Handle MOVE [FORWARD | BACKWARD] n IN cursor_name.
    pub(super) fn handle_move(
        &self,
        session_id: SessionId,
        upper: &str,
    ) -> PgWireResult<Vec<Response>> {
        let parts: Vec<&str> = upper.split_whitespace().collect();

        // MOVE FORWARD n IN cursor_name
        // MOVE BACKWARD n IN cursor_name
        // MOVE n IN cursor_name
        let (forward, count, cursor_name) = parse_move(&parts)?;

        let moved = self
            .sessions
            .move_cursor(session_id, &cursor_name, forward, count)
            .map_err(|e| cursor_error(&e.to_string()))?;

        Ok(vec![Response::Execution(Tag::new("MOVE").with_rows(moved))])
    }
}

enum FetchDirection {
    Forward,
    Backward,
    All,
}

/// Parse FETCH [ALL | FORWARD n | BACKWARD n | n] FROM cursor_name.
fn parse_fetch(upper: &str, parts: &[&str]) -> PgWireResult<(FetchDirection, usize, String)> {
    // FETCH ALL FROM cursor_name
    if upper.contains(" ALL ") {
        let from_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FROM"))
            .unwrap_or(parts.len() - 1);
        let cursor_name = parts.get(from_idx + 1).unwrap_or(&"default").to_lowercase();
        return Ok((FetchDirection::All, 0, cursor_name));
    }

    // FETCH BACKWARD n FROM cursor_name
    if upper.contains("BACKWARD") {
        let bw_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("BACKWARD"))
            .unwrap_or(1);
        let count = parts
            .get(bw_idx + 1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let from_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FROM"))
            .unwrap_or(parts.len() - 1);
        let cursor_name = parts.get(from_idx + 1).unwrap_or(&"default").to_lowercase();
        return Ok((FetchDirection::Backward, count, cursor_name));
    }

    // FETCH FORWARD n FROM cursor_name
    if upper.contains("FORWARD") {
        let fw_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FORWARD"))
            .unwrap_or(1);
        let count = parts
            .get(fw_idx + 1)
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let from_idx = parts
            .iter()
            .position(|p| p.eq_ignore_ascii_case("FROM"))
            .unwrap_or(parts.len() - 1);
        let cursor_name = parts.get(from_idx + 1).unwrap_or(&"default").to_lowercase();
        return Ok((FetchDirection::Forward, count, cursor_name));
    }

    // FETCH n FROM cursor_name (default: forward)
    if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("FROM") {
        let count = parts[1].parse::<usize>().unwrap_or(1);
        let cursor_name = parts[3].to_lowercase();
        return Ok((FetchDirection::Forward, count, cursor_name));
    }

    // FETCH FROM cursor_name (1 row forward)
    if parts.len() >= 3 && parts[1].eq_ignore_ascii_case("FROM") {
        let cursor_name = parts[2].to_lowercase();
        return Ok((FetchDirection::Forward, 1, cursor_name));
    }

    // FETCH cursor_name (1 row forward)
    let cursor_name = parts.get(1).unwrap_or(&"default").to_lowercase();
    Ok((FetchDirection::Forward, 1, cursor_name))
}

/// Parse MOVE [FORWARD | BACKWARD] n IN cursor_name.
fn parse_move(parts: &[&str]) -> PgWireResult<(bool, usize, String)> {
    // MOVE FORWARD n IN cursor_name
    if parts.len() >= 5 && parts[1].eq_ignore_ascii_case("FORWARD") {
        let count = parts[2].parse::<usize>().unwrap_or(1);
        let cursor_name = parts[4].to_lowercase();
        return Ok((true, count, cursor_name));
    }

    // MOVE BACKWARD n IN cursor_name
    if parts.len() >= 5 && parts[1].eq_ignore_ascii_case("BACKWARD") {
        let count = parts[2].parse::<usize>().unwrap_or(1);
        let cursor_name = parts[4].to_lowercase();
        return Ok((false, count, cursor_name));
    }

    // MOVE n IN cursor_name (forward by default)
    if parts.len() >= 4 && parts[2].eq_ignore_ascii_case("IN") {
        let count = parts[1].parse::<usize>().unwrap_or(1);
        let cursor_name = parts[3].to_lowercase();
        return Ok((true, count, cursor_name));
    }

    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "42601".to_owned(),
        "syntax: MOVE [FORWARD|BACKWARD] <count> IN <cursor_name>".to_owned(),
    ))))
}

fn cursor_error(msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "34000".to_owned(),
        msg.to_owned(),
    )))
}
