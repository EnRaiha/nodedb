// SPDX-License-Identifier: BUSL-1.1

//! `CrashHarness` query helpers over pgwire.
//!
//! Split out of `mod.rs` (wiring + process lifecycle only) because this is a
//! distinct concern: running SQL against the harness and shaping the results,
//! as opposed to spawning/killing the child process itself.

use std::time::{Duration, Instant};

use super::CrashHarness;

impl CrashHarness {
    /// Open a fresh pgwire connection, run one statement, and return the
    /// resulting messages. Panics on connect/exec error.
    ///
    /// Retries the transient "no sequencer leader elected yet" startup
    /// condition: `/healthz` intentionally reports ready before the Calvin
    /// sequencer group has elected a leader (the sequencer is deliberately not
    /// a data group in the readiness gate), so a cross-shard write issued in
    /// the first moments of uptime can race the election and get a clean,
    /// retryable error. On a loaded machine that window is wide enough to lose.
    /// A real client retries; so does the harness, bounded, before writing.
    async fn simple_query_ready(&self, sql: &str) -> Vec<tokio_postgres::SimpleQueryMessage> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let (client, connection) =
                tokio_postgres::connect(&self.pgwire_conn_str(), tokio_postgres::NoTls)
                    .await
                    .expect("connect for exec");
            let conn_handle = tokio::spawn(async move {
                let _ = connection.await;
            });
            let result = client.simple_query(sql).await;
            drop(client);
            let _ = conn_handle.await;
            match result {
                Ok(messages) => return messages,
                Err(e)
                    if Instant::now() < deadline
                        && e.as_db_error().is_some_and(|db| {
                            db.message().contains("no sequencer leader elected yet")
                        }) =>
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                // `tokio_postgres::Error` renders as a bare "db error"; the
                // server's message only lives on the DbError payload. The
                // server log tail comes along because the interesting failures
                // here are server-side (stalled apply, failed recovery) and the
                // harness's tempdir is gone by the time anyone reads the panic.
                Err(e) => {
                    let log = self.server_log();
                    let tail: String = log
                        .lines()
                        .rev()
                        .take(60)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect::<Vec<_>>()
                        .join("\n");
                    panic!(
                        "exec: {e}{}\n--- server log (last 60 lines) ---\n{tail}",
                        e.as_db_error()
                            .map(|db| format!(" — {}: {}", db.code().code(), db.message()))
                            .unwrap_or_default()
                    )
                }
            }
        }
    }

    /// Open a fresh pgwire connection, run one statement, and drop the
    /// connection. Panics on connect/exec error.
    pub async fn exec(&self, sql: &str) {
        let _ = self.simple_query_ready(sql).await;
    }

    /// Run a query and return column `col` from every returned row, as text
    /// (via `simple_query`, so the value survives regardless of its type OID).
    pub async fn query_col(&self, sql: &str, col: &str) -> Vec<String> {
        let messages = self.simple_query_ready(sql).await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(col).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }

    /// Run a query and return column `idx` (0-based) from every returned row.
    ///
    /// Positional rather than by-name: a bare `COUNT(*)` — and even
    /// `COUNT(*) AS n` — does not surface a usable column name through the
    /// pgwire row description, so aggregates must be read by index.
    pub async fn query_col_idx(&self, sql: &str, idx: usize) -> Vec<String> {
        let messages = self.simple_query_ready(sql).await;
        messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some(row.get(idx).unwrap_or_default().to_string())
                }
                _ => None,
            })
            .collect()
    }
}
