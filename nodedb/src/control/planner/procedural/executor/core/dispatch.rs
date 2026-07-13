// SPDX-License-Identifier: BUSL-1.1

//! DML dispatch and transaction control for the statement executor.

use super::super::transaction::ProcedureTransactionCtx;
use super::StatementExecutor;
use crate::control::planner::procedural::ast::SqlExpr;
use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::eval;
use crate::types::TraceId;

impl<'a> StatementExecutor<'a> {
    // ── ASSIGN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_assign(
        &self,
        target: &str,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let target_upper = target.to_uppercase();
        if let Some(field_name) = target_upper.strip_prefix("NEW.") {
            let bound_expr = bindings.substitute(&expr.sql);
            let value = eval::evaluate_to_value(self.state, self.tenant_id, &bound_expr).await?;
            let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
            guard.insert(field_name.to_lowercase(), value);
        }
        Ok(())
    }

    // ── RETURN handling ─────────────────────────────────────────────────

    pub(super) async fn execute_return(
        &self,
        expr: &SqlExpr,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let bound_expr = bindings.substitute(&expr.sql);
        let value = eval::evaluate_to_value(self.state, self.tenant_id, &bound_expr).await?;
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert("__return".to_string(), value);
        Ok(())
    }

    // ── SQL dispatch ────────────────────────────────────────────────────

    pub(super) async fn execute_sql(&self, sql: &str, bindings: &RowBindings) -> crate::Result<()> {
        let bound_sql = fold_literal_string_concat(&bindings.substitute(sql));

        // First, attempt unified dispatch for NodeDB SQL extensions (PUBLISH TO,
        // topic/consumer-group DDL, etc.). If the SQL is not an extension,
        // `dispatch_sql` returns None and we fall through to plan_sql with
        // transaction-buffer semantics preserved exactly as before.
        if let Some(result) = crate::control::sql_dispatch::dispatch_sql(
            self.state,
            &self.identity_for_dispatch(),
            &bound_sql,
        )
        .await
        {
            result?;
            return Ok(());
        }

        // Not a NodeDB extension: route through plan_sql with transaction buffering.
        let ctx = crate::control::planner::context::QueryContext::for_state(self.state);
        let (tasks, _output_schema) = ctx
            .plan_sql(
                &bound_sql,
                self.tenant_id,
                crate::types::DatabaseId::DEFAULT,
            )
            .await?;

        if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            for task in tasks {
                guard.buffer_task(task);
            }
        } else {
            for task in tasks {
                let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
                    &self.state.wal,
                    task.tenant_id,
                    task.vshard_id,
                    task.database_id,
                    &task.plan,
                )?;

                crate::control::server::dispatch_utils::dispatch_write_to_data_plane(
                    self.state,
                    crate::control::server::dispatch_utils::WriteDispatch {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        vshard_id: task.vshard_id,
                        plan: task.plan,
                        trace_id: TraceId::ZERO,
                        event_source: self.event_source,
                        txn_id: None,
                        wal_lsn: outcome.lsn,
                        resolved_now_ms: outcome.resolved_now_ms,
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Return the procedural session's identity for use when dispatching SQL extensions.
    fn identity_for_dispatch(&self) -> crate::control::security::identity::AuthenticatedIdentity {
        self.identity.clone()
    }

    // ── Transaction control ─────────────────────────────────────────────

    pub(super) async fn execute_commit(&self) -> crate::Result<()> {
        self.flush_transaction_buffer().await
    }

    pub(super) fn execute_rollback(&self) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK", |ctx| {
            ctx.rollback();
            Ok(())
        })
    }

    pub(super) fn execute_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("SAVEPOINT", |ctx| {
            ctx.savepoint(name);
            Ok(())
        })
    }

    pub(super) fn execute_rollback_to(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("ROLLBACK TO", |ctx| ctx.rollback_to(name))
    }

    pub(super) fn execute_release_savepoint(&self, name: &str) -> crate::Result<()> {
        self.with_tx_ctx("RELEASE SAVEPOINT", |ctx| ctx.release_savepoint(name))
    }

    fn with_tx_ctx(
        &self,
        stmt_name: &str,
        f: impl FnOnce(&mut ProcedureTransactionCtx) -> crate::Result<()>,
    ) -> crate::Result<()> {
        match self.tx_ctx {
            Some(ref tx_ctx) => {
                let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
                f(&mut guard)
            }
            None => Err(crate::Error::BadRequest {
                detail: format!("{stmt_name} is only valid inside stored procedures"),
            }),
        }
    }

    /// Flush the procedure transaction buffer: WAL append + dispatch as batch.
    pub(super) async fn flush_transaction_buffer(&self) -> crate::Result<()> {
        let tasks = if let Some(ref tx_ctx) = self.tx_ctx {
            let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
            guard.take_buffered_tasks()
        } else {
            return Ok(());
        };

        if tasks.is_empty() {
            return Ok(());
        }

        // Each task's WAL record has its own LSN; the batch dispatch below
        // carries the highest so the Data Plane's write-version floor advances
        // past every write it applies. Same approximation for the resolved TTL
        // instant: a single scalar can't represent one-per-task resolved
        // instants for a heterogeneous multi-statement batch, so it is only
        // threaded through when the buffer holds exactly one task (below);
        // resolving that properly for N>1 would need `MetaOp::TransactionBatch`
        // to carry a per-plan `Vec<Option<u64>>`, a separate, wider change to
        // the procedural batch-flush path, not this KV-write fix.
        let mut max_wal_lsn: Option<crate::types::Lsn> = None;
        let mut single_task_resolved_now_ms: Option<u64> = None;
        for task in &tasks {
            let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
                &self.state.wal,
                task.tenant_id,
                task.vshard_id,
                task.database_id,
                &task.plan,
            )?;
            if let Some(lsn) = outcome.lsn {
                max_wal_lsn = Some(max_wal_lsn.map_or(lsn, |cur| cur.max(lsn)));
            }
            single_task_resolved_now_ms = outcome.resolved_now_ms;
        }

        if tasks.len() == 1 {
            if let Some(task) = tasks.into_iter().next() {
                crate::control::server::dispatch_utils::dispatch_write_to_data_plane(
                    self.state,
                    crate::control::server::dispatch_utils::WriteDispatch {
                        tenant_id: task.tenant_id,
                        database_id: task.database_id,
                        vshard_id: task.vshard_id,
                        plan: task.plan,
                        trace_id: TraceId::ZERO,
                        event_source: self.event_source,
                        txn_id: None,
                        wal_lsn: max_wal_lsn,
                        resolved_now_ms: single_task_resolved_now_ms,
                    },
                )
                .await?;
            }
        } else {
            let tenant_id = tasks[0].tenant_id;
            let database_id = tasks[0].database_id;
            let vshard_id = tasks[0].vshard_id;
            let plans: Vec<_> = tasks.into_iter().map(|t| t.plan).collect();
            let batch_plan = crate::bridge::envelope::PhysicalPlan::Meta(
                nodedb_physical::physical_plan::MetaOp::TransactionBatch {
                    plans,
                    txn_id: None,
                },
            );
            crate::control::server::dispatch_utils::dispatch_write_to_data_plane(
                self.state,
                crate::control::server::dispatch_utils::WriteDispatch {
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan: batch_plan,
                    trace_id: TraceId::ZERO,
                    event_source: self.event_source,
                    txn_id: None,
                    wal_lsn: max_wal_lsn,
                    // N>1 batch: no single instant represents every task's
                    // resolved TTL — see the comment above the WAL-append loop.
                    resolved_now_ms: None,
                },
            )
            .await?;
        }

        Ok(())
    }
}

fn fold_literal_string_concat(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let bytes = sql.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] != b'\'' {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        }

        let start = i;
        let Some((mut cursor, mut combined)) = parse_single_quoted_literal(sql, i) else {
            result.push(bytes[i] as char);
            i += 1;
            continue;
        };

        let mut folded = false;
        while let Some(op_end) = consume_string_concat_operator(bytes, cursor) {
            let next_lit = skip_ascii_whitespace(bytes, op_end);
            let Some((next_cursor, next_literal)) = parse_single_quoted_literal(sql, next_lit)
            else {
                break;
            };

            combined.push_str(&next_literal);
            cursor = next_cursor;
            folded = true;
        }

        if folded {
            result.push('\'');
            for ch in combined.chars() {
                if ch == '\'' {
                    result.push_str("''");
                } else {
                    result.push(ch);
                }
            }
            result.push('\'');
        } else {
            result.push_str(&sql[start..cursor]);
        }
        i = cursor;
    }

    result
}

fn parse_single_quoted_literal(sql: &str, start: usize) -> Option<(usize, String)> {
    let bytes = sql.as_bytes();
    if bytes.get(start) != Some(&b'\'') {
        return None;
    }

    let mut i = start + 1;
    let mut literal = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                if bytes.get(i + 1) == Some(&b'\'') {
                    literal.push('\'');
                    i += 2;
                } else {
                    return Some((i + 1, literal));
                }
            }
            byte => {
                literal.push(byte as char);
                i += 1;
            }
        }
    }

    None
}

use super::super::sql_bytes::skip_ascii_whitespace;

fn consume_string_concat_operator(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = skip_ascii_whitespace(bytes, start);
    if bytes.get(i) != Some(&b'|') {
        return None;
    }
    i += 1;
    i = skip_ascii_whitespace(bytes, i);
    if bytes.get(i) != Some(&b'|') {
        return None;
    }
    Some(i + 1)
}

#[cfg(test)]
mod tests {
    use super::fold_literal_string_concat;

    #[test]
    fn folds_adjacent_string_concat_literals() {
        let sql = "INSERT INTO log VALUES ('as1' || '_log', 'ok')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "INSERT INTO log VALUES ('as1_log', 'ok')");
    }

    #[test]
    fn folds_multiple_concat_segments() {
        let sql = "VALUES ('a' || 'b' || 'c')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "VALUES ('abc')");
    }

    #[test]
    fn folds_spaced_concat_operator_segments() {
        let sql = "VALUES ('a' | | 'b')";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, "VALUES ('ab')");
    }

    #[test]
    fn leaves_non_concat_literals_unchanged() {
        let sql = "VALUES ('a', col)";
        let folded = fold_literal_string_concat(sql);
        assert_eq!(folded, sql);
    }
}
