// SPDX-License-Identifier: BUSL-1.1

//! Statement executor for procedural SQL blocks with DML.
//!
//! Split into sub-modules:
//! - `control_flow`: IF/WHILE/LOOP/FOR execution
//! - `dispatch`: DML dispatch, ASSIGN, RETURN, transaction control

mod control_flow;
mod dispatch;
pub mod sql_literal_concat;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::control::planner::procedural::ast::*;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::bindings::RowBindings;
use super::exception::exception_matches;
use super::fuel::ExecutionBudget;
use super::transaction::ProcedureTransactionCtx;

/// Maximum trigger cascade depth (trigger A fires trigger B fires trigger A).
pub const MAX_CASCADE_DEPTH: u32 = 16;

/// Source-write context propagated into a trigger body's executor so that DML
/// targeting a remote-homed collection is dispatched to the owning node via the
/// cross-shard event subsystem instead of being silently mis-written to the
/// local Data-Plane core.
///
/// Populated ONLY by the Event-Plane AFTER-trigger fire path (from the source
/// `WriteEvent`). Stored procedures and normal client SQL leave it `None`; its
/// presence is the gate that enables cross-shard routing in `execute_sql`.
#[derive(Debug, Clone)]
pub struct CrossShardOrigin {
    /// LSN of the source write that fired the trigger (target-side HWM dedup key).
    pub source_lsn: u64,
    /// Sequence number of the source write (monotonic per core/collection).
    pub source_sequence: u64,
    /// vShard that owns the source collection (dedup key on the target).
    pub source_vshard: u32,
    /// Collection whose write fired the trigger.
    pub source_collection: String,
}

/// Statement executor: steps through procedural SQL blocks with DML.
pub struct StatementExecutor<'a> {
    pub(super) state: &'a SharedState,
    #[allow(dead_code)]
    pub(super) identity: AuthenticatedIdentity,
    pub(super) tenant_id: TenantId,
    /// Database scope fixed for this executor's lifetime.
    pub(super) database_id: DatabaseId,
    cascade_depth: u32,
    pub(super) event_source: crate::event::EventSource,
    /// Arc<Mutex> required (not RefCell) because execute_statement returns `+ Send` futures.
    pub(super) new_mutations: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    pub(super) tx_ctx: Option<Arc<Mutex<ProcedureTransactionCtx>>>,
    pub(super) out_values: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    /// Cross-shard origin context; `Some` only in the Event-Plane trigger fire
    /// path. Gates remote-write dispatch in `execute_sql`.
    pub(super) cross_shard_origin: Option<CrossShardOrigin>,
}

/// Control flow signal from statement execution.
pub(super) enum Flow {
    Continue,
    Break,
    LoopContinue,
}

impl<'a> StatementExecutor<'a> {
    pub fn new(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            crate::event::EventSource::User,
        )
    }

    pub fn with_source(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
        )
    }

    /// Construct an executor in an explicit database scope when the caller
    /// carries definition or event database context independent of identity.
    pub fn with_source_in_database(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        database_id: DatabaseId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        Self {
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
            new_mutations: Arc::new(Mutex::new(HashMap::new())),
            tx_ctx: None,
            out_values: Arc::new(Mutex::new(HashMap::new())),
            cross_shard_origin: None,
        }
    }

    /// Enable procedure transaction context for COMMIT/ROLLBACK/SAVEPOINT.
    pub fn with_transaction_context(mut self) -> Self {
        self.tx_ctx = Some(Arc::new(Mutex::new(ProcedureTransactionCtx::new())));
        self
    }

    /// Attach cross-shard origin context (Event-Plane AFTER-trigger fire path).
    ///
    /// When set, `execute_sql` route-resolves every write task: a task homed on
    /// a remote node is dispatched to that node via the cross-shard dispatcher
    /// instead of being written to the local core.
    pub fn with_cross_shard_origin(mut self, origin: CrossShardOrigin) -> Self {
        self.cross_shard_origin = Some(origin);
        self
    }

    pub fn take_new_mutations(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn take_out_values(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub async fn execute_block(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let mut budget = ExecutionBudget::trigger_default();
        self.execute_block_with_exceptions(
            &block.statements,
            &block.exception_handlers,
            bindings,
            &mut budget,
        )
        .await
    }

    pub async fn execute_block_with_budget(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        let result = self
            .execute_block_with_exceptions(
                &block.statements,
                &block.exception_handlers,
                bindings,
                budget,
            )
            .await;

        if result.is_ok() {
            self.flush_transaction_buffer().await?;
        }

        result
    }

    async fn execute_block_with_exceptions(
        &self,
        stmts: &[Statement],
        handlers: &[ExceptionHandler],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        let result = self.execute_statements(stmts, bindings, budget).await;

        if let Err(ref err) = result
            && !handlers.is_empty()
        {
            if let Some(ref tx_ctx) = self.tx_ctx {
                let mut guard = tx_ctx.lock().unwrap_or_else(|p| p.into_inner());
                guard.rollback();
            }

            let err_str = err.to_string();
            for handler in handlers {
                if exception_matches(&handler.condition, &err_str) {
                    return self
                        .execute_statements(&handler.body, bindings, budget)
                        .await;
                }
            }
        }

        result
    }

    fn execute_statement<'b>(
        &'b self,
        stmt: &'b Statement,
        bindings: &'b RowBindings,
        budget: &'b mut ExecutionBudget,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<Flow>> + Send + 'b>> {
        Box::pin(async move {
            budget.check()?;

            match stmt {
                Statement::Sql { sql } => {
                    self.execute_sql(sql, bindings).await?;
                    Ok(Flow::Continue)
                }
                Statement::If {
                    condition,
                    then_block,
                    elsif_branches,
                    else_block,
                } => {
                    self.execute_if(
                        control_flow::IfBranches {
                            condition,
                            then_block,
                            elsif_branches,
                            else_block,
                        },
                        bindings,
                        budget,
                    )
                    .await
                }
                Statement::While { condition, body } => {
                    self.execute_while(condition, body, bindings, budget).await
                }
                Statement::Loop { body } => self.execute_loop(body, bindings, budget).await,
                Statement::For {
                    var,
                    start,
                    end,
                    reverse,
                    body,
                } => {
                    self.execute_for(
                        control_flow::ForLoopSpec {
                            var,
                            start,
                            end,
                            reverse: *reverse,
                            body,
                        },
                        bindings,
                        budget,
                    )
                    .await
                }
                Statement::Break => Ok(Flow::Break),
                Statement::Continue => Ok(Flow::LoopContinue),
                Statement::Raise {
                    level: RaiseLevel::Exception,
                    message,
                } => {
                    let msg = bindings.substitute(&message.sql);
                    let clean_msg = msg.trim().trim_matches('\'').to_string();
                    Err(crate::Error::BadRequest {
                        detail: format!("raised exception: {clean_msg}"),
                    })
                }
                Statement::Raise { .. } => Ok(Flow::Continue),
                Statement::Declare { .. } => Ok(Flow::Continue),
                Statement::Assign { target, expr } => {
                    self.execute_assign(target, expr, bindings).await?;
                    Ok(Flow::Continue)
                }
                Statement::Return { expr } => {
                    self.execute_return(expr, bindings).await?;
                    Ok(Flow::Continue)
                }
                Statement::ReturnQuery { .. } => Ok(Flow::Continue),
                Statement::Commit => {
                    self.execute_commit().await?;
                    Ok(Flow::Continue)
                }
                Statement::Rollback => {
                    self.execute_rollback()?;
                    Ok(Flow::Continue)
                }
                Statement::Savepoint { name } => {
                    self.execute_savepoint(name)?;
                    Ok(Flow::Continue)
                }
                Statement::RollbackTo { name } => {
                    self.execute_rollback_to(name)?;
                    Ok(Flow::Continue)
                }
                Statement::ReleaseSavepoint { name } => {
                    self.execute_release_savepoint(name)?;
                    Ok(Flow::Continue)
                }
            }
        })
    }

    pub(super) async fn execute_statements(
        &self,
        stmts: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        for stmt in stmts {
            self.execute_statement(stmt, bindings, budget).await?;
        }
        Ok(())
    }

    pub(super) async fn execute_statements_flow(
        &self,
        stmts: &[Statement],
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<Flow> {
        for stmt in stmts {
            let flow = self.execute_statement(stmt, bindings, budget).await?;
            match flow {
                Flow::Continue => {}
                Flow::Break | Flow::LoopContinue => return Ok(flow),
            }
        }
        Ok(Flow::Continue)
    }

    pub fn cascade_depth(&self) -> u32 {
        self.cascade_depth
    }
}
