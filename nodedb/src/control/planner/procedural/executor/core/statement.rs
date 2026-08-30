//! Single-statement dispatch: matches a `Statement` variant to its handler.

use crate::control::planner::procedural::ast::{RaiseLevel, Statement};

use super::super::bindings::RowBindings;
use super::super::fuel::ExecutionBudget;
use super::{Flow, StatementExecutor, control_flow};

impl<'a> StatementExecutor<'a> {
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
}
