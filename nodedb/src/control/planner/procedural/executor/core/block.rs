//! Block-level execution: runs a procedural block's statements with exception handling.

use crate::control::planner::procedural::ast::{ExceptionHandler, ProceduralBlock, Statement};

use super::super::bindings::RowBindings;
use super::super::exception::exception_matches;
use super::super::fuel::ExecutionBudget;
use super::StatementExecutor;

impl<'a> StatementExecutor<'a> {
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
}
