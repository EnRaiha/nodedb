//! Block-level execution: runs a procedural block's statements with exception handling.

use crate::control::planner::procedural::ast::{ExceptionHandler, ProceduralBlock, Statement};
use crate::control::server::shared::session::conn_scope::scoped_system_txn;

use super::super::bindings::RowBindings;
use super::super::exception::exception_matches;
use super::super::fuel::ExecutionBudget;
use super::StatementExecutor;

impl<'a> StatementExecutor<'a> {
    /// Run `block` under the trigger budget. See [`Self::execute_block_with_budget`].
    pub async fn execute_block(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
    ) -> crate::Result<()> {
        let mut budget = ExecutionBudget::trigger_default();
        self.execute_block_with_budget(block, bindings, &mut budget)
            .await
    }

    /// Run `block` and commit what it staged as one transaction. On error
    /// nothing it staged since its last COMMIT is applied.
    ///
    /// The block runs in its own connection slots, so its DDL buffers with
    /// its writes and never into a client transaction it runs inside. A body
    /// joined to its statement's transaction runs in that transaction's
    /// slots instead.
    pub async fn execute_block_with_budget(
        &self,
        block: &ProceduralBlock,
        bindings: &RowBindings,
        budget: &mut ExecutionBudget,
    ) -> crate::Result<()> {
        let run = async {
            let result = self
                .execute_block_with_exceptions(
                    &block.statements,
                    &block.exception_handlers,
                    bindings,
                    budget,
                )
                .await;

            match result {
                Ok(()) => self.flush_transaction_buffer().await,
                Err(error) => {
                    self.discard_transaction_buffer().await;
                    Err(error)
                }
            }
        };
        // A joined body buffers its DDL into its statement's transaction,
        // which runs in the connection's slots.
        if self.joined.is_some() {
            run.await
        } else {
            scoped_system_txn(run).await
        }
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
            self.discard_transaction_buffer().await;

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
