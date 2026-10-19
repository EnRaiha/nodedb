// SPDX-License-Identifier: BUSL-1.1

//! The one OLLP retry policy: a retry budget, and the backoff and circuit
//! accounting the orchestrator applies to each retry.
//!
//! Every Control-Plane loop that re-reads a moved prediction spends retries
//! from this budget: the coordinator's dependent-read loop and a transaction
//! statement's predicate stage.

use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;

/// The retries one OLLP statement may spend on a predicate class.
pub struct OllpRetryBudget<'a> {
    orchestrator: &'a OllpOrchestrator,
    predicate_class: u64,
    max_retries: u32,
    spent: u32,
}

impl<'a> OllpRetryBudget<'a> {
    /// A budget of `max_retries` retries on `predicate_class`.
    pub fn new(orchestrator: &'a OllpOrchestrator, predicate_class: u64, max_retries: u32) -> Self {
        Self {
            orchestrator,
            predicate_class,
            max_retries,
            spent: 0,
        }
    }

    /// A budget of the orchestrator's configured retries on
    /// `predicate_class`.
    pub fn configured(orchestrator: &'a OllpOrchestrator, predicate_class: u64) -> Self {
        let max_retries = u32::from(orchestrator.ollp_max_retries());
        Self::new(orchestrator, predicate_class, max_retries)
    }

    /// Whether every retry is spent.
    pub fn exhausted(&self) -> bool {
        self.spent >= self.max_retries
    }

    /// The budget's size, as an exhaustion error reports it.
    pub fn max_retries(&self) -> u8 {
        u8::try_from(self.max_retries).unwrap_or(u8::MAX)
    }

    /// Spend one retry: the orchestrator records it in the predicate class's
    /// circuit window and backs off before the caller retries.
    pub async fn spend(&mut self) {
        self.orchestrator
            .on_retry_required(self.predicate_class, self.spent)
            .await;
        self.spent += 1;
    }
}
