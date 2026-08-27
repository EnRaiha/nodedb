// SPDX-License-Identifier: BUSL-1.1

//! Why an optimistic retry loop ran out of attempts.
//!
//! Carried by [`Error::OllpExhausted`] so exhaustion reports what actually
//! failed. Asserting predicate drift unconditionally is wrong: a loop that
//! never got a transaction admitted never observed a matching set at all.

use super::types::Error;

/// The reason an optimistic (OLLP) retry loop gave up.
#[derive(Debug, thiserror::Error)]
pub enum OllpExhaustedCause {
    /// Every attempt re-resolved a different matching set — real concurrent
    /// drift, and the only cause a plain retry can clear.
    #[error(
        "the predicate's matching set changed on every attempt under concurrent writes. \
         Retry the statement, or rephrase it as a static-key UPDATE"
    )]
    PredicateDrift,

    /// Every attempt failed before the transaction was admitted, for the
    /// carried reason. The inputs reproduce it, so retrying cannot clear it.
    #[error("every attempt failed before admission: {0}. Fix that condition; retrying cannot")]
    PreAdmission(Box<Error>),

    /// Every attempt was refused by the orchestrator's admission gate — an open
    /// circuit breaker, an exhausted tenant retry budget, or an unreachable
    /// sequencer. Transient: the statement succeeds once the gate reopens.
    #[error("every attempt was refused before submission: {detail}")]
    AdmissionRefused { detail: String },
}
