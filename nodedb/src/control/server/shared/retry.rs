// SPDX-License-Identifier: BUSL-1.1

//! Transparent statement retry for `RetryableSchemaChanged`.
//!
//! A descriptor lease drain is a short barrier the DDL path runs before
//! committing the next `descriptor_version`. Statement setup can observe it in
//! two places: the planner's catalog read (`SqlCatalogError::RetryableSchemaChanged`)
//! and the post-planning lease acquisition (`SharedState::acquire_plan_lease_scope`).
//! Both surface `crate::Error::RetryableSchemaChanged`, and both must sit inside
//! the SAME retried unit — a drain that starts between them is exactly the race
//! this loop exists to absorb.
//!
//! The retry is intentionally **dumb**: it re-runs the whole setup unit,
//! including parsing. A smarter implementation would hold onto the parsed AST
//! and only re-resolve. That's a future optimisation — for the common drain
//! case (sub-second drains on clusters with short query lifetimes) the extra
//! parse cost is negligible.
//!
//! ## Retry budget
//!
//! Five attempts total with 50/100/200/400 ms backoff between them — roughly
//! 750ms of tolerance for a drain to complete. The `DEFAULT_DRAIN_TIMEOUT` from
//! `metadata_proposer` is 35s, so in practice either drain completes within our
//! retry budget (the proposer is actively draining and is probably close to done
//! by the time we observe it) or drain is stuck and our error helps the operator
//! diagnose.
//!
//! The budget is per statement. Nesting one retried unit inside another would
//! multiply it, so a caller wraps its setup unit exactly once.

use std::time::Duration;

use crate::error::Error;

/// Maximum number of attempts (including the initial call).
const MAX_ATTEMPTS: usize = 5;

/// Backoff durations BETWEEN attempts. `BACKOFFS[i]` is the sleep
/// duration before attempt `i + 1`. Length must be
/// `MAX_ATTEMPTS - 1`.
const BACKOFFS: [Duration; MAX_ATTEMPTS - 1] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
    Duration::from_millis(400),
];

/// Classification hook for the retry loop.
///
/// Implemented by every error type a retried setup unit can fail with, so a
/// protocol-specific error wrapper stays retry-aware without the loop having to
/// sniff rendered SQLSTATE codes.
pub trait RetryableSchemaChange {
    /// The descriptor whose schema change makes this error retryable, or
    /// `None` when the failure is terminal.
    fn retryable_descriptor(&self) -> Option<&str>;
}

impl RetryableSchemaChange for Error {
    fn retryable_descriptor(&self) -> Option<&str> {
        // Deliberately narrow: only the descriptor-version race is retryable.
        // Widening this to other transient-looking variants would silently
        // re-run statements whose failure is real.
        match self {
            Error::RetryableSchemaChanged { descriptor } => Some(descriptor.as_str()),
            _ => None,
        }
    }
}

/// Run `op` up to `MAX_ATTEMPTS` times. Retries only while the error classifies
/// as a retryable schema change. Any other error is returned immediately on the
/// first attempt. Returns the last error observed if every attempt was
/// retryable.
///
/// The closure takes no arguments — callers capture whatever context (sql text,
/// tenant_id, security context) they need via move semantics. The closure is
/// `async` so it can `.await` the planner.
pub async fn retry_on_schema_change<F, Fut, T, E>(mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: RetryableSchemaChange + From<Error>,
{
    let mut last_err: Option<E> = None;
    for attempt in 0..MAX_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let Some(descriptor) = error.retryable_descriptor() else {
                    return Err(error);
                };
                tracing::debug!(
                    attempt,
                    descriptor,
                    "retrying statement setup after schema change"
                );
                last_err = Some(error);
                if let Some(backoff) = BACKOFFS.get(attempt) {
                    tokio::time::sleep(*backoff).await;
                }
            }
        }
    }
    // Exhausted retries — surface the last retryable error.
    Err(last_err.unwrap_or_else(|| {
        E::from(Error::PlanError {
            detail: "retry_on_schema_change: no attempts recorded".into(),
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn drain_error_classifies_as_retryable() {
        let error = Error::RetryableSchemaChanged {
            descriptor: "orders at version 3".into(),
        };
        assert_eq!(error.retryable_descriptor(), Some("orders at version 3"));
    }

    #[test]
    fn non_drain_lease_failures_are_not_reclassified() {
        // A configuration fault and an internal fault are the shapes a
        // non-drain lease failure takes. Neither may be retried.
        assert!(
            Error::Config {
                detail: "lease grant rejected".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
        assert!(
            Error::Internal {
                detail: "metadata raft unavailable".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
        assert!(
            Error::PlanError {
                detail: "syntax error".into(),
            }
            .retryable_descriptor()
            .is_none()
        );
    }

    #[tokio::test]
    async fn first_attempt_success() {
        let calls = AtomicUsize::new(0);
        let result: Result<i32, Error> = retry_on_schema_change(|| {
            let c = calls.fetch_add(1, Ordering::SeqCst);
            async move { Ok(c as i32) }
        })
        .await;
        assert_eq!(result.expect("first attempt succeeds"), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retries_on_schema_change_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let result: Result<&str, Error> = retry_on_schema_change(|| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(Error::RetryableSchemaChanged {
                        descriptor: format!("attempt {n}"),
                    })
                } else {
                    Ok("done")
                }
            }
        })
        .await;
        assert_eq!(result.expect("third attempt succeeds"), "done");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn surfaces_error_after_budget_exhausted() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), Error> = retry_on_schema_change(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::RetryableSchemaChanged {
                    descriptor: "orders".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::RetryableSchemaChanged { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }

    #[tokio::test]
    async fn non_retryable_error_surfaces_immediately() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), Error> = retry_on_schema_change(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(Error::PlanError {
                    detail: "syntax error".into(),
                })
            }
        })
        .await;
        assert!(matches!(result, Err(Error::PlanError { .. })));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
