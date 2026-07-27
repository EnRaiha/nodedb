// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral containment for one connection's asynchronous work.

use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;

/// Outcome of a connection-scoped future after panic containment.
///
/// Panic payloads are deliberately discarded. Protocol listeners must close
/// the affected connection and log only non-sensitive connection metadata.
#[derive(Debug, Eq, PartialEq)]
pub enum ConnectionFutureOutcome<T> {
    /// The connection future completed normally.
    Completed(T),
    /// The connection future unwound; its payload is intentionally unavailable.
    Panicked,
}

/// Run one complete connection future without allowing an application panic to
/// escape its listener task.
///
/// Boxing happens synchronously before the returned future is built. This keeps
/// large protocol state machines off the listener task's stack while preserving
/// cancellation: dropping the returned future drops the boxed connection future.
pub fn isolate_connection_future<T>(
    future: impl Future<Output = T>,
) -> impl Future<Output = ConnectionFutureOutcome<T>> {
    let future = Box::pin(future);
    async move {
        match AssertUnwindSafe(future).catch_unwind().await {
            Ok(value) => ConnectionFutureOutcome::Completed(value),
            Err(_) => ConnectionFutureOutcome::Panicked,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::{ConnectionFutureOutcome, isolate_connection_future};

    #[tokio::test]
    async fn panic_becomes_typed_outcome_without_payload() {
        let outcome = isolate_connection_future(async {
            panic!("connection panic payload must remain private");
        })
        .await;

        assert!(matches!(outcome, ConnectionFutureOutcome::Panicked));
    }

    #[tokio::test]
    async fn owned_permit_releases_during_panic_unwind() {
        let permits = Arc::new(Semaphore::new(1));
        let permit = permits
            .clone()
            .try_acquire_owned()
            .unwrap_or_else(|_| unreachable!());
        let outcome = isolate_connection_future(async move {
            let _permit = permit;
            panic!("connection panic payload must remain private");
        })
        .await;

        assert!(matches!(outcome, ConnectionFutureOutcome::Panicked));
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn completed_output_is_preserved() {
        let outcome = isolate_connection_future(async { 42_u64 }).await;

        assert_eq!(outcome, ConnectionFutureOutcome::Completed(42));
    }
}
