// SPDX-License-Identifier: BUSL-1.1

//! Client-facing notices raised below the response shaper.
//!
//! A response shaper turns a notice carried in a payload (an array slice's
//! `truncated_before_horizon`) into a client notice. Some reads never reach a
//! shaper with their own payload: a cluster array leaf inlined into a larger
//! plan becomes plain rows. Such a read raises its notice here instead. The
//! notice lives in the connection scope ([`super::conn_scope`]), and each
//! protocol takes it at the end of the statement:
//!
//! - pgwire sends it as a `NoticeResponse` before the result.
//! - The native protocol adds it to the response's `warnings`.
//!
//! Outside a connection scope there is no client to receive it, so it is
//! logged as a warning.

use super::conn_scope::with_scope;

/// Raise `message` for the client of the statement now running.
pub fn raise(message: String) {
    let mut pending = Some(message);
    let delivered = with_scope(false, |scope| {
        if let Some(message) = pending.take() {
            scope.statement_notices.borrow_mut().push(message);
        }
        true
    });
    if !delivered && let Some(message) = pending {
        tracing::warn!(notice = %message, "statement notice raised outside a connection scope");
    }
}

/// Take every notice the current statement raised, oldest first.
pub fn take() -> Vec<String> {
    with_scope(Vec::new(), |scope| {
        std::mem::take(&mut *scope.statement_notices.borrow_mut())
    })
}

#[cfg(test)]
mod tests {
    use super::super::conn_scope;
    use super::*;

    #[tokio::test]
    async fn a_raised_notice_is_taken_once_by_the_statement() {
        conn_scope::scoped(async {
            raise("first".into());
            tokio::task::yield_now().await;
            raise("second".into());
            assert_eq!(take(), vec!["first".to_string(), "second".to_string()]);
            assert!(take().is_empty(), "a notice is delivered once");
        })
        .await;
    }

    #[test]
    fn outside_a_connection_scope_nothing_is_taken() {
        raise("unscoped".into());
        assert!(take().is_empty());
    }
}
