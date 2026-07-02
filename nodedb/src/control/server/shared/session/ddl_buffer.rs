// SPDX-License-Identifier: BUSL-1.1

//! Per-session DDL transaction buffer.
//!
//! When a connection session is inside a `BEGIN` block and executes DDL
//! statements (CREATE, DROP, ALTER), the `propose_catalog_entry`
//! path checks this buffer. If the buffer is active (non-None), the
//! entry is pushed into it instead of being proposed immediately.
//!
//! On `COMMIT`, the buffer is flushed as a single
//! `MetadataEntry::Batch`, so either all DDL in the transaction
//! commits atomically or none does.
//!
//! On `ROLLBACK`, the buffer is cleared without proposing.

use std::cell::RefCell;

use super::audit_context::AuditCtx;

/// One buffered DDL statement: the encoded `CatalogEntry` payload
/// plus the optional audit context captured from
/// [`super::audit_context::current()`] at buffer time. The audit
/// context is stamped at *statement* time, not at COMMIT time, so
/// each sub-entry's audit record correctly names the DDL that
/// produced it (not just the COMMIT).
#[derive(Debug, Clone)]
pub struct BufferedDdl {
    pub payload: Vec<u8>,
    pub audit: Option<AuditCtx>,
}

/// Encoded DDL payloads buffered during a transaction.
pub type DdlBuffer = Vec<BufferedDdl>;

thread_local! {
    /// Thread-local flag: when `Some`, `propose_catalog_entry` pushes
    /// into this buffer instead of proposing through raft. Set by
    /// `activate` before DDL dispatch, cleared by `take`.
    ///
    /// Thread-local is safe here because pgwire DDL handlers run
    /// synchronously via `block_in_place` — the buffer is set and
    /// read on the same OS thread within a single handler call.
    static ACTIVE_BUFFER: RefCell<Option<DdlBuffer>> = const { RefCell::new(None) };
}

/// Activate the DDL buffer for the current thread. Any subsequent
/// call to `try_buffer` will push into this buffer instead of
/// returning `None`.
pub fn activate() {
    ACTIVE_BUFFER.with(|b| {
        let mut guard = b.borrow_mut();
        if guard.is_none() {
            *guard = Some(Vec::new());
        }
    });
}

/// Try to buffer a DDL payload. Returns `true` if the buffer is
/// active and the payload was pushed. Returns `false` if no buffer
/// is active (caller should propose normally).
pub fn try_buffer(payload: Vec<u8>) -> bool {
    ACTIVE_BUFFER.with(|b| {
        let mut guard = b.borrow_mut();
        if let Some(buf) = guard.as_mut() {
            buf.push(BufferedDdl {
                payload,
                audit: super::audit_context::current(),
            });
            true
        } else {
            false
        }
    })
}

/// Take the accumulated buffer contents and deactivate. Returns
/// `None` if the buffer was never activated.
pub fn take() -> Option<DdlBuffer> {
    ACTIVE_BUFFER.with(|b| b.borrow_mut().take())
}

/// Deactivate and discard the buffer without returning its contents.
pub fn discard() {
    ACTIVE_BUFFER.with(|b| {
        let _ = b.borrow_mut().take();
    });
}

/// Returns `true` if a DDL buffer is currently active on this thread.
pub fn is_active() -> bool {
    ACTIVE_BUFFER.with(|b| b.borrow().is_some())
}

/// Number of DDL statements buffered in the current thread's
/// active transaction. Returns 0 if no buffer is active.
pub fn buffer_len() -> usize {
    ACTIVE_BUFFER.with(|b| b.borrow().as_ref().map(|v| v.len()).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inactive_buffer_does_not_capture() {
        discard(); // ensure clean state
        assert!(!try_buffer(vec![1, 2, 3]));
        assert!(!is_active());
    }

    #[test]
    fn active_buffer_captures() {
        activate();
        assert!(is_active());
        assert!(try_buffer(vec![1]));
        assert!(try_buffer(vec![2]));
        let buf = take().unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].payload, vec![1]);
        assert_eq!(buf[1].payload, vec![2]);
        assert!(!is_active());
    }

    #[test]
    fn discard_clears_buffer() {
        activate();
        try_buffer(vec![1]);
        discard();
        assert!(!is_active());
        assert!(take().is_none());
    }

    #[test]
    fn take_on_inactive_returns_none() {
        discard();
        assert!(take().is_none());
    }
}
