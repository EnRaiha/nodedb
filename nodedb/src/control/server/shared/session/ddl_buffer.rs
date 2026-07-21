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

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;
use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, encode_entry};

use super::audit_context::AuditCtx;
use super::outcome::AbortReason;

/// One buffered DDL statement: the unstamped `CatalogEntry`
/// plus the optional audit context captured from
/// [`super::audit_context::current()`] at buffer time. The audit
/// context is stamped at *statement* time, not at COMMIT time, so
/// each sub-entry's audit record correctly names the DDL that
/// produced it (not just the COMMIT).
#[derive(Debug, Clone)]
pub struct BufferedDdl {
    pub entry: CatalogEntry,
    pub audit: Option<AuditCtx>,
}

/// Unstamped DDL entries buffered during a transaction.
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

/// Try to buffer an unstamped DDL entry. Returns `true` if the buffer is
/// active and the entry was pushed. Returns `false` if no buffer is active
/// (caller should prepare and propose normally).
pub fn try_buffer(entry: CatalogEntry) -> bool {
    ACTIVE_BUFFER.with(|b| {
        let mut guard = b.borrow_mut();
        if let Some(buf) = guard.as_mut() {
            buf.push(BufferedDdl {
                entry,
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

/// Flush buffered entries as one fenced metadata-Raft batch.
pub(super) fn flush(state: &SharedState) -> Option<AbortReason> {
    let buffered = take()?;
    if buffered.is_empty() {
        return None;
    }
    let handle = state.metadata_raft.get()?;
    let _local_guard = match state.metadata_ddl_lock.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: "metadata DDL preparation lock poisoned".into(),
            }));
        }
    };
    let distributed_guard = match crate::control::metadata_proposer::acquire_ddl_prepare_lease(
        state,
        handle.as_ref(),
    ) {
        Ok(guard) => guard,
        Err(error) => return Some(AbortReason::DdlPropose(error)),
    };

    for item in &buffered {
        if let Some((descriptor_id, prior_version)) =
            crate::control::lease::descriptor_id_and_prior_version(&item.entry, state)
            && prior_version > 0
            && let Err(error) = crate::control::lease::drain_for_ddl(
                state,
                descriptor_id,
                prior_version,
                crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            )
        {
            return Some(AbortReason::DdlPropose(error));
        }
    }
    let audits: Vec<_> = buffered.iter().map(|item| item.audit.clone()).collect();
    let entries: Vec<_> = buffered.into_iter().map(|item| item.entry).collect();
    let stamped = if state
        .cluster_version_view()
        .can_activate_feature(crate::control::rolling_upgrade::DESCRIPTOR_VERSIONING_VERSION)
    {
        crate::control::catalog_entry::descriptor_stamp::stamp_batch(
            entries,
            &state.hlc_clock,
            state.credentials.catalog(),
        )
    } else {
        entries
    };

    let mut sub_entries = Vec::with_capacity(stamped.len());
    for (entry, audit) in stamped.into_iter().zip(audits) {
        let payload = match crate::control::catalog_entry::encode(&entry) {
            Ok(payload) => payload,
            Err(error) => return Some(AbortReason::DdlPropose(error)),
        };
        sub_entries.push(match audit {
            Some(ctx) => MetadataEntry::CatalogDdlAudited {
                payload,
                auth_user_id: ctx.auth_user_id,
                auth_user_name: ctx.auth_user_name,
                sql_text: ctx.sql_text,
            },
            None => MetadataEntry::CatalogDdl { payload },
        });
    }
    let prepared = MetadataEntry::DdlPrepared {
        token: distributed_guard.token(),
        entry: Box::new(MetadataEntry::Batch {
            entries: sub_entries,
        }),
    };
    let raw = match encode_entry(&prepared) {
        Ok(raw) => raw,
        Err(error) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("DDL batch encode: {error}"),
            }));
        }
    };
    let log_index = match handle.propose(raw) {
        Ok(index) => index,
        Err(error) => {
            return Some(AbortReason::DdlPropose(crate::Error::Internal {
                detail: format!("DDL batch propose: {error}"),
            }));
        }
    };
    let watcher = state.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = tokio::task::block_in_place(|| {
        watcher.wait_for(
            log_index,
            crate::control::metadata_proposer::DEFAULT_PROPOSE_TIMEOUT,
        )
    });
    if !outcome.is_reached() {
        return Some(AbortReason::DdlPropose(crate::Error::Internal {
            detail: format!(
                "DDL batch propose timed out waiting for log index {log_index} (current: {})",
                watcher.current()
            ),
        }));
    }
    if state
        .metadata_ddl_applied_token
        .load(std::sync::atomic::Ordering::Acquire)
        != distributed_guard.token()
    {
        return Some(AbortReason::DdlPropose(crate::Error::Internal {
            detail: "DDL preparation ownership was superseded before apply".into(),
        }));
    }
    None
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

    fn sample_entry(name: &str) -> CatalogEntry {
        CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: name.to_string(),
        }
    }

    #[test]
    fn inactive_buffer_does_not_capture() {
        discard(); // ensure clean state
        assert!(!try_buffer(sample_entry("one")));
        assert!(!is_active());
    }

    #[test]
    fn active_buffer_captures() {
        activate();
        assert!(is_active());
        assert!(try_buffer(sample_entry("one")));
        assert!(try_buffer(sample_entry("two")));
        let buf = take().unwrap();
        assert_eq!(buf.len(), 2);
        assert!(matches!(
            &buf[0].entry,
            CatalogEntry::DeleteSequence { name, .. } if name == "one"
        ));
        assert!(matches!(
            &buf[1].entry,
            CatalogEntry::DeleteSequence { name, .. } if name == "two"
        ));
        assert!(!is_active());
    }

    #[test]
    fn discard_clears_buffer() {
        activate();
        try_buffer(sample_entry("one"));
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
