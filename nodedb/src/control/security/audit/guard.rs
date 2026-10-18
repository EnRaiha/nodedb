// SPDX-License-Identifier: BUSL-1.1

//! Lock guard for the shared `AuditLog` that exports recorded entries after it unlocks.

use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex, MutexGuard};

use super::entry::AuditEntry;
use super::export_sink::AuditExportSink;
use super::log::AuditLog;

/// Exclusive access to the shared `AuditLog`.
///
/// On drop it moves the entries recorded meanwhile out of the log.
/// Fields drop in declaration order, so the lock releases before `export` runs.
/// So export never runs under the audit lock.
/// An entry recorded under a plain `Mutex::lock` is exported by the next drop of this guard.
pub struct AuditLogGuard<'a> {
    guard: MutexGuard<'a, AuditLog>,
    export: PendingExport,
}

/// Entries waiting for export. Its drop hands them to the sink.
#[derive(Default)]
struct PendingExport {
    entries: Vec<AuditEntry>,
    sink: Option<Arc<dyn AuditExportSink>>,
}

impl Drop for PendingExport {
    fn drop(&mut self) {
        if let Some(sink) = self.sink.take() {
            for entry in self.entries.drain(..) {
                sink.export(entry);
            }
        }
    }
}

/// Lock `audit`. A poisoned lock still yields the log, because dropping a security event is worse.
pub fn lock_audit_log(audit: &Mutex<AuditLog>) -> AuditLogGuard<'_> {
    let guard = match audit.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("audit log mutex poisoned, recovering");
            poisoned.into_inner()
        }
    };
    AuditLogGuard {
        guard,
        export: PendingExport::default(),
    }
}

impl Deref for AuditLogGuard<'_> {
    type Target = AuditLog;

    fn deref(&self) -> &AuditLog {
        &self.guard
    }
}

impl DerefMut for AuditLogGuard<'_> {
    fn deref_mut(&mut self) -> &mut AuditLog {
        &mut self.guard
    }
}

impl Drop for AuditLogGuard<'_> {
    fn drop(&mut self) {
        let (entries, sink) = self.guard.take_export_pending();
        self.export = PendingExport { entries, sink };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::audit::AuditEvent;

    /// Records each export and whether the audit lock was free at that moment.
    struct ProbeSink {
        audit: Arc<Mutex<AuditLog>>,
        seen: Mutex<Vec<(u64, bool)>>,
        active: bool,
    }

    impl AuditExportSink for ProbeSink {
        fn is_active(&self) -> bool {
            self.active
        }

        fn export(&self, entry: AuditEntry) {
            let unlocked = self.audit.try_lock().is_ok();
            self.seen
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push((entry.seq, unlocked));
        }
    }

    fn probe(active: bool) -> (Arc<Mutex<AuditLog>>, Arc<ProbeSink>) {
        let audit = Arc::new(Mutex::new(AuditLog::new(100)));
        let sink = Arc::new(ProbeSink {
            audit: Arc::clone(&audit),
            seen: Mutex::new(Vec::new()),
            active,
        });
        lock_audit_log(&audit).set_export_sink(Arc::clone(&sink) as Arc<dyn AuditExportSink>);
        (audit, sink)
    }

    #[test]
    fn export_runs_after_the_lock_releases() {
        let (audit, sink) = probe(true);
        {
            let mut log = lock_audit_log(&audit);
            log.record(AuditEvent::AdminAction, None, "src", "a");
            log.record(AuditEvent::AuthSuccess, None, "src", "b");
            assert!(
                sink.seen.lock().unwrap().is_empty(),
                "no export under the lock"
            );
        }
        assert_eq!(*sink.seen.lock().unwrap(), vec![(1, true), (2, true)]);
    }

    #[test]
    fn plain_lock_entries_export_on_the_next_guard_drop() {
        let (audit, sink) = probe(true);
        audit
            .lock()
            .unwrap()
            .record(AuditEvent::AdminAction, None, "src", "plain");
        drop(lock_audit_log(&audit));
        assert_eq!(*sink.seen.lock().unwrap(), vec![(1, true)]);
    }

    #[test]
    fn inactive_sink_gets_nothing() {
        let (audit, sink) = probe(false);
        lock_audit_log(&audit).record(AuditEvent::AdminAction, None, "src", "off");
        assert!(sink.seen.lock().unwrap().is_empty());
        assert!(audit.lock().unwrap().take_export_pending().0.is_empty());
    }
}
