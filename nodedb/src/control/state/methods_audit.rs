// SPDX-License-Identifier: BUSL-1.1

//! Audit recording and memory-estimate update methods for `SharedState`.

use std::sync::Arc;

use tracing::{error, warn};

use super::SharedState;

impl SharedState {
    /// Reset per-second rate counters. Called by a 1-second timer.
    pub fn reset_tenant_rate_counters(&self) {
        match self.tenants.lock() {
            Ok(mut t) => t.reset_rate_counters(),
            Err(poisoned) => poisoned.into_inner().reset_rate_counters(),
        }
    }

    /// Record an audit event (best-effort) with full database context.
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it.
    pub fn audit_record_with_db(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) {
        self.audit_record_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &crate::control::security::audit::AuditAuth::default(),
        );
    }

    /// Record an audit event (best-effort) that carries the authenticated user and session.
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it.
    pub fn audit_record_with_auth(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) {
        let (durable, seq) = self.audit_record_with_backpressure(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth,
        );
        if let Err(e) = durable {
            error!(error = %e, seq, "audit WAL write failed — entry recorded in-memory only");
            crate::diag::audit_wal_append_failed(&e, seq);
        }
    }

    /// Record an audit event with strict durability and full database context.
    ///
    /// Returns an error if the durable audit WAL write fails.
    /// A buffer past its backpressure point makes this wait for a flush, with a time bound.
    /// The wait holds no lock, and this call never runs a flush itself.
    pub fn audit_record_with_db_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        self.audit_record_with_backpressure(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &crate::control::security::audit::AuditAuth::default(),
        )
        .0
    }

    /// Record an audit event, then wait for a flush when the buffer is past its backpressure point.
    ///
    /// Returns the WAL result and the entry's seq.
    fn audit_record_with_backpressure(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) -> (crate::Result<()>, u64) {
        let (durable, seq, backpressure) = self.audit_record_buffered_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            auth,
        );
        if backpressure {
            self.await_audit_backpressure();
        }
        (durable, seq)
    }

    /// Record an audit event into the buffer and the audit WAL, with no backpressure wait.
    ///
    /// Returns the WAL result, the entry's seq, and whether the buffer is past its backpressure point.
    /// The flush path uses this directly, because a flush must never wait for a flush.
    pub(super) fn audit_record_buffered(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) -> (crate::Result<()>, u64, bool) {
        self.audit_record_buffered_with_auth(
            event,
            tenant_id,
            database_id,
            source,
            detail,
            &crate::control::security::audit::AuditAuth::default(),
        )
    }

    /// [`Self::audit_record_buffered`] for an entry that carries the authenticated user and session.
    fn audit_record_buffered_with_auth(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
        auth: &crate::control::security::audit::AuditAuth,
    ) -> (crate::Result<()>, u64, bool) {
        // The guard exports the entry to SIEM after it unlocks.
        let mut log = crate::control::security::audit::lock_audit_log(&self.audit);
        let durable =
            log.record_with_auth_durable(event, tenant_id, database_id, source, detail, auth);
        let (seq, backpressure) = (log.last_seq(), log.needs_backpressure());
        drop(log);
        (durable.map(|_| ()), seq, backpressure)
    }

    /// Record one Event Plane audit record handed over by the recorder channel, with no backpressure wait.
    ///
    /// Runs on the recorder's blocking task, never on the Event Plane.
    pub fn record_pending_audit(&self, record: &crate::control::security::audit::PendingAudit) {
        let (durable, seq, _) = self.audit_record_buffered(
            record.event.clone(),
            record.tenant_id,
            record.database_id,
            &record.source,
            &record.detail,
        );
        if let Err(e) = durable {
            error!(error = %e, seq, "audit WAL write failed — entry recorded in-memory only");
            crate::diag::audit_wal_append_failed(&e, seq);
        }
    }

    /// Record Event Plane audit records in order, then apply the backpressure wait once.
    pub fn record_pending_audits(&self, records: &[crate::control::security::audit::PendingAudit]) {
        for record in records {
            self.record_pending_audit(record);
        }
        self.await_audit_backpressure();
    }

    /// Wait for a flush when the buffer is past its backpressure point, with a time bound.
    ///
    /// The wait holds no lock. With no flush executor running it returns at once.
    fn await_audit_backpressure(&self) {
        let control = {
            let log = crate::control::security::audit::lock_audit_log(&self.audit);
            if !log.needs_backpressure() {
                return;
            }
            log.flush_control()
        };
        if control.executor_attached() && !control.await_flush() {
            warn!("audit flush did not finish within the backpressure wait, entries stay buffered");
        }
    }

    /// The Control Plane recorder for Event Plane audit records. The first call spawns it.
    ///
    /// The task holds weak references, so it never keeps this state alive.
    /// It records one entry at a time, so a failure partway returns only the unrecorded tail.
    pub fn audit_recorder(self: &Arc<Self>) -> &crate::control::security::audit::AuditRecorder {
        use crate::control::security::audit::PendingAudit;

        self.audit_recorder.get_or_init(|| {
            let for_record = Arc::downgrade(self);
            let for_wait = Arc::downgrade(self);
            crate::control::security::audit::AuditRecorder::spawn(
                crate::control::security::audit::recorder::RECORDER_CHANNEL_CAPACITY,
                Arc::new(move |record: &PendingAudit| match for_record.upgrade() {
                    Some(state) => {
                        state.record_pending_audit(record);
                        true
                    }
                    None => false,
                }),
                Arc::new(move || {
                    if let Some(state) = for_wait.upgrade() {
                        state.await_audit_backpressure();
                    }
                }),
            )
        })
    }

    /// Record an audit event (best-effort).
    ///
    /// Writes to both the in-memory cache and the durable audit WAL (if available).
    /// On audit WAL failure, logs an error but does not propagate it. Use
    /// [`audit_record_strict`] when the caller must abort on audit failure
    /// (e.g. data-modifying DDL where the accounting standard requires atomic
    /// audit + data durability).
    pub fn audit_record(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) {
        self.audit_record_with_db(event, tenant_id, None, source, detail);
    }

    /// Record an audit event with strict durability.
    ///
    /// Returns an error if the durable audit WAL write fails. Callers that
    /// guard data mutations MUST use this and abort on failure — the accounting
    /// standard requires: "If the audit write fails, the data write also fails."
    pub fn audit_record_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        self.audit_record_with_db_strict(event, tenant_id, None, source, detail)
    }

    /// Update per-tenant memory estimates.
    pub fn update_tenant_memory_estimates(&self) {
        let total_allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap_or(0) as u64;

        let mut tenants = match self.tenants.lock() {
            Ok(t) => t,
            Err(p) => p.into_inner(),
        };

        let tenant_requests: Vec<(crate::types::TenantId, u64)> = {
            let users = self.credentials.list_user_details();
            let mut seen = std::collections::HashSet::new();
            let mut result = Vec::new();
            for user in &users {
                if seen.insert(user.tenant_id) {
                    let total = tenants
                        .usage(user.tenant_id)
                        .map_or(0, |u| u.total_requests);
                    result.push((user.tenant_id, total));
                }
            }
            result
        };

        let total_reqs: u64 = tenant_requests.iter().map(|(_, r)| *r).sum();
        if total_reqs == 0 {
            return;
        }

        for (tid, reqs) in &tenant_requests {
            let proportion = *reqs as f64 / total_reqs as f64;
            let estimated_bytes = (total_allocated as f64 * proportion) as u64;
            tenants.update_memory(*tid, estimated_bytes);
        }
    }
}
