// SPDX-License-Identifier: BUSL-1.1

//! Audit recording and memory-estimate update methods for `SharedState`.

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
        if let Err(e) =
            self.audit_record_with_db_strict(event, tenant_id, database_id, source, detail)
        {
            error!(error = %e, "audit WAL write failed — entry recorded in-memory only");
        }
    }

    /// Record an audit event with strict durability and full database context.
    ///
    /// Returns an error if the durable audit WAL write fails.
    pub fn audit_record_with_db_strict(
        &self,
        event: crate::control::security::audit::AuditEvent,
        tenant_id: Option<crate::types::TenantId>,
        database_id: Option<nodedb_types::DatabaseId>,
        source: &str,
        detail: &str,
    ) -> crate::Result<()> {
        let entry = match self.audit.lock() {
            Ok(mut log) => {
                log.record_with_database(event, tenant_id, database_id, source, detail);
                log.all().back().cloned()
            }
            Err(poisoned) => {
                warn!("audit log mutex poisoned, recovering");
                let mut log = poisoned.into_inner();
                log.record_with_database(event, tenant_id, database_id, source, detail);
                log.all().back().cloned()
            }
        };

        // Write to durable audit WAL — failure is a hard error.
        if let Some(ref entry) = entry {
            // SIEM export. This is the single choke point every audit event
            // passes through, so it is the only place the exporter has to be
            // fed. `is_configured()` is checked first so an unconfigured
            // exporter — the default — costs one atomic-free bool read and
            // never clones the entry.
            if self.siem.is_configured() {
                if entry.event.is_auth_event() {
                    self.siem.push_auth(entry.clone());
                } else {
                    self.siem.push_audit(entry.clone());
                }
            }

            let bytes =
                zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("audit entry serialization failed: {e}"),
                })?;
            let data_lsn = self.wal.next_lsn().as_u64();
            self.wal.append_audit_durable(&bytes, data_lsn)?;
        }
        Ok(())
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

    /// Flush in-memory audit entries to the persistent catalog.
    pub fn flush_audit_log(&self) {
        let entries = match self.audit.lock() {
            Ok(mut log) => log.drain_for_persistence(),
            Err(poisoned) => {
                warn!("audit log mutex poisoned during flush, recovering");
                poisoned.into_inner().drain_for_persistence()
            }
        };

        if entries.is_empty() {
            return;
        }

        {
            let catalog = self.credentials.catalog();
            let stored: Vec<crate::control::security::catalog::StoredAuditEntry> = entries
                .iter()
                .map(|e| crate::control::security::catalog::StoredAuditEntry {
                    seq: e.seq,
                    timestamp_us: e.timestamp_us,
                    event: format!("{:?}", e.event),
                    tenant_id: e.tenant_id.map(|t| t.as_u64()),
                    database_id: e.database_id.map(|d| d.as_u64()),
                    source: e.source.clone(),
                    detail: e.detail.clone(),
                    prev_hash: e.prev_hash.clone(),
                })
                .collect();

            if let Err(e) = catalog.append_audit_entries(&stored) {
                warn!(error = %e, count = stored.len(), "failed to persist audit entries");
                if let Ok(mut log) = self.audit.lock() {
                    for entry in entries {
                        log.record_with_database(
                            entry.event,
                            entry.tenant_id,
                            entry.database_id,
                            &entry.source,
                            &entry.detail,
                        );
                    }
                }
            } else {
                tracing::debug!(count = stored.len(), "flushed audit entries to catalog");

                // Age-based pruning: remove entries older than retention window.
                if self.audit_retention_days > 0 {
                    let retention_us = self.audit_retention_days as u64 * 86400 * 1_000_000;
                    let now_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;
                    let cutoff = now_us.saturating_sub(retention_us);
                    match catalog.prune_audit_before(cutoff) {
                        Ok(0) => {}
                        Ok(n) => {
                            tracing::info!(
                                pruned = n,
                                days = self.audit_retention_days,
                                "pruned old audit entries"
                            );
                        }
                        Err(e) => warn!(error = %e, "failed to prune old audit entries"),
                    }
                }

                // Count-based pruning: trim to audit_max_entries ceiling.
                if self.audit_max_entries > 0 {
                    match catalog.prune_audit_to_count(self.audit_max_entries) {
                        Ok((0, _, _)) => {}
                        Ok((pruned_count, last_deleted_hash, oldest_kept_seq)) => {
                            tracing::info!(
                                pruned = pruned_count,
                                oldest_kept_seq,
                                "pruned audit entries by count cap"
                            );
                            // Emit checkpoint so the surviving chain stays
                            // verifiable.
                            if let Ok(mut log) = self.audit.lock() {
                                let seq = log.allocate_seq();
                                let detail = format!(
                                    "pruned_count={pruned_count} oldest_kept_seq={oldest_kept_seq}"
                                );
                                let entry = crate::control::security::audit::AuditEntry {
                                    seq,
                                    timestamp_us: crate::control::security::audit::entry::now_us(),
                                    event:
                                        crate::control::security::audit::AuditEvent::AuditCheckpoint,
                                    tenant_id: None,
                                    database_id: None,
                                    auth_user_id: String::new(),
                                    auth_user_name: String::new(),
                                    session_id: String::new(),
                                    source: "retention".to_string(),
                                    detail,
                                    prev_hash: last_deleted_hash,
                                };
                                log.push_checkpoint(entry);
                            }
                        }
                        Err(e) => warn!(error = %e, "failed to prune audit entries by count"),
                    }
                }
            }
        }
    }
}
