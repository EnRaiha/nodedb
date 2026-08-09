// SPDX-License-Identifier: BUSL-1.1

//! `MOVE TENANT` journal access.
//!
//! The journal makes `MOVE TENANT` crash-safe: on startup, the recovery module
//! scans for in-progress entries and either completes or compensates each one.
//!
//! The table definition and the persisted record types belong to the catalog
//! (`control::security::catalog::move_tenant_journal_types`) because the
//! catalog owns the redb table — it creates it at bootstrap and is the only
//! module that can reach `SystemCatalog::db`. What lives here is only the
//! workflow-facing surface: thin wrappers over
//! `SystemCatalog::move_tenant_journal_*`.

use crate::control::security::catalog::{MoveTenantJournalEntry, SystemCatalog};
use crate::types::TenantId;

/// Load the journal entry for `tenant_id`, if one exists.
pub fn load_journal_entry(
    catalog: &SystemCatalog,
    tenant_id: TenantId,
) -> crate::Result<Option<MoveTenantJournalEntry>> {
    catalog.move_tenant_journal_load(tenant_id.as_u64())
}

/// Write or overwrite the journal entry for `entry.tenant_id`.
pub fn save_journal_entry(
    catalog: &SystemCatalog,
    entry: &MoveTenantJournalEntry,
) -> crate::Result<()> {
    catalog.move_tenant_journal_save(entry)
}

/// Remove the journal entry for `tenant_id` (move completed or compensated).
pub fn delete_journal_entry(catalog: &SystemCatalog, tenant_id: TenantId) -> crate::Result<()> {
    catalog.move_tenant_journal_delete(tenant_id.as_u64())
}

/// Cleanup-path delete: best-effort but visible. A failure here means the next
/// startup will re-process the entry (the workflow is idempotent), but we want
/// the failure observable in logs rather than silently swallowed via `let _ =`.
pub fn delete_journal_entry_logged(catalog: &SystemCatalog, tenant_id: TenantId) {
    if let Err(e) = delete_journal_entry(catalog, tenant_id) {
        tracing::warn!(
            tenant = tenant_id.as_u64(),
            error = %e,
            "move_tenant: failed to delete journal entry; will be retried on next startup"
        );
    }
}

/// Scan all in-progress journal entries. Used by startup recovery.
pub fn scan_all_journal_entries(
    catalog: &SystemCatalog,
) -> crate::Result<Vec<MoveTenantJournalEntry>> {
    catalog.move_tenant_journal_scan_all()
}
