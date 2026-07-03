// SPDX-License-Identifier: BUSL-1.1

//! `_system.move_tenant_journal` redb table.
//!
//! Key: `tenant_id (u64)`.
//! Value: MessagePack-serialized [`MoveTenantJournalEntry`].
//!
//! The journal makes `MOVE TENANT` crash-safe: on startup, the recovery module
//! scans for in-progress entries and either completes or compensates each one.
//!
//! The CRUD methods are thin wrappers around `SystemCatalog::move_tenant_journal_*`
//! which live inside the catalog module (the only place that can access
//! `SystemCatalog::db` directly).

use redb::TableDefinition;

use crate::control::security::catalog::SystemCatalog;
use crate::types::TenantId;

pub(crate) const MOVE_TENANT_JOURNAL: TableDefinition<u64, &[u8]> =
    TableDefinition::new("_system.move_tenant_journal");

/// Phase of the in-progress move at the time the journal entry was last written.
///
/// Every `match` on this enum must be exhaustive — no `_ =>` arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
pub enum MovePhase {
    /// Pre-flight verified; drain about to start.
    Preflight = 1,
    /// Drain issued; waiting for sessions to wind down.
    Drain = 2,
    /// Drain complete; snapshot in progress.
    Snapshot = 3,
    /// Snapshot complete; cutover Raft proposal in progress.
    Cutover = 4,
    /// Cutover succeeded; tenant is in the target database.
    Resumed = 5,
}

/// Persisted state for a single in-progress `MOVE TENANT` operation.
#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack, Debug, Clone)]
#[msgpack(map)]
pub struct MoveTenantJournalEntry {
    pub tenant_id: u64,
    pub tenant_name: String,
    pub source_db_id: u64,
    pub source_db_name: String,
    pub target_db_id: u64,
    pub target_db_name: String,
    pub phase: MovePhase,
    /// WAL LSN at the time this entry was last written.
    pub last_durable_lsn: u64,
    /// Key under which the in-cluster temporary snapshot was stored, if any.
    #[msgpack(default)]
    pub temp_snapshot_key: Option<String>,
}

impl MoveTenantJournalEntry {
    /// Return a clone of this entry with the given phase.
    pub fn with_phase(self, phase: MovePhase) -> Self {
        Self { phase, ..self }
    }

    /// Return a clone of this entry with a temp snapshot key set.
    pub fn with_temp_snapshot_key(self, key: String) -> Self {
        Self {
            temp_snapshot_key: Some(key),
            ..self
        }
    }
}

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
