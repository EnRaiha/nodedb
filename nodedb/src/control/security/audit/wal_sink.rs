// SPDX-License-Identifier: BUSL-1.1

//! Durable sink every recorded audit entry is appended to before its record call returns.

use super::entry::AuditEntry;
use crate::wal::WalManager;

/// Durable append target for audit entries.
///
/// `AuditLog` calls `append` under its own lock, once per entry, in seq order.
/// So positions ascend with seq.
/// The sink holds every entry no flush has committed, and boot replays it into the catalog.
pub trait AuditWalSink: Send + Sync {
    /// Append one entry durably and return its position.
    /// The entry is on stable storage when this returns `Ok`.
    fn append(&self, entry: &AuditEntry) -> crate::Result<u64>;

    /// Drop every record at or below `position`.
    ///
    /// The caller must pass only a position whose entry, and every earlier one, the catalog has committed.
    fn truncate_through(&self, position: u64) -> crate::Result<()>;
}

impl AuditWalSink for WalManager {
    fn append(&self, entry: &AuditEntry) -> crate::Result<u64> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("audit entry serialization failed: {e}"),
        })?;
        let data_lsn = self.next_lsn().as_u64();
        self.append_audit_durable(&bytes, data_lsn)
    }

    fn truncate_through(&self, position: u64) -> crate::Result<()> {
        self.truncate_audit_through(position)
    }
}
