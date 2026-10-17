// SPDX-License-Identifier: BUSL-1.1

//! Durable sink every recorded audit entry is appended to before its record call returns.

use super::entry::AuditEntry;
use crate::wal::WalManager;

/// Durable append target for audit entries.
///
/// `AuditLog` calls `append_batch` under its own lock, once per record call, in seq order.
/// So positions ascend with seq, and WAL order equals seq order.
/// The record call then waits in `sync_through`, after it releases the audit lock.
/// The sink holds every entry no flush has committed, and boot replays it into the catalog.
pub trait AuditWalSink: Send + Sync {
    /// Append every entry of `entries` in order, with no sync.
    ///
    /// Returns the position of each entry, in order.
    fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>>;

    /// Wait until every entry at or below `position` is durable.
    ///
    /// Concurrent callers share one sync. A failed sync reaches every caller it covered.
    fn sync_through(&self, position: u64) -> crate::Result<()>;

    /// Drop every record at or below `position`.
    ///
    /// The caller must pass only a position whose entry, and every earlier one, the catalog has committed.
    fn truncate_through(&self, position: u64) -> crate::Result<()>;
}

impl AuditWalSink for WalManager {
    fn append_batch(&self, entries: &[AuditEntry]) -> crate::Result<Vec<u64>> {
        let data_lsn = self.next_lsn().as_u64();
        let records = entries
            .iter()
            .map(|entry| {
                zerompk::to_msgpack_vec(entry)
                    .map(|bytes| (bytes, data_lsn))
                    .map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("audit entry serialization failed: {e}"),
                    })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        self.append_audit_batch(&records)
    }

    fn sync_through(&self, position: u64) -> crate::Result<()> {
        self.sync_audit_through(position)
    }

    fn truncate_through(&self, position: u64) -> crate::Result<()> {
        self.truncate_audit_through(position)
    }
}
