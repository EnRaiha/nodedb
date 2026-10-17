// SPDX-License-Identifier: BUSL-1.1

use super::core::WalManager;

impl WalManager {
    /// Write an audit entry durably to the dedicated audit WAL.
    ///
    /// `data_lsn` is the data WAL LSN this audit entry corresponds to.
    /// The audit entry is serialized, appended, and fsynced before returning.
    /// Returns the audit WAL LSN of the record, or 0 when no audit WAL is open.
    pub fn append_audit_durable(&self, audit_bytes: &[u8], data_lsn: u64) -> crate::Result<u64> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.append_durable(audit_bytes, data_lsn),
            None => Ok(0),
        }
    }

    /// Recover all audit WAL entries for crash recovery.
    ///
    /// Returns `(data_lsn, audit_entry_bytes)` pairs in LSN order.
    pub fn recover_audit_entries(&self) -> crate::Result<Vec<(u64, Vec<u8>)>> {
        if let Some(ref audit_wal) = self.audit_wal {
            audit_wal.recover()
        } else {
            Ok(Vec::new())
        }
    }

    /// Drop every audit WAL record at or below `audit_lsn`.
    ///
    /// The caller must pass only an LSN whose record, and every earlier one, the catalog has committed.
    pub fn truncate_audit_through(&self, audit_lsn: u64) -> crate::Result<()> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.truncate_through(audit_lsn),
            None => Ok(()),
        }
    }

    /// The highest audit WAL LSN written, or 0 when none is.
    pub fn audit_last_lsn(&self) -> crate::Result<u64> {
        match self.audit_wal {
            Some(ref audit_wal) => audit_wal.last_lsn(),
            None => Ok(0),
        }
    }

    /// Whether the durable audit WAL is available.
    pub fn has_audit_wal(&self) -> bool {
        self.audit_wal.is_some()
    }
}
