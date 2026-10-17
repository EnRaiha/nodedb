// SPDX-License-Identifier: BUSL-1.1

//! Which DML audit rows the durable audit log already holds.
//!
//! A DML audit row carries the key of the event it records, in its detail
//! text. The durable audit log is the audit sink's ledger: each row reaches
//! the audit WAL with its key in one append, and a flush moves it to the
//! system catalog. The WAL is truncated through what the catalog holds, so
//! startup reads the keys above each core's watermark from both. A replayed
//! event whose key is present was audited before the restart and is not
//! audited again.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::control::security::audit::{AuditEntry, AuditEvent};

use super::key::SinkEventKey;

/// The text that introduces a key in a DML audit row's detail.
const KEY_MARKER: &str = " key=";

/// The detail suffix that names `key`.
pub fn detail_suffix(key: &SinkEventKey) -> String {
    format!("{KEY_MARKER}{}", key.to_token())
}

/// The key a DML audit row's detail names, if any.
fn key_of(detail: &str) -> Option<SinkEventKey> {
    let at = detail.rfind(KEY_MARKER)?;
    SinkEventKey::from_token(detail.get(at + KEY_MARKER.len()..)?)
}

/// The `(core, lsn)` of the event a DML audit row's detail names, if any.
///
/// The catalog indexes DML audit rows by this position, so boot reads only rows above each core's floor.
pub fn dml_event_position(detail: &str) -> Option<(u32, u64)> {
    key_of(detail).map(|key| (key.core, key.lsn))
}

/// Keys of DML audit rows written above each core's watermark.
#[derive(Debug, Default)]
pub struct AuditedKeys {
    keys: Mutex<HashSet<SinkEventKey>>,
}

impl AuditedKeys {
    /// Decode the recovered audit WAL records.
    pub fn decode_recovered(records: &[(u64, Vec<u8>)]) -> crate::Result<Vec<AuditEntry>> {
        records
            .iter()
            .map(|(_, bytes)| {
                zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("recovered audit entry: {e}"),
                })
            })
            .collect()
    }

    /// Collect the keys of the durable audit entries that lie above `watermark(core)`.
    ///
    /// `entries` is the catalog's DML audit rows together with the audit WAL's records.
    /// A key both hold counts once.
    pub fn from_entries<'a>(
        entries: impl IntoIterator<Item = &'a AuditEntry>,
        watermark: impl Fn(u32) -> u64,
    ) -> Self {
        let mut keys = HashSet::new();
        for entry in entries {
            if entry.event != AuditEvent::DmlAudit {
                continue;
            }
            if let Some(key) = key_of(&entry.detail)
                && key.lsn > watermark(key.core)
            {
                keys.insert(key);
            }
        }
        Self {
            keys: Mutex::new(keys),
        }
    }

    /// Whether the durable audit log already records `key`.
    pub fn contains(&self, key: &SinkEventKey) -> bool {
        self.keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(key)
    }

    /// Drop the keys of `core` at or below `through`.
    pub fn prune(&self, core: u32, through: u64) {
        self.keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|key| key.core != core || key.lsn > through);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(core: u32, lsn: u64) -> SinkEventKey {
        SinkEventKey {
            core,
            lsn,
            occurrence: 0,
            delete: false,
            collection: "orders".into(),
            row_kind: 0,
            row: "o-1".into(),
        }
    }

    #[test]
    fn a_detail_carries_its_key() {
        let detail = format!("INSERT orders:o-1 lsn=7{}", detail_suffix(&key(1, 7)));
        assert_eq!(key_of(&detail), Some(key(1, 7)));
        assert_eq!(key_of("INSERT orders:o-1 lsn=7"), None);
    }

    #[test]
    fn keys_at_or_below_a_watermark_are_dropped() {
        let audited = AuditedKeys::default();
        audited
            .keys
            .lock()
            .expect("lock")
            .extend([key(0, 5), key(0, 9), key(1, 5)]);
        audited.prune(0, 5);
        assert!(!audited.contains(&key(0, 5)));
        assert!(audited.contains(&key(0, 9)));
        assert!(audited.contains(&key(1, 5)));
    }
}
