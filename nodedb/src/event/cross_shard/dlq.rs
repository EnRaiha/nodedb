// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard Dead Letter Queue.
//!
//! Failed cross-shard writes (after max retries) are persisted here.
//! Queryable via `SELECT * FROM _system.dead_letter_queue` and replayable
//! via `CALL replay_dead_letters(collection, since)`.

use std::collections::VecDeque;
use std::path::Path;

use redb::TableDefinition;
use tracing::warn;

use crate::event::redb_store::RedbStore;

/// redb table: entry_id (u64) → MessagePack-serialized DlqEntry.
const CROSS_SHARD_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("cross_shard_dlq");

/// Maximum DLQ entries per node.
const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// A cross-shard write that exhausted all retries.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CrossShardDlqEntry {
    /// Monotonic entry identifier.
    pub entry_id: u64,
    /// Tenant that owns the source data.
    pub tenant_id: u64,
    /// Collection that triggered this cross-shard write.
    pub source_collection: String,
    /// SQL statement that failed to execute on the target.
    pub sql: String,
    /// Source vShard ID.
    pub source_vshard: u32,
    /// Target vShard ID.
    pub target_vshard: u32,
    /// Target node ID.
    pub target_node: u64,
    /// Source LSN for idempotency correlation.
    pub source_lsn: u64,
    /// Source sequence number.
    pub source_sequence: u64,
    /// Body that emitted the request; with the source position it is the
    /// receiver's dedup key.
    pub origin: String,
    /// Last error message from the target.
    pub error: String,
    /// Total retry attempts before DLQ.
    pub retry_count: u32,
    /// Epoch ms when the entry was created.
    pub created_at: u64,
    /// Whether this entry has been resolved (replayed successfully).
    pub resolved: bool,
}

/// Parameters for enqueuing a new DLQ entry.
pub struct DlqEnqueueParams {
    pub tenant_id: u64,
    pub source_collection: String,
    pub sql: String,
    pub source_vshard: u32,
    pub target_vshard: u32,
    pub target_node: u64,
    pub source_lsn: u64,
    pub source_sequence: u64,
    pub origin: String,
    pub error: String,
    pub retry_count: u32,
}

/// Cross-shard Dead Letter Queue backed by redb.
///
/// Every mutation writes redb first and changes the in-memory index only
/// after the write commits, so the index never holds a change redb refused.
pub struct CrossShardDlq {
    store: RedbStore<CrossShardDlqEntry>,
    /// In-memory index for fast listing. Mirrors redb.
    entries: VecDeque<CrossShardDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
}

impl crate::storage::RedbBacked for CrossShardDlq {
    fn redb_database(&self) -> &redb::Database {
        self.store.redb_database()
    }
}

impl CrossShardDlq {
    /// Open or create the cross-shard DLQ at
    /// `{data_dir}/event_plane/cross_shard_dlq.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let store = RedbStore::open(
            data_dir,
            "cross_shard_dlq.redb",
            CROSS_SHARD_DLQ,
            "cross-shard DLQ",
        )?;
        let loaded = store.load()?;
        Ok(Self {
            store,
            entries: loaded.records,
            next_entry_id: loaded.next_key,
            max_entries: DEFAULT_MAX_ENTRIES,
        })
    }

    /// Enqueue a failed cross-shard write.
    ///
    /// At capacity the oldest entries are removed in the same redb
    /// transaction that writes the new one. On error nothing changes.
    pub fn enqueue(&mut self, params: DlqEnqueueParams) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = CrossShardDlqEntry {
            entry_id,
            tenant_id: params.tenant_id,
            source_collection: params.source_collection,
            sql: params.sql,
            source_vshard: params.source_vshard,
            target_vshard: params.target_vshard,
            target_node: params.target_node,
            source_lsn: params.source_lsn,
            source_sequence: params.source_sequence,
            origin: params.origin,
            error: params.error,
            retry_count: params.retry_count,
            created_at: now,
            resolved: false,
        };

        let evict_count = (self.entries.len() + 1).saturating_sub(self.max_entries);
        let evicted: Vec<u64> = self
            .entries
            .iter()
            .take(evict_count)
            .map(|e| e.entry_id)
            .collect();
        self.store.put_evicting(entry_id, &entry, &evicted)?;

        self.next_entry_id += 1;
        for _ in 0..evict_count {
            if let Some(old) = self.entries.pop_front() {
                warn!(
                    entry_id = old.entry_id,
                    collection = %old.source_collection,
                    "cross-shard DLQ capacity exceeded, evicting oldest entry"
                );
            }
        }
        self.entries.push_back(entry);
        Ok(entry_id)
    }

    /// List all unresolved DLQ entries.
    pub fn list_unresolved(&self) -> Vec<&CrossShardDlqEntry> {
        self.entries.iter().filter(|e| !e.resolved).collect()
    }

    /// List unresolved entries for a specific collection, optionally since a timestamp.
    pub fn list_for_collection(
        &self,
        collection: &str,
        since_epoch_ms: Option<u64>,
    ) -> Vec<&CrossShardDlqEntry> {
        self.entries
            .iter()
            .filter(|e| {
                !e.resolved
                    && e.source_collection == collection
                    && since_epoch_ms.is_none_or(|since| e.created_at >= since)
            })
            .collect()
    }

    /// Mark an entry as resolved (successfully replayed). Returns `Ok(false)`
    /// when no entry has `entry_id`. On error the entry stays unresolved.
    pub fn resolve(&mut self, entry_id: u64) -> crate::Result<bool> {
        let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) else {
            return Ok(false);
        };
        let mut updated = entry.clone();
        updated.resolved = true;
        self.store.put(entry_id, &updated)?;
        entry.resolved = true;
        Ok(true)
    }

    /// Number of entries (including resolved).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Number of unresolved entries.
    pub fn unresolved_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.resolved).count()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get entries ready for replay (unresolved, for a collection, since a time).
    /// Returns owned clones suitable for async replay.
    ///
    /// An entry older than [`super::dedup::DLQ_REPLAY_WINDOW_MS`] is never a
    /// candidate: its receiver can have pruned the dedup key, so a replay
    /// applies the write twice.
    pub fn replay_candidates(
        &self,
        collection: &str,
        since_epoch_ms: u64,
    ) -> Vec<CrossShardDlqEntry> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let oldest = now.saturating_sub(super::dedup::DLQ_REPLAY_WINDOW_MS);
        self.entries
            .iter()
            .filter(|e| {
                !e.resolved
                    && e.source_collection == collection
                    && e.created_at >= since_epoch_ms.max(oldest)
            })
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_params(collection: &str, lsn: u64) -> DlqEnqueueParams {
        DlqEnqueueParams {
            tenant_id: 1,
            source_collection: collection.into(),
            sql: format!("INSERT INTO audit (lsn) VALUES ({lsn})"),
            source_vshard: 3,
            target_vshard: 7,
            target_node: 2,
            source_lsn: lsn,
            source_sequence: lsn,
            origin: "trigger/1/audit".into(),
            error: "shard unavailable".into(),
            retry_count: 5,
        }
    }

    #[test]
    fn enqueue_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        let id = dlq.enqueue(make_params("orders", 100)).unwrap();
        assert_eq!(id, 1);
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.unresolved_count(), 1);

        let unresolved = dlq.list_unresolved();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].source_lsn, 100);
    }

    #[test]
    fn resolve_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        let id = dlq.enqueue(make_params("orders", 100)).unwrap();
        assert!(dlq.resolve(id).unwrap());
        assert_eq!(dlq.unresolved_count(), 0);
        assert_eq!(dlq.len(), 1); // Still present, just resolved.
    }

    #[test]
    fn list_for_collection() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        dlq.enqueue(make_params("orders", 100)).unwrap();
        dlq.enqueue(make_params("users", 200)).unwrap();
        dlq.enqueue(make_params("orders", 300)).unwrap();

        let orders = dlq.list_for_collection("orders", None);
        assert_eq!(orders.len(), 2);
        let users = dlq.list_for_collection("users", None);
        assert_eq!(users.len(), 1);
    }

    #[test]
    fn replay_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();

        dlq.enqueue(make_params("orders", 100)).unwrap();
        dlq.enqueue(make_params("orders", 200)).unwrap();

        // All candidates (since epoch 0).
        let candidates = dlq.replay_candidates("orders", 0);
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn entries_past_the_replay_window_are_not_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();
        dlq.enqueue(make_params("orders", 100)).unwrap();
        for entry in dlq.entries.iter_mut() {
            entry.created_at = 0;
        }
        assert!(dlq.replay_candidates("orders", 0).is_empty());
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = CrossShardDlq::open(dir.path()).unwrap();
            dlq.enqueue(make_params("orders", 100)).unwrap();
            dlq.enqueue(make_params("orders", 200)).unwrap();
        }
        let dlq = CrossShardDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.len(), 2);
        assert_eq!(dlq.unresolved_count(), 2);
    }

    #[test]
    fn eviction_on_overflow() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = CrossShardDlq::open(dir.path()).unwrap();
        dlq.max_entries = 3;

        for i in 1..=5 {
            dlq.enqueue(make_params("orders", i * 100)).unwrap();
        }
        assert_eq!(dlq.len(), 3);
        // Oldest entries evicted.
        let entries: Vec<u64> = dlq.list_unresolved().iter().map(|e| e.source_lsn).collect();
        assert_eq!(entries, vec![300, 400, 500]);
    }
}
