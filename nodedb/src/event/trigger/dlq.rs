// SPDX-License-Identifier: BUSL-1.1

//! Dead-Letter Queue for failed async trigger events.
//!
//! When an async trigger's DML fails after max retries, the failed event
//! is enqueued here with full context for debugging and manual replay.
//! Separate from the sync DLQ (which handles CRDT constraint violations).
//!
//! Bounded per tenant: oldest entries evicted when capacity is reached.
//! Persisted to redb for durability across restarts.

use std::collections::VecDeque;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::TableDefinition;
use tracing::{debug, warn};

use crate::event::action::FailedAction;
use crate::event::redb_store::RedbStore;

/// redb table: monotonic entry_id → MessagePack-serialized `TriggerDlqEntry`.
const TRIGGER_DLQ: TableDefinition<u64, &[u8]> = TableDefinition::new("trigger_dlq");

/// Maximum DLQ entries per node (bounded to prevent unbounded growth).
const DEFAULT_MAX_ENTRIES: usize = 100_000;

/// An action that exhausted its retries, kept for an operator to inspect and
/// put back.
///
/// The action carries its own tenant, collection, row, and source position, so
/// none of that is copied alongside it — a second copy would be one more thing
/// that can disagree with what a requeue actually re-runs.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct TriggerDlqEntry {
    /// Unique entry ID (monotonic within this node).
    pub entry_id: u64,
    /// The action, as it will be re-run if requeued.
    pub action: FailedAction,
    /// Timestamp (Unix epoch millis) when the entry was created.
    pub created_at: u64,
    /// Whether this entry has been resolved, by an operator or by a requeue.
    pub resolved: bool,
}

impl TriggerDlqEntry {
    /// Tenant that owns the action.
    pub fn tenant_id(&self) -> u64 {
        self.action.context.tenant_id
    }

    /// Collection whose write produced the action.
    pub fn collection(&self) -> &str {
        &self.action.context.collection
    }

    /// The trigger or event definition the action belongs to.
    pub fn owner(&self) -> &str {
        self.action.owner()
    }

    /// Error text from the attempt that exhausted the action's retries.
    pub fn error(&self) -> &str {
        &self.action.last_error
    }

    /// Attempts made before the action was dead-lettered.
    pub fn retry_count(&self) -> u32 {
        self.action.attempts
    }
}

/// Why an entry could not be taken out of the DLQ for another attempt.
#[derive(Debug, thiserror::Error)]
pub enum RequeueTakeError {
    #[error("no dead-letter entry {entry_id}")]
    NotFound { entry_id: u64 },

    #[error("dead-letter entry {entry_id} is already resolved")]
    AlreadyResolved { entry_id: u64 },

    /// redb refused the resolved mark. The entry stays unresolved.
    #[error("dead-letter entry {entry_id} could not be marked resolved: {source}")]
    Persist {
        entry_id: u64,
        #[source]
        source: Box<crate::Error>,
    },
}

/// Trigger dead-letter queue.
///
/// Every mutation writes redb first and changes the in-memory index only
/// after the write commits, so the index never holds a change redb refused.
pub struct TriggerDlq {
    store: RedbStore<TriggerDlqEntry>,
    /// In-memory index for fast listing. Mirrors redb.
    entries: VecDeque<TriggerDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
}

impl crate::storage::RedbBacked for TriggerDlq {
    fn redb_database(&self) -> &redb::Database {
        self.store.redb_database()
    }
}

impl TriggerDlq {
    /// Open or create the trigger DLQ at `{data_dir}/event_plane/trigger_dlq.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let store = RedbStore::open(data_dir, "trigger_dlq.redb", TRIGGER_DLQ, "trigger DLQ")?;
        let loaded = store.load()?;
        debug!(
            entries = loaded.records.len(),
            next_id = loaded.next_key,
            "trigger DLQ loaded"
        );
        Ok(Self {
            store,
            entries: loaded.records,
            next_entry_id: loaded.next_key,
            max_entries: DEFAULT_MAX_ENTRIES,
        })
    }

    /// Record an action that has exhausted its retries.
    ///
    /// At capacity the oldest entries are removed in the same redb
    /// transaction that writes the new one. On error nothing changes.
    pub fn enqueue(&mut self, action: FailedAction) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        let entry = TriggerDlqEntry {
            entry_id,
            action,
            created_at: now_ms(),
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
                    owner = %old.owner(),
                    "trigger DLQ evicted oldest entry (at capacity)"
                );
            }
        }
        debug!(
            entry_id,
            owner = %entry.owner(),
            "deferred action sent to DLQ"
        );
        self.entries.push_back(entry);
        Ok(entry_id)
    }

    /// List all unresolved DLQ entries.
    pub fn list_unresolved(&self) -> Vec<&TriggerDlqEntry> {
        self.entries.iter().filter(|e| !e.resolved).collect()
    }

    /// Mark an entry as resolved. Returns `Ok(false)` when no entry has
    /// `entry_id`. On error the entry stays unresolved.
    pub fn resolve(&mut self, entry_id: u64) -> crate::Result<bool> {
        let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) else {
            return Ok(false);
        };
        mark_resolved(&self.store, entry)?;
        Ok(true)
    }

    /// Take the action of an unresolved entry so it can be run again, and
    /// mark the entry resolved.
    ///
    /// Resolving as part of taking is deliberate: the action is now the
    /// Event Plane's, and leaving the entry unresolved would invite a second
    /// requeue of work already back in flight.
    pub fn take_for_requeue(&mut self, entry_id: u64) -> Result<FailedAction, RequeueTakeError> {
        let entry = self
            .entries
            .iter_mut()
            .find(|e| e.entry_id == entry_id)
            .ok_or(RequeueTakeError::NotFound { entry_id })?;
        if entry.resolved {
            return Err(RequeueTakeError::AlreadyResolved { entry_id });
        }
        mark_resolved(&self.store, entry).map_err(|e| RequeueTakeError::Persist {
            entry_id,
            source: Box::new(e),
        })?;
        Ok(entry.action.clone())
    }

    /// Every entry, newest last, for operator introspection.
    pub fn list(&self) -> impl Iterator<Item = &TriggerDlqEntry> {
        self.entries.iter()
    }

    /// Total entries (resolved + unresolved).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Persist `entry` as resolved, then set the flag in memory.
fn mark_resolved(
    store: &RedbStore<TriggerDlqEntry>,
    entry: &mut TriggerDlqEntry,
) -> crate::Result<()> {
    let mut updated = entry.clone();
    updated.resolved = true;
    store.put(updated.entry_id, &updated)?;
    entry.resolved = true;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::action::{ActionContext, ActionId, ActionKey, ActionPayload};

    /// One dead-lettered ROW trigger.
    fn failed(collection: &str, row_id: &str, trigger: &str, lsn: u64) -> FailedAction {
        FailedAction {
            key: ActionKey {
                source_lsn: lsn,
                source_sequence: 1,
                source_vshard: 0,
                action: ActionId::TriggerRow {
                    trigger_name: trigger.to_owned(),
                },
            },
            payload: ActionPayload::TriggerRow {
                operation: "INSERT".into(),
                new_fields: None,
                old_fields: None,
            },
            context: ActionContext {
                database_id: nodedb_types::DatabaseId::DEFAULT,
                tenant_id: 1,
                collection: collection.to_owned(),
                row_id: row_id.to_owned(),
                cascade_depth: 0,
            },
            attempts: 5,
            last_error: "timeout".into(),
        }
    }

    #[test]
    fn dlq_enqueue_and_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();

        let id = dlq
            .enqueue(failed("orders", "order-1", "audit_trigger", 100))
            .unwrap();

        assert_eq!(dlq.len(), 1);
        let unresolved = dlq.list_unresolved();
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].entry_id, id);
        assert_eq!(unresolved[0].owner(), "audit_trigger");
        assert_eq!(unresolved[0].collection(), "orders");
        assert_eq!(unresolved[0].retry_count(), 5);
    }

    #[test]
    fn dlq_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();

        assert!(dlq.resolve(id).unwrap());
        assert!(dlq.list_unresolved().is_empty());
        assert!(!dlq.resolve(999).unwrap());
    }

    #[test]
    fn a_resolve_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            let id = dlq
                .enqueue(failed("orders", "order-1", "audit", 100))
                .unwrap();
            assert!(dlq.resolve(id).unwrap());
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(dlq.list_unresolved().is_empty());
    }

    #[test]
    fn eviction_is_persisted_with_the_new_entry() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            dlq.max_entries = 2;
            for i in 0u64..4 {
                dlq.enqueue(failed("c", &format!("r-{i}"), "t", i)).unwrap();
            }
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        let rows: Vec<&str> = dlq
            .list()
            .map(|e| e.action.context.row_id.as_str())
            .collect();
        assert_eq!(rows, vec!["r-2", "r-3"]);
        assert_eq!(dlq.next_entry_id, 5);
    }

    #[test]
    fn dlq_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            dlq.enqueue(failed("orders", "o-1", "t1", 100)).unwrap();
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq.list_unresolved()[0].owner(), "t1");
    }

    #[test]
    fn dlq_evicts_oldest_at_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        dlq.max_entries = 3;

        for i in 0u64..5 {
            dlq.enqueue(failed("c", &format!("r-{i}"), "t", i)).unwrap();
        }
        assert_eq!(dlq.len(), 3);
        assert_eq!(dlq.entries.front().unwrap().action.context.row_id, "r-2");
    }

    #[test]
    fn an_entry_can_be_taken_back_for_another_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();

        let action = dlq.take_for_requeue(id).expect("take");
        assert_eq!(action.owner(), "audit");
        assert!(
            dlq.list_unresolved().is_empty(),
            "taking resolves the entry so the same work is not requeued twice"
        );
    }

    #[test]
    fn an_entry_cannot_be_taken_twice() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        let id = dlq
            .enqueue(failed("orders", "order-1", "audit", 100))
            .unwrap();
        dlq.take_for_requeue(id).expect("first take");
        assert!(matches!(
            dlq.take_for_requeue(id).unwrap_err(),
            RequeueTakeError::AlreadyResolved { entry_id } if entry_id == id
        ));
    }

    #[test]
    fn an_unknown_entry_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(matches!(
            dlq.take_for_requeue(99).unwrap_err(),
            RequeueTakeError::NotFound { entry_id: 99 }
        ));
    }

    #[test]
    fn a_resolved_take_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut dlq = TriggerDlq::open(dir.path()).unwrap();
            let id = dlq
                .enqueue(failed("orders", "order-1", "audit", 100))
                .unwrap();
            dlq.take_for_requeue(id).expect("take");
        }
        let dlq = TriggerDlq::open(dir.path()).unwrap();
        assert!(
            dlq.list_unresolved().is_empty(),
            "a restart must not offer already-requeued work again"
        );
        assert_eq!(dlq.len(), 1, "the record itself is kept for history");
    }
}
