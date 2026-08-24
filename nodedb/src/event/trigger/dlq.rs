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

use redb::{Database, ReadableTable, TableDefinition};

use crate::event::action::FailedAction;
use tracing::{debug, warn};

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
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RequeueTakeError {
    #[error("no dead-letter entry {entry_id}")]
    NotFound { entry_id: u64 },

    #[error("dead-letter entry {entry_id} is already resolved")]
    AlreadyResolved { entry_id: u64 },
}

/// Trigger dead-letter queue.
pub struct TriggerDlq {
    db: Database,
    /// In-memory index for fast listing (mirrors redb for read performance).
    entries: VecDeque<TriggerDlqEntry>,
    next_entry_id: u64,
    max_entries: usize,
}

impl TriggerDlq {
    /// Open or create the trigger DLQ at `{data_dir}/event_plane/trigger_dlq.redb`.
    pub fn open(data_dir: &Path) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("create dir {}: {e}", dir.display()),
        })?;

        let path = dir.join("trigger_dlq.redb");
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("open trigger DLQ db {}: {e}", path.display()),
        })?;

        // Ensure table exists and load existing entries.
        let mut entries = VecDeque::new();
        let mut max_id = 0u64;
        {
            let txn = db.begin_write().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("begin_write: {e}"),
            })?;
            {
                let table = txn
                    .open_table(TRIGGER_DLQ)
                    .map_err(|e| crate::Error::Storage {
                        engine: "event_plane".into(),
                        detail: format!("open_table: {e}"),
                    })?;
                let mut range = table.range(0u64..).map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("range: {e}"),
                })?;
                while let Some(Ok((key_guard, value_guard))) = range.next() {
                    let id: u64 = key_guard.value();
                    if id > max_id {
                        max_id = id;
                    }
                    let bytes: &[u8] = value_guard.value();
                    if let Ok(entry) = zerompk::from_msgpack::<TriggerDlqEntry>(bytes) {
                        entries.push_back(entry);
                    }
                }
            }
            txn.commit().map_err(|e| crate::Error::Storage {
                engine: "event_plane".into(),
                detail: format!("commit: {e}"),
            })?;
        }

        debug!(
            entries = entries.len(),
            next_id = max_id + 1,
            "trigger DLQ loaded"
        );

        Ok(Self {
            db,
            entries,
            next_entry_id: max_id + 1,
            max_entries: DEFAULT_MAX_ENTRIES,
        })
    }

    /// Record an action that has exhausted its retries.
    pub fn enqueue(&mut self, action: FailedAction) -> crate::Result<u64> {
        let entry_id = self.next_entry_id;
        self.next_entry_id += 1;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = TriggerDlqEntry {
            entry_id,
            action,
            created_at: now,
            resolved: false,
        };

        // Evict oldest if at capacity.
        while self.entries.len() >= self.max_entries {
            if let Some(evicted) = self.entries.pop_front() {
                self.delete_from_redb(evicted.entry_id);
                warn!(
                    entry_id = evicted.entry_id,
                    owner = %evicted.owner(),
                    "trigger DLQ evicted oldest entry (at capacity)"
                );
            }
        }

        // Persist to redb.
        self.write_to_redb(&entry)?;

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

    /// Mark an entry as resolved.
    pub fn resolve(&mut self, entry_id: u64) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.entry_id == entry_id) {
            entry.resolved = true;
            true
        } else {
            return false;
        };
        // Persist the update to redb (entry cloned to avoid borrow conflict).
        if let Some(entry) = self.entries.iter().find(|e| e.entry_id == entry_id) {
            let _ = self.write_to_redb(entry);
        }
        true
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
        let action = entry.action.clone();
        entry.resolved = true;
        let updated = entry.clone();
        let _ = self.write_to_redb(&updated);
        Ok(action)
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

    fn write_to_redb(&self, entry: &TriggerDlqEntry) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("trigger DLQ entry: {e}"),
        })?;
        let txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("begin_write: {e}"),
        })?;
        {
            let mut table = txn
                .open_table(TRIGGER_DLQ)
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("open_table: {e}"),
                })?;
            table
                .insert(entry.entry_id, bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "event_plane".into(),
                    detail: format!("insert: {e}"),
                })?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "event_plane".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    fn delete_from_redb(&self, entry_id: u64) {
        if let Ok(txn) = self.db.begin_write() {
            if let Ok(mut table) = txn.open_table(TRIGGER_DLQ) {
                let _ = table.remove(entry_id);
            }
            let _ = txn.commit();
        }
    }
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

        assert!(dlq.resolve(id));
        assert!(dlq.list_unresolved().is_empty());
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
        assert_eq!(
            dlq.take_for_requeue(id).unwrap_err(),
            RequeueTakeError::AlreadyResolved { entry_id: id }
        );
    }

    #[test]
    fn an_unknown_entry_reports_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let mut dlq = TriggerDlq::open(dir.path()).unwrap();
        assert_eq!(
            dlq.take_for_requeue(99).unwrap_err(),
            RequeueTakeError::NotFound { entry_id: 99 }
        );
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
