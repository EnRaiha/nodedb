// SPDX-License-Identifier: BUSL-1.1

//! redb persistence for the deferred-action retry queue.
//!
//! An in-memory queue loses every pending action when the node restarts, and
//! the Event Plane consumer watermark can already have advanced past the write
//! that produced them — nothing would re-deliver the cause. Each pending
//! action is therefore written here on enqueue and removed only once it has
//! run to completion, which makes a crash replay the action rather than drop
//! it.
//!
//! Replay is at-least-once by construction: a crash between an action
//! committing and its record being removed re-runs that action on restart.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::codec::{decode_action, encode_action, encode_key};
use super::record::FailedAction;

/// redb table: encoded [`super::record::ActionKey`] → encoded [`FailedAction`].
const PENDING_ACTIONS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pending_actions");

/// Durable set of actions awaiting retry.
pub struct ActionStore {
    db: Database,
}

impl ActionStore {
    /// The file one consumer core's store lives in.
    pub fn path_for(data_dir: &Path, core_id: usize) -> std::path::PathBuf {
        data_dir
            .join("event_plane")
            .join(format!("action_retry_core{core_id}.redb"))
    }

    /// Open or create the store for one consumer core at
    /// `{data_dir}/event_plane/action_retry_core{core_id}.redb`.
    ///
    /// One file per core, matching the per-core watermark: consumers own
    /// disjoint event streams and never read each other's pending actions, so
    /// a shared file would serialise unrelated writes behind one lock.
    pub fn open(data_dir: &Path, core_id: usize) -> crate::Result<Self> {
        let dir = data_dir.join("event_plane");
        std::fs::create_dir_all(&dir)
            .map_err(|e| storage_error(format!("create dir {}: {e}", dir.display())))?;
        let path = dir.join(format!("action_retry_core{core_id}.redb"));
        let db = Database::create(&path)
            .map_err(|e| storage_error(format!("open action retry db {}: {e}", path.display())))?;
        let store = Self { db };
        store.ensure_table()?;
        Ok(store)
    }

    /// Create the table so a first read on an empty database does not fail.
    fn ensure_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage_error(format!("begin write: {e}")))?;
        txn.open_table(PENDING_ACTIONS)
            .map_err(|e| storage_error(format!("open table: {e}")))?;
        txn.commit()
            .map_err(|e| storage_error(format!("commit: {e}")))
    }

    /// Record an action as pending, replacing any earlier record for the same
    /// key so a re-enqueue carries the current attempt count.
    pub fn put(&self, action: &FailedAction) -> crate::Result<()> {
        let key = encode_key(&action.key)?;
        let value = encode_action(action)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage_error(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(PENDING_ACTIONS)
                .map_err(|e| storage_error(format!("open table: {e}")))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|e| storage_error(format!("insert pending action: {e}")))?;
        }
        txn.commit()
            .map_err(|e| storage_error(format!("commit: {e}")))
    }

    /// Forget an action that has run to completion or reached the DLQ.
    pub fn remove(&self, key: &super::record::ActionKey) -> crate::Result<()> {
        let encoded = encode_key(key)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage_error(format!("begin write: {e}")))?;
        {
            let mut table = txn
                .open_table(PENDING_ACTIONS)
                .map_err(|e| storage_error(format!("open table: {e}")))?;
            table
                .remove(encoded.as_slice())
                .map_err(|e| storage_error(format!("remove pending action: {e}")))?;
        }
        txn.commit()
            .map_err(|e| storage_error(format!("commit: {e}")))
    }

    /// Every action still pending, for replay at startup.
    pub fn load_all(&self) -> crate::Result<Vec<FailedAction>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| storage_error(format!("begin read: {e}")))?;
        let table = txn
            .open_table(PENDING_ACTIONS)
            .map_err(|e| storage_error(format!("open table: {e}")))?;
        let mut actions = Vec::new();
        let iter = table
            .iter()
            .map_err(|e| storage_error(format!("iterate pending actions: {e}")))?;
        for row in iter {
            let (_key, value) =
                row.map_err(|e| storage_error(format!("read pending action: {e}")))?;
            actions.push(decode_action(value.value())?);
        }
        Ok(actions)
    }
}

fn storage_error(detail: String) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail,
    }
}
