// SPDX-License-Identifier: BUSL-1.1

//! Durable write sets of journalled writes.
//!
//! The Control Plane journals a grouped write's write set after apply, as
//! the parts of the write's record group. A crash between the apply and that
//! append must leave restart state, point-in-time restore and the rebuilt
//! event stream in agreement. The core therefore runs a journalled task with
//! the durability of the sparse and edge stores deferred (see
//! `crate::engine::durability_gate`), and holds the task's events. Once the
//! task ran, one durable sparse commit stores the write set and persists
//! every effect before it. The edge store persists next, and the events go
//! out last.
//!
//! - A crash before that commit rolls the effects back, and no event left the
//!   core. Boot cancels the write's group.
//! - A crash after it leaves the effects and the stored write set. Boot
//!   journals the parts the WAL lacks from it, and replay rebuilds any edge
//!   the crash rolled back.
//!
//! Boot does both in `crate::bootstrap::write_group_settle`.

use std::collections::HashMap;

use crate::bridge::dispatch::{JournalGroup, WriteSetJournal};
use crate::bridge::envelope::Response;
use crate::event::WriteEvent;
use crate::wal::{CapturedEntry, OriginAppend, WriteSetCapture};

use super::CoreLoop;
use super::fail_stop::FailStopCause;
use crate::data::executor::task::ExecutionTask;

/// The journal side of a core.
#[derive(Default)]
pub(in crate::data::executor) struct WriteSetJournalState {
    /// The record group of each queued request that journals one.
    groups: HashMap<u64, JournalGroup>,
    /// Origins whose parts are durable. The next store drops their write
    /// sets.
    settled: Vec<u64>,
    /// The events of the running journalled tasks, sent once their write
    /// sets are stored. `None` outside a journalled task.
    pub(in crate::data::executor) held_events: Option<Vec<WriteEvent>>,
}

impl CoreLoop {
    /// Keep the journal a ring push handed over with request `request_id`.
    pub(in crate::data::executor) fn keep_write_set_journal(
        &mut self,
        request_id: u64,
        journal: WriteSetJournal,
    ) {
        if let Some(group) = journal.group {
            self.write_set_journal.groups.insert(request_id, group);
        }
        self.write_set_journal
            .settled
            .extend(journal.settled.iter().map(|lsn| lsn.as_u64()));
    }

    /// Whether `task` journals its write set.
    pub(in crate::data::executor) fn journals_write_set(&self, task: &ExecutionTask) -> bool {
        self.write_set_journal
            .groups
            .contains_key(&task.request_id().as_u64())
    }

    /// Take the record group `task` journals into, if any.
    pub(in crate::data::executor) fn take_journal_group(
        &mut self,
        task: &ExecutionTask,
    ) -> Option<JournalGroup> {
        self.write_set_journal
            .groups
            .remove(&task.request_id().as_u64())
    }

    /// Forget the group of request `request_id`, which never runs.
    pub(in crate::data::executor) fn drop_journal_group(&mut self, request_id: u64) {
        self.write_set_journal.groups.remove(&request_id);
    }

    /// Defer the durability of every commit on the sparse and edge stores,
    /// and hold every event, until [`Self::finish_journalled`].
    pub(in crate::data::executor) fn begin_journalled(&mut self) {
        self.sparse.db().defer();
        self.edge_store.db().defer();
        self.write_set_journal
            .held_events
            .get_or_insert_with(Vec::new);
    }

    /// Store `captures` durably with every effect before them, persist the
    /// edge store, stop deferring, and send the held events.
    ///
    /// A failed store fail-stops the core. The responses still go out, so the
    /// Control Plane journals their write sets, and restart replay rebuilds
    /// the effects from those records.
    pub(in crate::data::executor) fn finish_journalled(&mut self, captures: &[WriteSetCapture]) {
        let stored = self.store_write_sets(captures);
        self.sparse.db().resume();
        self.edge_store.db().resume();
        if let Err(error) = stored {
            self.fail_stop_core(FailStopCause::WriteSetUnpersisted, &error.to_string());
        }
        if let Some(events) = self.write_set_journal.held_events.take() {
            for event in events {
                self.send_write_event(event);
            }
        }
    }

    /// Store the write set `response` reports for `task`, then finish the
    /// journalled run (see [`Self::finish_journalled`]).
    pub(in crate::data::executor) fn finish_journalled_task(
        &mut self,
        task: &ExecutionTask,
        group: JournalGroup,
        response: &Response,
    ) {
        match self.write_set_capture(task, group, response) {
            Ok(capture) => self.finish_journalled(std::slice::from_ref(&capture)),
            Err(error) => {
                self.finish_journalled(&[]);
                self.fail_stop_core(FailStopCause::WriteSetUnpersisted, &error.to_string());
            }
        }
    }

    /// The write set `response` reports for `task`, with what boot needs to
    /// journal the task's origin again.
    pub(in crate::data::executor) fn write_set_capture(
        &self,
        task: &ExecutionTask,
        group: JournalGroup,
        response: &Response,
    ) -> crate::Result<WriteSetCapture> {
        let plan =
            zerompk::to_msgpack_vec(task.plan()).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("write set capture plan encode: {e}"),
            })?;
        Ok(WriteSetCapture {
            tenant_id: task.request.tenant_id.as_u64(),
            vshard_id: task.request.vshard_id.as_u32(),
            database_id: task.request.database_id.as_u64(),
            collection: group.collection,
            origin: group.origin.as_u64(),
            entries: response
                .write_set
                .iter()
                .map(CapturedEntry::from_entry)
                .collect(),
            origin_append: OriginAppend {
                plan,
                apply_key: group.apply_key,
                event_source: task.request.event_source.wal_code(),
                commit_hlc: group.commit_hlc,
                resolved_now_ms: task.request.resolved_now_ms,
                change_position: group
                    .change_position
                    .map(|position| (position.epoch, position.group_id, position.log_index)),
            },
        })
    }

    fn store_write_sets(&mut self, captures: &[WriteSetCapture]) -> crate::Result<()> {
        let settled = std::mem::take(&mut self.write_set_journal.settled);
        let txn = self.sparse.begin_durable_write()?;
        self.sparse
            .remove_write_set_captures_in_txn(&txn, &settled)?;
        for capture in captures {
            self.sparse
                .put_write_set_capture_in_txn(&txn, capture.origin, &capture.to_bytes()?)?;
        }
        txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse".into(),
            detail: format!("commit write set captures: {e}"),
        })?;
        self.edge_store.persist()
    }
}
