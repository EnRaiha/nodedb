// SPDX-License-Identifier: BUSL-1.1

//! Compaction of this node's Calvin sequencer log.
//!
//! The sequencer state machine lives in memory and is rebuilt from the log.
//! A compaction discards entries, so the state they built must survive a
//! restart some other way. Once the log retained the group's compaction
//! threshold of entries past the last capture, this node:
//!
//! 1. captures the state machine at its applied index, on the Raft tick
//!    thread, right after the apply;
//! 2. writes the capture durably as the kept sequencer snapshot (see
//!    [`super::sequencer_snapshot`]), off the async threads;
//! 3. records the applied index as the group's durable applied floor, so a
//!    restart restores the state machine from the file and the log delivers
//!    only the entries after it;
//! 4. compacts the log under the compactor's floors: every index a Calvin
//!    scheduler here still replays stays (see the `raft_compactor` wiring).
//!
//! One run is in flight at a time. A run that fails leaves the log as it is;
//! a later apply starts the next one.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use tracing::warn;

use crate::control::cluster::sequencer_snapshot::SequencerSnapshotStore;
use crate::control::state::SharedState;

/// Captures, persists and compacts the sequencer log on this node.
pub struct SequencerCompaction {
    store: Arc<SequencerSnapshotStore>,
    shared: Weak<SharedState>,
    /// The group's compaction threshold: entries applied past the last
    /// capture before the next run.
    threshold: u64,
    /// The applied index of the last capture written durably.
    persisted: AtomicU64,
    /// Whether a run is in flight.
    running: AtomicBool,
}

impl SequencerCompaction {
    /// A compaction for the sequencer log of `shared`'s node, or `None` when
    /// the group has no compaction threshold: its log is never compacted.
    pub fn new(
        store: Arc<SequencerSnapshotStore>,
        shared: &Arc<SharedState>,
        threshold: Option<u64>,
    ) -> Option<Arc<Self>> {
        let threshold = threshold?;
        Some(Arc::new(Self {
            store,
            shared: Arc::downgrade(shared),
            threshold: threshold.max(1),
            persisted: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }))
    }

    /// Start a run when the state machine applied through `applied` and the
    /// threshold passed since the last capture. The caller released the
    /// state machine's lock. Never blocks: the capture is in memory and the
    /// disk work runs on a blocking thread.
    pub fn after_apply(self: &Arc<Self>, applied: u64) {
        let persisted = self.persisted.load(Ordering::Acquire);
        if applied < persisted.saturating_add(self.threshold) {
            return;
        }
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let bytes = match self.store.capture(applied) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(applied, %error, "sequencer compaction: the capture failed");
                self.running.store(false, Ordering::Release);
                return;
            }
        };
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let blocking = Arc::clone(&this);
            let run =
                tokio::task::spawn_blocking(move || blocking.persist_and_compact(applied, &bytes))
                    .await;
            match run {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(applied, %error, "sequencer compaction did not complete"),
                Err(error) => warn!(applied, %error, "sequencer compaction task failed"),
            }
            this.running.store(false, Ordering::Release);
        });
    }

    /// Steps 2 to 4 of the module docs. Blocks on disk.
    fn persist_and_compact(&self, applied: u64, bytes: &[u8]) -> crate::Result<()> {
        self.store.persist(bytes)?;
        let shared = self
            .shared
            .upgrade()
            .ok_or_else(|| crate::Error::Internal {
                detail: "sequencer compaction: the node shut down".into(),
            })?;
        if let Some(sink) = shared.raft_applied_index_sink.get() {
            sink(SEQUENCER_GROUP_ID, applied)?;
        }
        self.persisted.store(applied, Ordering::Release);
        if let Some(compactor) = shared.raft_compactor.get() {
            compactor(SEQUENCER_GROUP_ID, applied)?;
        }
        Ok(())
    }
}
