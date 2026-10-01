// SPDX-License-Identifier: BUSL-1.1

//! A claim on the durability of a group's staged writes.
//!
//! A caller takes a ticket under the `MultiRaft` lock, right after the Raft
//! call that staged the writes its reply depends on. It releases the lock,
//! awaits the ticket, and only then sends the reply or the vote request. No
//! other group, and no other caller of this group, waits on the disk
//! meanwhile.

use std::sync::Arc;

use super::writer::GroupDisk;

/// The durability of every write a group staged up to one sequence number.
#[derive(Clone)]
pub struct DurabilityTicket {
    disk: Arc<GroupDisk>,
    seq: u64,
}

impl std::fmt::Debug for DurabilityTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurabilityTicket")
            .field("group_id", &self.disk.group_id())
            .field("seq", &self.seq)
            .finish()
    }
}

impl DurabilityTicket {
    /// A ticket for every write `disk` staged so far, or `None` when all of
    /// them are durable.
    pub(super) fn for_staged(disk: &Arc<GroupDisk>) -> Option<Self> {
        let seq = disk.staged_through();
        (disk.progress().durable_seq < seq).then(|| Self {
            disk: Arc::clone(disk),
            seq,
        })
    }

    /// The group the ticket belongs to.
    pub fn group_id(&self) -> u64 {
        self.disk.group_id()
    }

    /// Wait until the writes are durable. Fails when the group's disk closed
    /// first: the group was unmounted, and the writes will never be durable.
    pub async fn durable(self) -> crate::Result<()> {
        let mut rx = self.disk.subscribe();
        loop {
            let progress = *rx.borrow_and_update();
            if progress.durable_seq >= self.seq {
                return Ok(());
            }
            if progress.closed || rx.changed().await.is_err() {
                return Err(crate::ClusterError::Storage {
                    detail: format!(
                        "raft group {}: the disk closed before write {} was durable",
                        self.disk.group_id(),
                        self.seq
                    ),
                });
            }
        }
    }

    /// Block until the writes are durable. Returns whether they are. For
    /// callers off the async threads.
    pub fn wait_blocking(&self) -> bool {
        self.disk.wait_blocking(self.seq)
    }
}
