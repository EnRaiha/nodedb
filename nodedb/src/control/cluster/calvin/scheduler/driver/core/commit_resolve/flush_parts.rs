// SPDX-License-Identifier: BUSL-1.1

//! The stored write set of a committed Calvin flush.
//!
//! A committed Calvin record is whole at append: its flush reports no rows,
//! so no part follows it. The core still runs the flush journalled, which
//! stores the flush's append inputs beside its effects for boot. Once the
//! record is durable, the core drops what it stored.

use crate::control::cluster::calvin::scheduler::driver::core::scheduler::Scheduler;
use crate::types::{Lsn, VShardId};

impl Scheduler {
    /// Note that the record at `origin` is durable, so the core drops the
    /// write set it stored.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn note_flush_settled(
        &self,
        origin: Lsn,
    ) {
        let vshard_id = VShardId::new(self.vshard_id);
        match self.shared.dispatcher.lock() {
            Ok(mut d) => d.note_write_set_settled(vshard_id, origin),
            Err(poisoned) => poisoned
                .into_inner()
                .note_write_set_settled(vshard_id, origin),
        }
    }
}
