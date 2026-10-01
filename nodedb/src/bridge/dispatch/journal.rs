// SPDX-License-Identifier: BUSL-1.1

//! What a core learns about the write-set journal of the requests it runs.
//!
//! A write that journals its write set after apply names its record group
//! here. The core then stores the write set beside the write's effects,
//! together with what boot needs to journal the write's origin again when a
//! crash cut it from the WAL (see `crate::bootstrap::write_group_settle`).
//! The Control Plane reports each group whose parts are durable, and the core
//! drops its stored write set.
//!
//! The Control Plane keeps the journal of a queued request beside the request
//! and hands both over in one ring push.

use crate::event::cdc::position::ReplicatedPosition;
use crate::types::Lsn;

/// The record group a dispatched write journals its write set into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalGroup {
    /// LSN of the group's origin record.
    pub origin: Lsn,
    /// The collection of every write-set entry without its own.
    pub collection: String,
    /// The idempotency key every record of the write carries.
    pub apply_key: u64,
    /// The commit instant every record of the write carries, when it was
    /// known before the append.
    pub commit_hlc: Option<u64>,
    /// The replicated position whose marker precedes the origin.
    pub change_position: Option<ReplicatedPosition>,
}

/// The journal part of one ring push.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSetJournal {
    /// The group of the pushed request, when it journals one.
    pub group: Option<JournalGroup>,
    /// Origins whose parts are durable since the last push to this core.
    pub settled: Vec<Lsn>,
}
