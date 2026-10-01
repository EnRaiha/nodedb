// SPDX-License-Identifier: BUSL-1.1

//! What a committed entry produced, as its propose waiter receives it.

use crate::bridge::envelope::Response;
use crate::types::Lsn;

/// What a committed entry produced on the replica that applied it.
///
/// Carries the write's per-collection version alongside the payload because the
/// proposer has no other way to learn it: the version is minted inside the apply
/// path (the write funnel's WAL append), never on the wire, and the propose
/// tracker resolves on the very node that applied locally — so the version this
/// carries is that node's own, which is exactly what shard-local OCC validates
/// against.
#[derive(Debug, Clone)]
pub struct AppliedWrite {
    /// The Data Plane's response payload, verbatim.
    pub payload: Vec<u8>,
    /// The written collection's `coll_write_lsn` AFTER this write, in the local
    /// WAL-LSN domain — the one domain OCC's read validator compares in. It is
    /// NOT the Raft log index: the log index is a per-group counter that shares
    /// no scale with the WAL LSNs every other feed of that map records, and
    /// mixing the two silently breaks both directions of the comparison.
    pub write_version: Lsn,
}

impl AppliedWrite {
    /// Take both fields off the Data Plane's response to a committed write.
    ///
    /// `Response::read_version_lsn` is stamped by the core loop from the written
    /// collection's `coll_write_lsn`, read AFTER the handler recorded this
    /// write's LSN into the version index — so on a write response it is the
    /// post-write version. It is `Lsn::ZERO` for a plan that maps to no single
    /// user collection (see [`AppliedWrite::write_version`]).
    pub fn from_response(response: &Response) -> Self {
        Self {
            payload: response.payload.to_vec(),
            write_version: response.read_version_lsn,
        }
    }

    /// An applied entry that publishes no per-collection write-version: it wrote
    /// no Data-Plane collection state (a decode skip, a forwarded read result, a
    /// schema snapshot), or it was deduplicated before reaching the funnel. There
    /// is no version to floor a later read at, and `Lsn::ZERO` is the read-set
    /// capture's "no own-write floor" value — never a fabricated stand-in for a
    /// version that exists but was not read back.
    pub fn unversioned(payload: Vec<u8>) -> Self {
        Self {
            payload,
            write_version: Lsn::ZERO,
        }
    }
}

/// Result sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<AppliedWrite, crate::Error>;
