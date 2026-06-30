// SPDX-License-Identifier: BUSL-1.1

//! Self-describing WAL payload for CRDT delta records.
//!
//! Both the writer (`control/server/wal_dispatch/core.rs`) and the reader
//! (`data/executor/wal_replay.rs`) live in this crate and use this single
//! struct, encoded/decoded with `zerompk`, so there is exactly one
//! unambiguous decode path — no arity guessing.

/// WAL payload for a `RecordType::CrdtDelta` record.
///
/// `collection` distinguishes the two producers:
/// - `Some(_)` for a per-document delta apply (`CrdtOp::Apply`)
/// - `None` for a whole-tenant snapshot import (`CrdtOp::ImportSnapshot`)
#[derive(
    serde::Serialize, serde::Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub(crate) struct CrdtDeltaWalPayload {
    pub bytes: Vec<u8>,
    pub collection: Option<String>,
    pub provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
}
