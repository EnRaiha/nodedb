// SPDX-License-Identifier: BUSL-1.1

//! The net change a committed transaction makes to each document and KV row.
//!
//! A redo record holds each row's final image, which cannot say whether the
//! row existed before the commit. The resolve that builds the record compares
//! the staged final state with the row's committed state and records the net
//! kind here. Several writes to one row collapse into one change:
//! - insert, then update: [`RedoRowKind::Insert`];
//! - update, then delete: [`RedoRowKind::Delete`];
//! - insert, then delete: [`RedoRowKind::NoChange`], which publishes nothing.
//!
//! The Control-Plane change stream publishes a committed transaction's
//! document and KV events from these entries. Its events carry no row image,
//! so an entry carries none either. Event-Plane events take their images from
//! the install, which reads the committed row as it replaces it.

use serde::{Deserialize, Serialize};

/// The row name an entry uses for every row of its collection: a staged
/// TRUNCATE.
pub const EVERY_ROW: &str = "*";

/// The net change of one row.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum RedoRowKind {
    /// The row did not exist before the commit and exists after it.
    Insert = 0,
    /// The row existed before the commit and exists after it.
    Update = 1,
    /// The row existed before the commit and does not exist after it.
    Delete = 2,
    /// The row exists neither before nor after the commit: the transaction
    /// inserted it and deleted it again.
    NoChange = 3,
}

impl RedoRowKind {
    /// The net kind of a row that `existed` before the commit and that the
    /// transaction left holding a value (`holds_value`) or deleted.
    pub fn net(existed: bool, holds_value: bool) -> Self {
        match (existed, holds_value) {
            (false, true) => Self::Insert,
            (true, true) => Self::Update,
            (true, false) => Self::Delete,
            (false, false) => Self::NoChange,
        }
    }
}

/// The net change of one document or KV row, or of a whole collection.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RedoRowChange {
    /// The collection, as the transaction's plans name it.
    pub collection: String,
    /// The row's identity as its change event names it: a document's
    /// identity, or a KV key as text. [`EVERY_ROW`] names every row.
    pub row: String,
    pub kind: RedoRowKind,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn several_writes_to_one_row_collapse_to_their_net_kind() {
        // Insert then update: absent before, holds a value after.
        assert_eq!(RedoRowKind::net(false, true), RedoRowKind::Insert);
        // Update of a committed row.
        assert_eq!(RedoRowKind::net(true, true), RedoRowKind::Update);
        // Update then delete: present before, deleted after.
        assert_eq!(RedoRowKind::net(true, false), RedoRowKind::Delete);
        // Insert then delete: absent before and after.
        assert_eq!(RedoRowKind::net(false, false), RedoRowKind::NoChange);
    }

    #[test]
    fn a_change_round_trips() {
        let change = RedoRowChange {
            collection: "orders".into(),
            row: "o1".into(),
            kind: RedoRowKind::Update,
        };
        let bytes = zerompk::to_msgpack_vec(&change).expect("encode");
        let decoded: RedoRowChange = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, change);
    }
}
