// SPDX-License-Identifier: Apache-2.0

//! Read-only lookup abstraction used by the constraint validator.
//!
//! Provides three probes against committed CRDT state: row existence (FK
//! checks), field-value presence across all rows (UNIQUE checks), and the
//! live-only variant of that check for bitemporal collections.
//! `CrdtState` implements this trait; callers may supply any conforming view.

/// Read-only row and field lookup used by the constraint validator.
///
/// Implement this trait to supply the validator with a view of committed state.
/// All methods are read-only; the trait carries no mutation capability.
pub trait RowLookup {
    /// Return `true` if a row with the given `row_id` exists in `collection`.
    ///
    /// For FK and BiTemporalFK constraints the validator calls this to confirm
    /// the referenced row is present in the target collection.
    fn row_exists(&self, collection: &str, row_id: &str) -> bool;

    /// Return `true` if any row in `collection` has `field` equal to `value`.
    ///
    /// When `exclude_row_id` is `Some`, the row with that id is skipped, so a
    /// row does not collide with its own already-committed version. Pass `None`
    /// to consider every row.
    ///
    /// Used for UNIQUE constraint checking on non-bitemporal collections.
    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &loro::LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool;

    /// Return `true` if any *live* row in `collection` has `field` equal to
    /// `value`.
    ///
    /// A row is live when its `_ts_valid_until` is absent or `i64::MAX`.
    /// When `exclude_row_id` is `Some`, the row with that id is skipped so a
    /// row does not collide with its own already-committed version; pass `None`
    /// to consider every live row.
    /// Used for UNIQUE constraint checking on bitemporal collections so that
    /// superseded versions of the same logical row do not cause spurious
    /// collisions.
    fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &loro::LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool;
}
