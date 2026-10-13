// SPDX-License-Identifier: BUSL-1.1

//! Which rows a KV secondary index holds.

/// Which rows a KV secondary index holds.
///
/// Only a `Complete` index answers an equality: its lookup is every
/// matching row, and an empty lookup means no row matches. A
/// `SinceRegistration` index lacks the rows written before it, so an
/// equality over it reads the whole collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCoverage {
    /// Every row: the index was backfilled, or it was registered while the
    /// collection held no row.
    Complete,
    /// Only rows written after the registration: it was registered with
    /// `backfill=false` over a collection that already held rows.
    SinceRegistration,
}

impl IndexCoverage {
    /// The coverage a registration gives: `Complete` when it backfills or
    /// the collection holds no row yet.
    pub fn of_registration(backfill: bool, collection_rows: usize) -> Self {
        if backfill || collection_rows == 0 {
            Self::Complete
        } else {
            Self::SinceRegistration
        }
    }

    /// Whether an equality lookup on the index is every matching row.
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_late_registration_without_backfill_is_partial() {
        assert_eq!(
            IndexCoverage::of_registration(true, 10),
            IndexCoverage::Complete
        );
        assert_eq!(
            IndexCoverage::of_registration(false, 0),
            IndexCoverage::Complete
        );
        assert_eq!(
            IndexCoverage::of_registration(false, 10),
            IndexCoverage::SinceRegistration
        );
    }
}
