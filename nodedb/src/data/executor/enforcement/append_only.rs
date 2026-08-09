// SPDX-License-Identifier: BUSL-1.1

//! Append-only enforcement: reject UPDATE and DELETE on append-only collections.
//!
//! A hash-chained collection is refused on exactly the same terms, and with the
//! same error. `verify_chain` walks entries in insertion order and each link
//! covers its predecessor's hash, so removing or rewriting a link does not
//! report THAT row as broken — it reports its SUCCESSOR's, because the
//! successor's stored hash no longer matches what the surviving sequence
//! computes. Tamper-evidence would then blame an untampered row for its
//! predecessor's removal, which is worse than no evidence at all. `HASH_CHAIN`
//! already implies `APPEND_ONLY` at DDL time, so refusing here enforces an
//! invariant the collection declared rather than inventing a new one — and
//! reusing `AppendOnlyViolation` says exactly that to the client.

use crate::bridge::envelope::ErrorCode;
use nodedb_physical::physical_plan::EnforcementOptions;

/// Whether this collection's declared options forbid mutating an existing row.
fn mutation_forbidden(opts: &EnforcementOptions) -> bool {
    opts.append_only || opts.hash_chain
}

/// Check whether an UPDATE is allowed on this collection.
///
/// For append-only (and hash-chained) collections, UPDATEs are unconditionally
/// rejected. `old_value` being `Some` means the document already exists (UPDATE
/// case).
pub fn check_point_put(
    collection: &str,
    opts: &EnforcementOptions,
    old_value: &Option<Vec<u8>>,
) -> Result<(), ErrorCode> {
    if mutation_forbidden(opts) && old_value.is_some() {
        return Err(ErrorCode::AppendOnlyViolation {
            collection: collection.to_string(),
        });
    }
    Ok(())
}

/// Check whether an UPDATE of an existing row is allowed on this collection.
///
/// The read-modify-write UPDATE path never holds the prior row as owned bytes —
/// it borrows the image it just read — so it cannot call
/// [`check_point_put`] without cloning the row purely to satisfy a signature.
/// The row is known to exist on every path that reaches this check: an UPDATE
/// that matched nothing returns before any enforcement runs.
pub fn check_point_update(collection: &str, opts: &EnforcementOptions) -> Result<(), ErrorCode> {
    if mutation_forbidden(opts) {
        return Err(ErrorCode::AppendOnlyViolation {
            collection: collection.to_string(),
        });
    }
    Ok(())
}

/// Check whether a DELETE is allowed on this collection.
///
/// For append-only (and hash-chained) collections, DELETEs are unconditionally
/// rejected.
pub fn check_point_delete(collection: &str, opts: &EnforcementOptions) -> Result<(), ErrorCode> {
    if mutation_forbidden(opts) {
        return Err(ErrorCode::AppendOnlyViolation {
            collection: collection.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(append_only: bool) -> EnforcementOptions {
        EnforcementOptions {
            append_only,
            ..Default::default()
        }
    }

    /// A hash-chained collection as DDL builds one: `HASH_CHAIN` implies
    /// `APPEND_ONLY`.
    fn chained() -> EnforcementOptions {
        EnforcementOptions {
            append_only: true,
            hash_chain: true,
            ..Default::default()
        }
    }

    #[test]
    fn insert_allowed_on_hash_chain() {
        assert!(check_point_put("ledger", &chained(), &None).is_ok());
    }

    /// Rewriting a link makes `verify_chain` report the SUCCESSOR row as
    /// broken, so the update is refused rather than allowed to frame an
    /// untampered row.
    #[test]
    fn update_rejected_on_hash_chain() {
        let old = Some(vec![1, 2, 3]);
        assert!(matches!(
            check_point_put("ledger", &chained(), &old),
            Err(ErrorCode::AppendOnlyViolation { .. })
        ));
        assert!(matches!(
            check_point_update("ledger", &chained()),
            Err(ErrorCode::AppendOnlyViolation { .. })
        ));
    }

    /// Removing a link has the same effect as rewriting one: the next row's
    /// stored hash stops matching the sequence that survives.
    #[test]
    fn delete_rejected_on_hash_chain() {
        assert!(matches!(
            check_point_delete("ledger", &chained()),
            Err(ErrorCode::AppendOnlyViolation { .. })
        ));
    }

    #[test]
    fn update_allowed_when_neither_flag_is_set() {
        assert!(check_point_update("ledger", &opts(false)).is_ok());
    }

    #[test]
    fn update_rejected_on_append_only_without_the_prior_bytes() {
        assert!(check_point_update("ledger", &opts(true)).is_err());
    }

    #[test]
    fn insert_allowed_on_append_only() {
        assert!(check_point_put("ledger", &opts(true), &None).is_ok());
    }

    #[test]
    fn update_rejected_on_append_only() {
        let old = Some(vec![1, 2, 3]);
        assert!(check_point_put("ledger", &opts(true), &old).is_err());
    }

    #[test]
    fn update_allowed_when_not_append_only() {
        let old = Some(vec![1, 2, 3]);
        assert!(check_point_put("ledger", &opts(false), &old).is_ok());
    }

    #[test]
    fn delete_rejected_on_append_only() {
        assert!(check_point_delete("ledger", &opts(true)).is_err());
    }

    #[test]
    fn delete_allowed_when_not_append_only() {
        assert!(check_point_delete("ledger", &opts(false)).is_ok());
    }
}
