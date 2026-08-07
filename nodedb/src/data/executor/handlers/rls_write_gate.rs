// SPDX-License-Identifier: BUSL-1.1

//! Data Plane enforcement of the row-level-security WRITE policy.
//!
//! The Control Plane compiles a collection's write policy into the plan's
//! `rls_write_check` slot. For a write whose row image is produced where it is
//! persisted — an update's post-image, a delete's pre-image, an upsert's merged
//! body — that image does not exist at plan time, so the predicate travels with
//! the plan and is decided here, against the bytes actually about to be written.
//!
//! Two rules this module exists to hold:
//!
//! - **A rejected row fails the statement.** Skipping it would report a write
//!   that never happened, and leave the remaining rows of a multi-row statement
//!   applied — a partial write the caller cannot see or undo.
//! - **An empty check admits everything.** Empty means "no write policy
//!   restricts this identity here" (or superuser), the same convention the read
//!   filters use, so an ungoverned collection pays nothing.
//!
//! Distinct from [`super::rls_eval`], which decides the READ policy: that one
//! bounds which rows a `RETURNING` clause may show, this one bounds which rows
//! may be written at all.

use nodedb_types::columnar::StrictSchema;

use super::returning_doc;
use super::rls_eval;

/// Decide one already-decoded row image against the compiled write policy.
///
/// Fails closed: an undecodable filter payload or an evaluation error denies,
/// so an adversarial predicate cannot be turned into an admitted write.
pub(in crate::data::executor) fn admit_row(
    rls_write_check: &[u8],
    image: &serde_json::Value,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if rls_write_check.is_empty() || rls_eval::rls_check_document(rls_write_check, image) {
        return Ok(());
    }
    Err(crate::Error::RejectedAuthz {
        tenant_id: crate::types::TenantId::new(tid),
        resource: format!("RLS write policy on '{collection}' rejected the row"),
    })
}

/// Decide one STORED row body — the bytes about to be written, or the bytes
/// about to be removed — against the compiled write policy.
///
/// `strict_schema` is `Some` exactly when the collection stores Binary Tuples.
/// The decode goes through [`returning_doc::from_stored`] for the reason that
/// module exists: the MessagePack decoder does not reject a Binary Tuple, it
/// succeeds and yields a document with every real column missing, which would
/// fail every predicate and reject writes the policy actually permits.
///
/// A body that does not decode at all is refused rather than written
/// unchecked — an image the policy could not be evaluated against is not an
/// image the policy admitted.
pub(in crate::data::executor) fn admit_stored_row(
    rls_write_check: &[u8],
    body: &[u8],
    doc_id: &str,
    strict_schema: Option<&StrictSchema>,
    tid: u64,
    collection: &str,
) -> crate::Result<()> {
    if rls_write_check.is_empty() {
        return Ok(());
    }
    match returning_doc::from_stored(body, doc_id, strict_schema) {
        Some(image) => admit_row(rls_write_check, &image, tid, collection),
        None => Err(crate::Error::RejectedAuthz {
            tenant_id: crate::types::TenantId::new(tid),
            resource: format!(
                "RLS write policy on '{collection}': row '{doc_id}' did not decode, so the policy \
                 could not be evaluated against it"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::scan_filter::ScanFilter;
    use serde_json::json;

    fn owner_policy(value: &str) -> Vec<u8> {
        let filter = ScanFilter {
            field: "owner".into(),
            op: "eq".into(),
            value: nodedb_types::Value::String(value.into()),
            clauses: Vec::new(),
            expr: None,
        };
        zerompk::to_msgpack_vec(&vec![filter]).expect("encode policy filter")
    }

    #[test]
    fn an_empty_check_admits_every_row() {
        assert!(admit_row(&[], &json!({"owner": "mallory"}), 1, "orders").is_ok());
    }

    #[test]
    fn a_conforming_row_is_admitted() {
        assert!(
            admit_row(
                &owner_policy("alice"),
                &json!({"owner": "alice"}),
                1,
                "orders"
            )
            .is_ok()
        );
    }

    #[test]
    fn a_violating_row_is_rejected() {
        assert!(matches!(
            admit_row(
                &owner_policy("alice"),
                &json!({"owner": "bob"}),
                1,
                "orders"
            ),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A row missing the governed column cannot satisfy the predicate, so it is
    /// rejected rather than admitted by omission.
    #[test]
    fn a_row_without_the_governed_column_is_rejected() {
        assert!(admit_row(&owner_policy("alice"), &json!({"note": "x"}), 1, "orders").is_err());
    }

    /// A filter payload that does not deserialize denies rather than passing
    /// the row through unchecked.
    #[test]
    fn a_corrupt_check_denies() {
        assert!(admit_row(&[0xFF, 0xFE], &json!({"owner": "alice"}), 1, "orders").is_err());
    }
}
