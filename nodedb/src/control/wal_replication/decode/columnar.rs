// SPDX-License-Identifier: BUSL-1.1

//! Decode `ReplicatedWrite` variants that produce `PhysicalPlan::Columnar`.

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::ColumnarOp;
use nodedb_types::RlsWriteCheck;

/// Reconstruct the columnar predicate-DML plan. The apply re-scans local
/// columnar state at this committed log position and mutates the predicate
/// matches — deterministic across replicas by Raft log order (identical
/// prior state ⇒ identical matching set).
///
/// No RLS predicate travels here: this shape only ever carries a collection
/// with NO write policy attached — `entry_columnar_family::columnar_write`
/// refuses to encode `ColumnarBulkDml` for a governed collection, so a
/// governed predicate DML never reaches this decoder. A collection that DOES
/// carry a write policy goes through [`bulk_dml_resolved`] instead, which
/// carries the already-decided rows rather than a predicate.
pub(super) fn bulk_dml(
    collection: &str,
    filters: &[u8],
    is_update: bool,
    updates: &[(String, Vec<u8>)],
) -> PhysicalPlan {
    if is_update {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            updates: updates.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    } else {
        PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            filters: filters.to_vec(),
            rls_write_check: RlsWriteCheck::already_decided_elsewhere(),
        })
    }
}

/// Reconstruct the columnar resolved-row-set DML plan
/// (`ColumnarOp::ResolvedUpdate` / `ColumnarOp::ResolvedDelete`).
///
/// Stamps `RlsWriteCheck::decided_earlier_in_request()`, not
/// `already_decided_elsewhere()`: the identity that authored this write
/// decided these exact rows against the write policy upstream, in the
/// Control Plane, and shipped the verdict — it did not go missing the way a
/// follower's own writing identity does. `decided_earlier_in_request` is the
/// tag for "a live identity already decided this row image"; every replica
/// applying this entry, including the leader that proposed it, is in that
/// same position.
pub(super) fn bulk_dml_resolved(
    collection: &str,
    is_update: bool,
    rows: &[super::super::types::ColumnarResolvedRow],
) -> crate::Result<PhysicalPlan> {
    let decode_value = |bytes: &[u8]| -> crate::Result<nodedb_types::Value> {
        nodedb_types::value_from_msgpack(bytes).map_err(|e| crate::Error::Internal {
            detail: format!("columnar resolved dml row decode failed: {e}"),
        })
    };
    if is_update {
        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            let pk = decode_value(&row.pk_msgpack)?;
            let new_row = match decode_value(&row.new_row_msgpack)? {
                nodedb_types::Value::Array(values) => values,
                other => {
                    return Err(crate::Error::Internal {
                        detail: format!(
                            "columnar resolved dml row: expected an array post-image, got {other:?}"
                        ),
                    });
                }
            };
            decoded.push((pk, new_row));
        }
        Ok(PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            rows: decoded,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    } else {
        let pks = rows
            .iter()
            .map(|row| decode_value(&row.pk_msgpack))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
            collection: nodedb_types::QualifiedCollection::from_stored(collection.to_owned()),
            pks,
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }))
    }
}
