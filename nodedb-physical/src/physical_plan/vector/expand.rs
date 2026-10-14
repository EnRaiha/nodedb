// SPDX-License-Identifier: Apache-2.0

//! The point writes a vector-primary page expands to.
//!
//! A transaction stages each row of a page on its own, so each row gets its
//! own overlay image, undo entry and redo entry. A key the page names twice
//! is in the overlay when its later row stages, so that row is refused,
//! skipped or merged as the page judges it.

use super::op::VectorOp;
use super::write::{VectorDirectRow, VectorDirectWriteIntent};

impl VectorOp {
    /// One `DirectInsert` / `DirectInsertIfAbsent` / `DirectUpsert` per row
    /// of a `DirectBatchWrite` page, in page order, by the page's intent.
    /// `None` for every other op.
    pub fn direct_batch_point_writes(&self) -> Option<Vec<VectorOp>> {
        let VectorOp::DirectBatchWrite {
            collection,
            field,
            intent,
            rows,
            quantization,
            storage_dtype,
            payload_indexes,
            returning,
            rls_filters,
            on_conflict_updates,
            rls_write_check,
        } = self
        else {
            return None;
        };
        Some(
            rows.iter()
                .map(|row| {
                    let VectorDirectRow {
                        surrogate,
                        pk_bytes,
                        vector,
                        payload,
                    } = row.clone();
                    match intent {
                        VectorDirectWriteIntent::Insert => VectorOp::DirectInsert {
                            collection: collection.clone(),
                            field: field.clone(),
                            surrogate,
                            pk_bytes,
                            vector,
                            payload,
                            quantization: *quantization,
                            storage_dtype: *storage_dtype,
                            payload_indexes: payload_indexes.clone(),
                            returning: returning.clone(),
                            rls_filters: rls_filters.clone(),
                        },
                        VectorDirectWriteIntent::InsertIfAbsent => VectorOp::DirectInsertIfAbsent {
                            collection: collection.clone(),
                            field: field.clone(),
                            surrogate,
                            pk_bytes,
                            vector,
                            payload,
                            quantization: *quantization,
                            storage_dtype: *storage_dtype,
                            payload_indexes: payload_indexes.clone(),
                            returning: returning.clone(),
                            rls_filters: rls_filters.clone(),
                        },
                        VectorDirectWriteIntent::Upsert => VectorOp::DirectUpsert {
                            collection: collection.clone(),
                            field: field.clone(),
                            surrogate,
                            pk_bytes,
                            vector,
                            payload,
                            quantization: *quantization,
                            storage_dtype: *storage_dtype,
                            payload_indexes: payload_indexes.clone(),
                            returning: returning.clone(),
                            rls_filters: rls_filters.clone(),
                            on_conflict_updates: on_conflict_updates.clone(),
                            rls_write_check: rls_write_check.clone(),
                        },
                    }
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, QualifiedCollection, RlsWriteCheck, Surrogate};

    use super::*;

    fn page(intent: VectorDirectWriteIntent) -> VectorOp {
        let row = |s: u32, pk: &str| VectorDirectRow {
            surrogate: Surrogate::new(s),
            pk_bytes: pk.as_bytes().to_vec(),
            vector: vec![0.5, 0.25],
            payload: Vec::new(),
        };
        VectorOp::DirectBatchWrite {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".into(),
            intent,
            rows: vec![row(1, "a"), row(2, "b")],
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
        }
    }

    #[test]
    fn a_page_expands_to_one_point_write_per_row_in_order() {
        let writes = page(VectorDirectWriteIntent::InsertIfAbsent)
            .direct_batch_point_writes()
            .expect("a page expands");
        let keys: Vec<Vec<u8>> = writes
            .iter()
            .map(|write| match write {
                VectorOp::DirectInsertIfAbsent { pk_bytes, .. } => pk_bytes.clone(),
                other => panic!("expected DirectInsertIfAbsent, got {other:?}"),
            })
            .collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert!(matches!(
            page(VectorDirectWriteIntent::Upsert)
                .direct_batch_point_writes()
                .as_deref(),
            Some([VectorOp::DirectUpsert { .. }, VectorOp::DirectUpsert { .. }])
        ));
    }

    #[test]
    fn a_point_write_does_not_expand() {
        let point = page(VectorDirectWriteIntent::Insert)
            .direct_batch_point_writes()
            .and_then(|writes| writes.into_iter().next())
            .expect("a row");
        assert!(point.direct_batch_point_writes().is_none());
    }
}
