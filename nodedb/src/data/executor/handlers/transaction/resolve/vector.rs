// SPDX-License-Identifier: BUSL-1.1

//! Vector serializer for transaction resolve.
//!
//! Unlike the KV / document / graph serializers, the vector serializer is
//! **plan-driven**, not overlay-driven: vector writes are never staged into a
//! transaction overlay (there is no `stage_vector`). A vector post-image is
//! also inexpressible — the HNSW graph mutation has no compact absolute form —
//! so the redo record logs the INSERT itself and replay rebuilds the index
//! (`replay_vector_wal`, dispatched from the redo reconstitute path). This
//! module therefore reads the [`VectorOp`] plan node directly and emits the
//! SAME engine-native WAL sub-record shape the autocommit vector path produces,
//! reusing its encoders (`control::server::wal_dispatch::vector`) so producer
//! and replay never drift:
//!
//! * `Insert` → `RecordType::VectorPut`, the 7-element
//!   `(collection, vector, dim, field_name, doc_id_compat, surrogate, provenance)`
//!   shape carrying the row's cross-engine surrogate identity.
//! * `BatchInsert` → `RecordType::VectorPut`, the 3-element
//!   `(collection, vectors, dim)` headless-batch shape.
//! * `Delete` → `RecordType::VectorDelete`, `(collection, vector_id, None)`.
//! * `DeleteBySurrogate` → `RecordType::VectorDelete`,
//!   `(collection, surrogate, field_name, provenance)`.
//! * `DirectUpsert` → `RecordType::VectorDirectUpsert`, the 8-element
//!   vector-primary post-image (`replay_direct_upsert`).
//! * `MultiVectorInsert` → `RecordType::MultiVectorPut`, the 6-element
//!   flattened multi-vector shape (`replay_multi_vector_put`).
//! * `MultiVectorDelete` → `RecordType::MultiVectorDelete`,
//!   `(collection, field_name, document_surrogate)` (`replay_multi_vector_delete`).
//! * `SparseInsert` → `RecordType::SparseVectorPut`,
//!   `(collection, field_name, doc_id, entries)` (`replay_sparse_put`).
//! * `SparseDelete` → `RecordType::SparseVectorDelete`,
//!   `(collection, field_name, doc_id)` (`replay_sparse_delete`).
//!
//! These five share the autocommit WAL shapes emitted by `wal_append_vector_op`
//! and decoded by `replay_vector_extended_wal`, which the redo replay path
//! invokes after `replay_vector_wal`, so producer and replay never drift.
//!
//! ## Ops that raise a typed error
//!
//! `SetParams` (vector-index DDL) raises a typed error, matching how the KV /
//! document serializers reject index / DDL ops: a `CREATE VECTOR INDEX` rides
//! its own autocommit `VectorParams` record, not a transaction redo.
//!
//! ## Ops that emit nothing
//!
//! Read and index-maintenance ops carry no persisted logical post-image: the
//! logical vectors survive via their `VectorPut` records and the index is
//! rebuilt from them on replay, so `Seal` / `CompactIndex` / `Rebuild` are
//! naturally reconstructed and need no redo sub-record.
//!
//! ## Determinism
//!
//! Emission is in plan order, which is already deterministic (the plan set is a
//! fixed `&[PhysicalPlan]`). A `VectorParams` record would have to precede its
//! puts on replay, but `SetParams` is rejected here, so ordering reduces to the
//! given plan order.

use nodedb_physical::physical_plan::VectorOp;
use nodedb_wal::record::RecordType;

use crate::control::server::wal_dispatch::{
    VectorDirectUpsertPayload, encode_multi_vector_delete_payload, encode_multi_vector_put_payload,
    encode_sparse_vector_delete_payload, encode_sparse_vector_put_payload,
    encode_vector_batch_put_payload, encode_vector_delete_by_surrogate_payload,
    encode_vector_delete_payload, encode_vector_direct_upsert_payload, encode_vector_put_payload,
};
use crate::wal::RedoSubRecord;

/// Append the redo sub-record(s) for a single vector plan op to `ops`.
///
/// Writes serialize to their engine-native record shape (`VectorPut` /
/// `VectorDelete` / `VectorDirectUpsert` / `MultiVectorPut` /
/// `MultiVectorDelete` / `SparseVectorPut` / `SparseVectorDelete`); read and
/// index-maintenance ops emit nothing; vector-index DDL (`SetParams`) raises a
/// typed error (see module docs).
pub(super) fn serialize_vector_op(
    op: &VectorOp,
    ops: &mut Vec<RedoSubRecord>,
) -> crate::Result<()> {
    match op {
        VectorOp::Insert {
            collection,
            vector,
            dim,
            field_name,
            surrogate,
            pk_bytes: _,
            provenance,
        } => {
            let payload = encode_vector_put_payload(
                collection,
                vector,
                *dim,
                field_name,
                *surrogate,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::BatchInsert {
            collection,
            vectors,
            dim,
            surrogates: _,
        } => {
            let payload = encode_vector_batch_put_payload(collection, vectors, *dim)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::Delete {
            collection,
            vector_id,
        } => {
            let payload = encode_vector_delete_payload(collection, *vector_id)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }
        VectorOp::DeleteBySurrogate {
            collection,
            surrogate,
            field_name,
            provenance,
        } => {
            let payload = encode_vector_delete_by_surrogate_payload(
                collection,
                *surrogate,
                field_name,
                provenance.as_ref(),
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDelete as u32,
                payload,
            });
            Ok(())
        }

        // Read families: no persisted post-image.
        VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::QueryStats { .. } => Ok(()),

        // Index maintenance: the logical vectors survive via their `VectorPut`
        // records and the index is rebuilt from them on replay, so seal /
        // compact / rebuild are reconstructed without a redo sub-record.
        VectorOp::Seal { .. } | VectorOp::CompactIndex { .. } | VectorOp::Rebuild { .. } => Ok(()),

        // Vector-index configuration DDL: rejected like the KV / document
        // index-DDL ops. No row-level post-image; a CREATE VECTOR INDEX rides
        // its own autocommit `VectorParams` record, not a transaction redo.
        VectorOp::SetParams { .. } => Err(crate::Error::PlanError {
            detail: "vector SetParams (index DDL) is not supported in transaction resolve"
                .to_string(),
        }),

        // Same contract on the teardown side: a DROP INDEX rides its own
        // autocommit `VectorIndexDrop` record, never a transaction redo.
        VectorOp::DropIndex { .. } => Err(crate::Error::PlanError {
            detail: "vector DropIndex (index DDL) is not supported in transaction resolve"
                .to_string(),
        }),

        // Vector-primary direct upsert: full post-image, replayed via
        // `replay_direct_upsert`.
        VectorOp::DirectUpsert {
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            // A redo record replays a write, and a replayed write answers
            // nobody — no client session is behind it to receive rows, so the
            // projection and its read gate are deliberately not carried.
            returning: _,
            rls_filters: _,
        } => {
            let payload = encode_vector_direct_upsert_payload(VectorDirectUpsertPayload {
                collection,
                field,
                surrogate: *surrogate,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
            })?;
            ops.push(RedoSubRecord {
                record_type: RecordType::VectorDirectUpsert as u32,
                payload,
            });
            Ok(())
        }
        // Multi-vector (ColBERT-style) insert, replayed via
        // `replay_multi_vector_put`.
        VectorOp::MultiVectorInsert {
            collection,
            field_name,
            document_surrogate,
            vectors,
            count,
            dim,
        } => {
            let payload = encode_multi_vector_put_payload(
                collection,
                field_name,
                *document_surrogate,
                vectors,
                *count,
                *dim,
            )?;
            ops.push(RedoSubRecord {
                record_type: RecordType::MultiVectorPut as u32,
                payload,
            });
            Ok(())
        }
        // Multi-vector delete, replayed via `replay_multi_vector_delete`.
        VectorOp::MultiVectorDelete {
            collection,
            field_name,
            document_surrogate,
        } => {
            let payload =
                encode_multi_vector_delete_payload(collection, field_name, *document_surrogate)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::MultiVectorDelete as u32,
                payload,
            });
            Ok(())
        }
        // Sparse-vector insert, replayed via `replay_sparse_put`.
        VectorOp::SparseInsert {
            collection,
            field_name,
            doc_id,
            entries,
        } => {
            let payload =
                encode_sparse_vector_put_payload(collection, field_name, doc_id, entries)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SparseVectorPut as u32,
                payload,
            });
            Ok(())
        }
        // Sparse-vector delete, replayed via `replay_sparse_delete`.
        VectorOp::SparseDelete {
            collection,
            field_name,
            doc_id,
        } => {
            let payload = encode_sparse_vector_delete_payload(collection, field_name, doc_id)?;
            ops.push(RedoSubRecord {
                record_type: RecordType::SparseVectorDelete as u32,
                payload,
            });
            Ok(())
        }
    }
}
