// SPDX-License-Identifier: Apache-2.0

//! Row-existence and row-targeting shapes of the vector-primary direct
//! writes.

use nodedb_types::Surrogate;

/// Row-existence semantics of a vector-primary direct write, as the three
/// insert-family ops carry them on the durable record.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorDirectWriteIntent {
    /// [`VectorOp::DirectInsert`](super::VectorOp::DirectInsert): an existing
    /// key is a `unique_violation`.
    Insert,
    /// [`VectorOp::DirectInsertIfAbsent`](super::VectorOp::DirectInsertIfAbsent):
    /// an existing key is left alone.
    InsertIfAbsent,
    /// [`VectorOp::DirectUpsert`](super::VectorOp::DirectUpsert): an existing
    /// key is replaced or patched.
    Upsert,
}

/// One row of a [`VectorOp::DirectBatchWrite`](super::VectorOp::DirectBatchWrite)
/// page. The fields mean what the same-named fields of
/// [`VectorOp::DirectUpsert`](super::VectorOp::DirectUpsert) mean.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct VectorDirectRow {
    /// Global surrogate the Control Plane bound to `pk_bytes`.
    pub surrogate: Surrogate,
    /// UTF-8 of the declared primary-key value.
    pub pk_bytes: Vec<u8>,
    /// FP32 vector values.
    pub vector: Vec<f32>,
    /// Pre-encoded MessagePack of every non-vector column the row supplied.
    pub payload: Vec<u8>,
}

/// The rows a vector-primary `DELETE` / `UPDATE` targets.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum VectorWriteTargets {
    /// Surrogates the Control Plane resolved from primary-key equalities. A
    /// key with no binding contributes no entry.
    Surrogates(Vec<Surrogate>),
    /// Serialized `Vec<ScanFilter>` the Data Plane evaluates against every
    /// payload sidecar row. Empty bytes match every row.
    Predicate(Vec<u8>),
}
