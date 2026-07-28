// SPDX-License-Identifier: BUSL-1.1

//! HNSW vector-index side-effects for `apply_point_put`: index declared
//! strict-schema `Vector(dim)` columns and schemaless `vector_params`
//! fields, and soft-delete a document's prior vector nodes.

mod fields;
mod put;
mod remove;
#[cfg(test)]
mod tests;
mod types;

pub(in crate::data::executor) use types::{VectorIndexDelta, VectorIndexPutParams};
