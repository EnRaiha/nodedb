// SPDX-License-Identifier: BUSL-1.1

//! Columnar base insert handler.
//!
//! Writes rows to `nodedb-columnar`'s `MutationEngine`. Accepts msgpack payload
//! (array of objects). Creates the engine on first insert with schema inferred
//! from the first row.

pub mod insert;
pub mod read_prior;
pub mod schema;
pub mod spatial;

pub(in crate::data::executor) use insert::ColumnarInsertParams;
pub(in crate::data::executor) use schema::ndb_field_to_value;
// `ensure_columnar_engine_schema` is an inherent `CoreLoop` method (defined
// in `schema.rs`), called via `self.` — no re-export needed.
