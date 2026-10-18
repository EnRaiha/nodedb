// SPDX-License-Identifier: Apache-2.0

//! Parameters and answer of [`EngineRules::plan_bulk_load`](super::EngineRules::plan_bulk_load).

use crate::types::SqlPlan;
use crate::types_array::ArrayInsertRow;

/// Parameters for planning one chunk of a bulk load (`COPY ... FROM`).
pub struct BulkLoadParams {
    /// The target collection or array.
    pub collection: String,
    /// The chunk's cells, typed against the array's schema. Empty when the
    /// caller asks only how the engine loads, before it decodes any row.
    pub cells: Vec<ArrayInsertRow>,
}

/// How an engine loads the rows of a bulk load.
#[derive(Debug)]
pub enum BulkLoadPlan {
    /// Each row loads as one `INSERT` statement through the engine's insert
    /// path, so the engine's triggers, defaults and checks apply per row.
    RowInserts,
    /// The rows decode against the array's schema and load in chunks. The
    /// plans load the chunk the parameters carry, in order. They are empty
    /// when the parameters carry no cells.
    CellChunks(Vec<SqlPlan>),
}
