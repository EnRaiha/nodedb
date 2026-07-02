// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral NodeDB DSL extensions — custom SQL-like commands beyond
//! standard SQL.
//!
//! - SEARCH <collection> USING FUSION(vector=..., graph=..., top_k=...)
//!   (`SEARCH <collection> USING VECTOR(...)` is preprocessor-rewritten to
//!   the canonical `SELECT ... ORDER BY vector_distance(...) LIMIT k` form
//!   in `nodedb-sql/src/parser/preprocess/search_vector.rs`.)
//! - CREATE VECTOR INDEX <name> ON <collection> [METRIC ...] [M ...] [EF_CONSTRUCTION ...] [DIM ...]
//!   [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M ...] [IVF_CELLS ...] [IVF_NPROBE ...]
//! - CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
//! - CREATE SEARCH INDEX ON <collection> FIELDS ...
//! - CREATE SPARSE INDEX [name] ON <collection> (<field>)
//! - CRDT MERGE INTO <collection> FROM <source_id> TO <target_id>
//!
//! Handlers build [`DdlResult`](super::super::result::DdlResult) directly and
//! carry no pgwire types.

mod crdt_merge;
mod fulltext_index;
mod helpers;
mod search_fusion;
mod search_index;
mod sparse_index;
mod support;
mod vector_index;

pub use crdt_merge::crdt_merge;
pub use fulltext_index::create_fulltext_index;
pub use search_fusion::search_fusion;
pub use search_index::create_search_index;
pub use sparse_index::create_sparse_index;
pub use vector_index::create_vector_index;
