// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral NodeDB DSL extensions — custom SQL-like commands beyond
//! standard SQL.
//!
//! - SEARCH <collection> USING FUSION(vector=..., graph=..., top_k=...)
//!   (`SEARCH <collection> USING VECTOR(...)` is preprocessor-rewritten to
//!   the canonical `SELECT ... ORDER BY vector_distance(...) LIMIT k` form
//!   in `nodedb-sql/src/parser/preprocess/search_vector.rs`.)
//! - CREATE VECTOR INDEX <name> ON <collection> [(<column>)] DIM <n> [METRIC ...]
//!   [M ...] [EF_CONSTRUCTION ...] [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M ...]
//!   [IVF_CELLS ...] [IVF_NPROBE ...]
//! - CREATE {FULLTEXT,SEARCH} INDEX [<name>] ON <collection> (<field>, ...)
//!   [ANALYZER '<name>'] [FUZZY true|false]
//! - CREATE SPARSE INDEX [<name>] ON <collection> [(<field>)]
//! - CRDT MERGE INTO <collection> FROM <source_id> TO <target_id>
//!
//! Handlers build [`DdlResult`](super::super::result::DdlResult) directly and
//! carry no pgwire types. Every `CREATE ... INDEX` surface parses through the
//! shared grammar in [`options`], so an option keyword these handlers do not
//! understand is an error rather than a token they skip.

mod crdt_merge;
pub(super) mod options;
mod search_fusion;
mod sparse_index;
mod support;
mod text_index;
mod vector_index;

pub use crdt_merge::crdt_merge;
pub use search_fusion::search_fusion;
pub use sparse_index::create_sparse_index;
pub use text_index::{create_fulltext_index, create_search_index};
pub use vector_index::create_vector_index;
