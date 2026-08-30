// SPDX-License-Identifier: Apache-2.0

//! HNSW graph structure — nodes, parameters, core index operations.
//!
//! Production implementation per Malkov & Yashunin (2018).
//! FP32 construction for structural integrity; heuristic neighbor selection.

pub mod index;
mod limits;
pub mod types;

pub(crate) use limits::ARENA_INITIAL_CAPACITY;
pub use limits::MAX_LAYER_CAP;

pub use index::HnswIndex;
pub use nodedb_types::hnsw::HnswParams;
pub use types::{Candidate, Node, NodeStorage, SearchResult, Xorshift64};
