// SPDX-License-Identifier: Apache-2.0

//! Dense integer CSR adjacency index with interned node IDs and labels.
//!
//! Split across:
//! - `types`     — struct definition, constructor, `Default`
//! - `interning` — node + label string↔id interning, node-label bitset
//! - `mutation`  — `add_edge`, `remove_edge`, `remove_node_edges`
//! - `lookup`    — neighbor queries, accessors, degree, iterators
//! - `scoped`    — collection-scoped read paths (MATCH / RAG)

pub mod interning;
pub mod lookup;
pub mod mutation;
pub mod scoped;
pub mod types;

#[cfg(test)]
mod tests;

pub use types::CsrIndex;
// Re-export shared Direction from nodedb-types via the types submodule.
pub use types::Direction;
