// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral tree-operations DDL family: `CREATE GRAPH INDEX`,
//! `TREE_SUM`, `TREE_CHILDREN`.
//!
//! These build on the existing CSR graph engine for hierarchical aggregation
//! over self-referential collections (e.g. chart of accounts with parent_id).

pub mod children;
pub mod create_index;
pub mod parse;
pub mod sum;
pub mod support;

pub use children::tree_children;
pub use create_index::create_graph_index;
pub use sum::tree_sum;
