// SPDX-License-Identifier: Apache-2.0

//! WHERE-clause subquery planning: `IN`, `EXISTS`, and scalar subqueries
//! rewritten into semi / anti / cross joins.

pub mod exists;
pub mod extract;
pub mod in_list;
pub mod scalar;

pub use extract::{SubqueryExtraction, SubqueryJoin, extract_subqueries};
