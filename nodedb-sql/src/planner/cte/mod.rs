// SPDX-License-Identifier: Apache-2.0

//! CTE (WITH clause) and WITH RECURSIVE planning.

pub mod inline;
mod join_link;
mod recursive_scan;
mod recursive_value;
mod validate;

pub use inline::{inline_cte, reads_cte_body_unchanged};
pub use recursive_scan::{DEFAULT_MAX_RECURSION_DEPTH, plan_recursive_cte};
