// SPDX-License-Identifier: Apache-2.0

//! SELECT query routing and search lowering.

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod join_columns;
mod payload;
mod pk_prefilter;
mod query;
mod search;

pub use query::{plan_query, plan_statement_query};
