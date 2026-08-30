// SPDX-License-Identifier: BUSL-1.1

//! PromQL expression evaluator.
//!
//! Evaluates a parsed AST against a set of pre-fetched time series.
//! Pure computation — the caller is responsible for data fetching.

mod aggregate;
mod binary;
mod call;
mod context;
mod dispatch;
mod helpers;
mod query;
mod selector;

pub use context::EvalContext;
pub use helpers::{group_key, group_labels, labels_key, match_key};
pub use query::{evaluate_instant, evaluate_range};

pub(crate) use dispatch::eval;
