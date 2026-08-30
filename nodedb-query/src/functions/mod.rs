// SPDX-License-Identifier: Apache-2.0

//! Scalar function evaluation for SqlExpr.
//!
//! All functions return `Value::Null` on invalid/missing
//! arguments (SQL NULL propagation semantics).

mod array;
mod conditional;
mod datetime;
mod eval;
pub(crate) mod fts;
mod id;
mod json;
mod math;
pub(crate) mod shared;
mod string;
mod system;
mod types;

pub use eval::eval_function;
