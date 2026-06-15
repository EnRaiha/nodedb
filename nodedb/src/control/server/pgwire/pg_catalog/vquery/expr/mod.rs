// SPDX-License-Identifier: BUSL-1.1

//! Expression AST + evaluator for virtual-table queries.

pub mod cast;
pub mod eval;
pub mod types;

pub use cast::{CatalogResolver, EvalCtx};
pub use eval::{apply_binary, eval, truthy};
pub use types::{AggFn, BinOp, CastType, EvalError, Expr, ScalarFn};
