// SPDX-License-Identifier: BUSL-1.1

//! sqlparser AST → internal `VSelect` representation.

pub mod error;
pub mod lower;
pub mod parse;
pub mod types;

pub use error::ParseError;
pub use parse::{parse_select, parse_select_with_params};
pub use types::{FromClause, FromRel, JoinKind, JoinSpec, VProj, VSelect};
