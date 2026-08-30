// SPDX-License-Identifier: BUSL-1.1

//! Procedural SQL parser.
//!
//! Converts a token stream from the tokenizer into a `ProceduralBlock` AST.
//! Split into sub-modules by concern: statement parsers, exception handlers, utilities.

mod block;
mod exception;
pub(crate) mod statements;
mod utils;

pub use block::parse_block;
pub(crate) use block::parse_statements;
