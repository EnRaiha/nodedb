// SPDX-License-Identifier: BUSL-1.1

//! Shared grammar for the `CREATE <kind> INDEX` DDL surfaces: a
//! quote- and paren-aware tokenizer, a header parser, and a closed-set
//! option-keyword parser.
//!
//! Every handler on these surfaces parses through this module so that a
//! statement is either understood exactly as written or rejected. Scanning a
//! token tail for recognized keywords cannot distinguish "option absent" from
//! "option misspelled", so it accepted statements it had not understood.

mod header;
mod keywords;
mod lex;
mod statement;

pub use header::{ColumnMode, HeaderSpec, NameMode};
pub use keywords::{OptionSpec, ParsedOptions, closed_set};
pub use statement::{IndexStatement, parse_index_statement};
