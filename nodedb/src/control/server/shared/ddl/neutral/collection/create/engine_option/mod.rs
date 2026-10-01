// SPDX-License-Identifier: BUSL-1.1

//! Canonical engine-option parsing for `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! The single accepted syntax is `WITH (engine='<name>')`. All legacy axes —
//! `TYPE <keyword>`, `WITH (profile='...')`, or bare `WITH (vector_field='...')`
//! without an explicit engine — are rejected hard with a helpful SQLSTATE error.
//!
//! `validate_engine_name` has one caller (`create::build::build_and_persist`).
//! Errors use `Result<_, DdlError>`.

pub mod parse;
pub mod validate;

pub use parse::parse_engine_option;
pub use validate::{CANONICAL_ENGINES, validate_engine_name};
