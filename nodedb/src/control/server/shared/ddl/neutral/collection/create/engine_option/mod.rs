// SPDX-License-Identifier: BUSL-1.1

//! Canonical engine-option parsing for `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! The single accepted syntax is `WITH (engine='<name>')`. All legacy axes —
//! `TYPE <keyword>`, `WITH (profile='...')`, or bare `WITH (vector_field='...')`
//! without an explicit engine — are rejected hard with a helpful SQLSTATE error.
//!
//! Relocated from `pgwire::ddl::collection::create::engine_option` (now
//! deleted): `validate_engine_name` had exactly one caller
//! (`create::build::build_and_persist`), which moved here too, so keeping this
//! module on the pgwire side would have left neutral importing back across the
//! pgwire boundary for no other reason. Only the error envelope changed
//! (`PgWireResult` → `Result<_, DdlError>`); every SQLSTATE code and message is
//! byte-identical.

pub mod parse;
pub mod validate;

pub use parse::parse_engine_option;
pub use validate::{CANONICAL_ENGINES, validate_engine_name};
