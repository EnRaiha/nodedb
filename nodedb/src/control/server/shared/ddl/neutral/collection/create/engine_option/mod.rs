// SPDX-License-Identifier: BUSL-1.1

//! Engine-name validation for `CREATE COLLECTION` and `CREATE TABLE`.
//!
//! The typed DDL parser reads the engine from `WITH (engine='<name>')`,
//! `ENGINE = <name>` or `TYPE <kind>`. `validate_engine_name` checks that
//! name against the canonical engines and refuses `WITH (profile='...')` and
//! `WITH (vector_field='...')` without an engine.
//!
//! `validate_engine_name` has one caller (`create::build::build_and_persist`).
//! Errors use `Result<_, DdlError>`.

pub mod validate;

pub use validate::{CANONICAL_ENGINES, validate_engine_name};
