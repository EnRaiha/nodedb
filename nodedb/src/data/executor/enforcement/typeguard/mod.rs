// SPDX-License-Identifier: BUSL-1.1

//! Typeguard enforcement: per-field DEFAULT / VALUE injection plus type,
//! REQUIRED and CHECK validation, on every write a schemaless collection
//! stores.

pub mod check;
pub mod compiled;
pub mod hook;
pub mod inject;

pub use check::check_type_guards;
pub use compiled::{CompiledTypeGuard, GuardClauses};
pub use hook::{GuardScope, enforce_type_guards};
pub use inject::inject_defaults;
