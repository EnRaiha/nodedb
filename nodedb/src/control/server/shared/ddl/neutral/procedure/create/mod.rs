// SPDX-License-Identifier: BUSL-1.1

//! `CREATE [OR REPLACE] PROCEDURE` — split by concern.
//!
//! - [`handler`] — the `create_procedure` protocol-neutral entry point
//! - [`parse`] — SQL surface-grammar parser for `CREATE PROCEDURE`
//! - [`routability`] — DML-target analysis that classifies a
//!   procedure body as `SingleCollection` / `MultiCollection` for
//!   vShard affinity routing

pub mod handler;
pub mod parse;
pub mod routability;

pub use handler::create_procedure;
