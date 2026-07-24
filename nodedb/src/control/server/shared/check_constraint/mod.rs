// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral runtime enforcement for general CHECK constraints.
//!
//! Relocated from the pgwire `ddl::collection::check_constraint` module (now
//! deleted): this is runtime write-path enforcement, not a DDL handler, so it
//! does not live under `ddl/`. See [`enforce::enforce_check_constraints`] for
//! the evaluation strategy.

mod enforce;
mod simple;
mod subquery;

pub use enforce::enforce_check_constraints;
pub(crate) use subquery::validate_in_subquery_check;
