// SPDX-License-Identifier: BUSL-1.1

//! A statement's tasks in its transaction: row triggers, clone copy-on-write
//! and staging, shared by every protocol's statement loop.

pub mod joined;
mod predicate;
pub mod route;
mod row_triggers;

pub use joined::{
    statement_fires_joined_body, statement_needs_implicit_txn, unbufferable_joined_statement,
};
pub use route::{StatementEvents, TxnTaskContext, TxnTaskOutcome, route_txn_task};
