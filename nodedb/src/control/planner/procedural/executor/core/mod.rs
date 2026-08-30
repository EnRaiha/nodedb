// SPDX-License-Identifier: BUSL-1.1

//! Statement executor for procedural SQL blocks with DML.
//!
//! Split into sub-modules:
//! - `state`: StatementExecutor struct, construction, cross-shard/mutation state
//! - `block`: block-level execution with exception handling
//! - `statement`: single-statement dispatch
//! - `control_flow`: IF/WHILE/LOOP/FOR execution
//! - `dispatch`: DML dispatch, ASSIGN, RETURN, transaction control

mod block;
mod control_flow;
mod dispatch;
pub mod sql_literal_concat;
mod state;
mod statement;

pub(super) use state::Flow;
pub use state::{CrossShardOrigin, MAX_CASCADE_DEPTH, StatementExecutor};
