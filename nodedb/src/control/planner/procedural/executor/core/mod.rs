// SPDX-License-Identifier: BUSL-1.1

//! Statement executor for procedural SQL blocks with DML.
//!
//! Split into sub-modules:
//! - `state`: StatementExecutor struct, construction, cross-shard/mutation state
//! - `block`: block-level execution with exception handling
//! - `statement`: single-statement dispatch
//! - `control_flow`: IF/WHILE/LOOP/FOR execution
//! - `dispatch`: DML dispatch, ASSIGN, RETURN, transaction control
//! - `body`: server-run bodies that execute as one transaction
//! - `route`: local-or-remote placement of a trigger statement
//! - `outbox`: committing cross-node writes and PUBLISH as redo-record
//!   messages
//! - `applied`: the key a body's commit records, so a body fired again
//!   applies once

mod applied;
mod block;
mod body;
mod control_flow;
mod dispatch;
mod outbox;
mod route;
pub mod sql_literal_concat;
mod state;
mod statement;

pub use body::AtomicBody;
pub(super) use state::Flow;
pub use state::{CrossShardOrigin, MAX_CASCADE_DEPTH, StatementExecutor};
