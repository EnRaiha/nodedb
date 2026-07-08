// SPDX-License-Identifier: BUSL-1.1

//! Transaction command handlers: BEGIN, COMMIT, ROLLBACK, SAVEPOINT.
//!
//! Extracted from `sql_exec.rs` — handles all transactional state management
//! including snapshot isolation conflict detection, WAL transaction batching,
//! GAP_FREE sequence reservation lifecycle, and deferred offset commits.
//!
//! `errors` holds the shared error constructors, `begin_rollback` holds
//! BEGIN/ROLLBACK plus the shared staging-overlay release helper, and
//! `commit` holds the COMMIT path (conflict detection, WAL batching, Calvin
//! dispatch, and post-commit finalization).

mod begin_rollback;
mod commit;
mod errors;
#[cfg(test)]
mod tests;

pub(in crate::control::server::pgwire::handler) use commit::PgwireTxnDp;
