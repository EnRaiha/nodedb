// SPDX-License-Identifier: BUSL-1.1

//! Transactions the database initiates for itself.
//!
//! A trigger body, stored procedure, scheduled job, alert write or DEFINE
//! EVENT THEN action runs with no client transaction around it. Running its
//! tasks as one transaction here gives that work the same atomicity,
//! descriptor fencing, and conflict detection a client statement gets — and
//! is what lets a failed action be retried without repeating the part of it
//! that already applied. [`OpenSystemTxn`] keeps the transaction open across
//! a body's statements so each one sees the writes before it.

mod data_plane;
mod joined;
mod live;
mod run;
mod scope;
#[cfg(test)]
mod tests;

pub use self::live::OpenSystemTxn;
pub use self::run::{
    SystemTxnError, SystemTxnStatement, run_statements_atomically, run_tasks_atomically,
};
pub use self::scope::SystemTxnScope;
