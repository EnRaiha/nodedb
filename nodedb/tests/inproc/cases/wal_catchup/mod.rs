// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for WAL catch-up: verifies that timeseries records
//! written to WAL but not dispatched to the Data Plane become queryable
//! after the catch-up task re-dispatches them.
//!
//! A live write applies through its replicated entry, as every write that
//! yields change events does. A dropped dispatch is a record appended to the
//! WAL alone.

mod catchup;
mod stack;
mod startup_replay;
