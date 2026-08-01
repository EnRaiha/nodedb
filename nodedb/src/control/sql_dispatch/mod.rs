// SPDX-License-Identifier: BUSL-1.1

pub mod dispatch;
pub mod outcome;

pub use dispatch::{dispatch_sql, dispatch_sql_in_database};
pub use outcome::DispatchOutcome;
