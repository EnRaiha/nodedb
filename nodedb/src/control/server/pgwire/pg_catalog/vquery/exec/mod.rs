// SPDX-License-Identifier: BUSL-1.1

//! Virtual-query execution pipeline.

pub mod meta;
pub mod project;
pub mod run;

pub use meta::{OutColumn, ResultSet};
pub use run::{ExecError, execute};
