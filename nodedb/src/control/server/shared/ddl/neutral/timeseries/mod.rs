// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral timeseries DDL family: `CREATE TIMESERIES`, `SHOW
//! PARTITIONS FOR`, `ALTER TIMESERIES`, `REWRITE PARTITIONS FOR`.

pub mod alter;
pub mod create;
mod helpers;
pub mod rewrite;
pub mod show;

pub use alter::alter_timeseries;
pub use create::create_timeseries;
pub use rewrite::rewrite_partitions;
pub use show::show_partitions;
