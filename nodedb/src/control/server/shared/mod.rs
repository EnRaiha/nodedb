//! Protocol-neutral machinery shared by every server entrypoint (pgwire, native, http).
pub mod authorization;
pub mod check_constraint;
pub mod ddl;
pub mod panic_isolation;
pub mod plan_util;
pub mod planning_overrides;
pub mod session;
pub mod sql;
pub mod write_admission;

pub use panic_isolation::{ConnectionFutureOutcome, isolate_connection_future};
