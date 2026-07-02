//! Protocol-neutral machinery shared by every server entrypoint (pgwire, native, http).
pub mod ddl;
pub mod session;
pub mod sql;
