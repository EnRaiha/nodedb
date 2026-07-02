// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch shared by native + http entrypoints.
pub mod dispatch;
pub mod result;

pub use self::dispatch::dispatch;
pub use self::result::{DdlError, DdlResult};
