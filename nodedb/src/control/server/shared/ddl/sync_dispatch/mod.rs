// SPDX-License-Identifier: BUSL-1.1

//! Async Data-Plane dispatch for DDL, DSL, and system-initiated work.
//!
//! Two doors, and the type system says which one a caller took: an
//! [`AuthorizedTask`](crate::control::server::shared::authorization::AuthorizedTask)
//! for work a user asked for, or a [`SystemTask`] naming why no user exists.

mod dispatch;
mod system_task;

pub(crate) use dispatch::{
    dispatch_authorized, dispatch_system, dispatch_system_response_with_source,
};
pub(crate) use system_task::{SystemReason, SystemTask};
