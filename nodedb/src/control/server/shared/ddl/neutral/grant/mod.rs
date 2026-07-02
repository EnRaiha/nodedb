// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `GRANT` / `REVOKE` DDL handlers (role membership,
//! object permissions, database permissions).

pub mod database_permission;
pub mod permission;
pub mod role;
pub mod support;
