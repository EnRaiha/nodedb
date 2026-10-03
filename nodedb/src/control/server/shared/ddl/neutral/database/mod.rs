// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral database DDL family handlers (CREATE / DROP / ALTER
//! DATABASE, SHOW DATABASES / QUOTA / USAGE / LINEAGE, CLONE / MIRROR / PROMOTE,
//! BACKUP / RESTORE). Every catalog / data-plane / audit / privilege-gate
//! side effect runs in these handlers.
//!
//! `USE DATABASE` is intentionally NOT here — it is session-coupled (mutates the
//! per-connection current database) and stays on the pgwire side.

pub mod alter;
pub mod backup_restore;
pub mod clone;
pub mod create;
pub mod drop;
pub mod gate;
pub mod materialize;
pub mod mirror;
pub mod show;
pub mod show_lineage;
pub mod show_quota;
pub mod show_usage;
pub mod support;
pub mod teardown;
