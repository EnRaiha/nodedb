// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral trigger DDL — CREATE / DROP / ALTER / SHOW.

pub mod create;
pub mod drop;
pub mod show;

pub use create::create_trigger;
pub use drop::{alter_trigger, drop_trigger, trigger_exists};
pub use show::show_triggers;
