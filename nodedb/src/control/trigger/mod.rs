// SPDX-License-Identifier: BUSL-1.1

pub mod batch;
mod condition;
pub mod dml_hook;
pub mod dml_hook_fire;
pub mod fire;
pub mod fire_after;
pub mod fire_before;
pub mod fire_common;
pub mod fire_instead;
pub mod fire_statement;
pub mod registry;
pub mod row_identity;
pub mod scope;
pub mod when_parse;

pub use condition::try_eval_simple_condition;
pub use registry::{DmlEvent, TriggerRegistry};
pub use scope::TriggerScope;
