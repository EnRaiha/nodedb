// SPDX-License-Identifier: BUSL-1.1

//! Backward-compatible re-exports from the split trigger fire modules.
//!
//! This file exists so that existing call sites (`control::trigger::fire::fire_after_insert`)
//! continue to work. New code should import from the specific sub-modules.

pub use super::fire_after::{
    FireAfterDeleteParams, FireAfterInsertParams, FireAfterUpdateParams, fire_after_delete,
    fire_after_insert, fire_after_update, fire_sql,
};
pub use super::fire_before::{fire_before_delete, fire_before_insert, fire_before_update};
pub use super::fire_instead::{
    InsteadOfResult, InsteadOfUpdateParams, fire_instead_of_delete, fire_instead_of_insert,
    fire_instead_of_update,
};
