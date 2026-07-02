// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL handlers for all constraint kinds: state transitions,
//! transition checks, general CHECK constraints, and SHOW CONSTRAINTS.

pub mod handlers;
pub mod parse;
pub mod show;
pub mod support;
pub mod validate;

pub use handlers::{
    add_check_constraint, add_state_constraint, add_transition_check, drop_constraint,
};
pub use show::show_constraints;
