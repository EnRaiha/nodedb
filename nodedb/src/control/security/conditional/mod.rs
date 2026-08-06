// SPDX-License-Identifier: BUSL-1.1

//! Conditional scope grants: temporal windows, MFA, IP ranges, step-up
//! authentication, and device trust.
//!
//! Conditions are attached to a grant by `GRANT SCOPE ... WHEN/REQUIRE` and
//! evaluated once per request, where the scope set is resolved.

pub mod condition;
pub mod evaluate;
pub mod parse;

pub use condition::{DEFAULT_STEP_UP_SECS, GrantCondition, render_conditions};
pub use evaluate::evaluate_conditions;
pub use parse::parse_conditions;
