// SPDX-License-Identifier: BUSL-1.1

//! Auto-escalation: repeated violations → suspend → ban.

pub mod config;
pub mod engine;
pub mod violation;

pub use config::EscalationConfig;
pub use engine::{Escalation, EscalationEngine};
pub use violation::{AuthViolation, ViolationSubject, record_auth_violation};
