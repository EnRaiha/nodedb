// SPDX-License-Identifier: BUSL-1.1

//! Risk scoring: combine signals into a score, expose as `$auth.risk_score`
//! in RLS, and refuse the request when the score crosses the deny band.

pub mod address;
pub mod cache;
pub mod config;
pub mod gate;
pub mod scorer;

pub use address::client_ip_from_peer;
pub use config::{RiskConfig, RiskDecision};
pub use gate::{RiskRefusal, STEP_UP_REQUIRED};
pub use scorer::RiskScorer;
