// SPDX-License-Identifier: BUSL-1.1

//! Persisted per-scope token quota definition.
//!
//! The enforcement mode is stored as a string rather than the
//! [`QuotaEnforcement`](crate::control::security::metering::quota::QuotaEnforcement)
//! enum so a catalog written by a newer build stays readable by an older one:
//! an unrecognised mode is a decode error the loader can skip and report,
//! not a variant-index mismatch that shifts every field after it.

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct StoredScopeQuota {
    /// Scope this quota applies to — the primary key.
    pub scope_name: String,
    /// Maximum tokens per period.
    pub max_tokens: u64,
    /// Period length in seconds.
    pub period_secs: u64,
    /// Enforcement mode: `hard`, `soft`, `throttle`, or `overage`.
    pub enforcement: String,
    /// Warning threshold as a fraction of `max_tokens` (0.0–1.0).
    pub warning_threshold: f64,
}
