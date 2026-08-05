// SPDX-License-Identifier: BUSL-1.1

//! `RedactionPolicy` / `RedactionRule` / `RedactionMode` data shapes and the
//! internal `policy_key` helper used by the store.

use serde::{Deserialize, Serialize};

/// A redaction policy: specifies which fields to redact for which roles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionPolicy {
    /// Policy name.
    pub name: String,
    /// Tenant scope.
    pub tenant_id: u64,
    /// Collection this policy applies to.
    pub collection: String,
    /// Role this policy applies to (e.g., "support").
    pub for_role: String,
    /// Field → redaction rule.
    pub rules: Vec<RedactionRule>,
}

/// A single field redaction rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedactionRule {
    /// Field name to redact.
    pub field: String,
    /// Redaction mode.
    pub mode: RedactionMode,
}

/// How a field value is redacted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedactionMode {
    /// Replace with a static mask string (e.g., "***@***.com").
    Mask(String),
    /// Replace with SHA-256 hash of the original value (pseudonymization).
    /// Joinable across queries but not human-readable.
    Hash,
    /// Replace with null.
    Null,
}

/// Build the lookup key for the policy map: `"{tenant_id}:{collection}:{for_role}"`.
pub(super) fn policy_key(tenant_id: u64, collection: &str, for_role: &str) -> String {
    format!("{tenant_id}:{collection}:{for_role}")
}
