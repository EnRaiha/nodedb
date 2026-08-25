// SPDX-License-Identifier: BUSL-1.1

//! `RlsPolicy` / `PolicyType` data shapes and the internal
//! `policy_key` helper used by the store.

use serde::{Deserialize, Serialize};

use crate::control::security::deny::DenyMode;
use crate::control::security::predicate::{PolicyMode, RlsPredicate};

/// A single RLS policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    /// Policy name (unique per collection).
    pub name: String,
    /// Collection this policy applies to, qualified with the owning
    /// database ID (see `db_qualified`). This is the lookup key used by
    /// the store and by enforcement — never show it to a user.
    pub collection: String,
    /// The collection name as the user wrote it in the DDL, unqualified.
    /// Display-only: `SHOW RLS POLICIES` renders this, not `collection`.
    pub display_collection: String,
    /// Tenant scope.
    pub tenant_id: u64,
    /// Policy type: read, write, or both.
    pub policy_type: PolicyType,
    /// Compiled predicate AST with `$auth.*` support.
    ///
    /// Substituted at plan time via `AuthContext` to produce concrete
    /// `ScanFilter` values. `None` means the policy has no row filter
    /// (vacuous — all rows pass).
    #[serde(default)]
    pub compiled_predicate: Option<RlsPredicate>,
    /// Policy combination mode: permissive (OR) or restrictive (AND).
    #[serde(default)]
    pub mode: PolicyMode,
    /// What happens when this policy denies access.
    #[serde(default)]
    pub on_deny: DenyMode,
    /// Whether this policy is enabled.
    pub enabled: bool,
    /// Creator username (for audit).
    pub created_by: String,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
}

/// Policy type: when the policy is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    /// Applied on SELECT/read — filters rows the user can see.
    Read,
    /// Applied on INSERT/UPDATE/DELETE — blocks disallowed writes.
    Write,
    /// Applied on both read and write paths.
    All,
}

/// Build the lookup key for the policy map: `"{tenant_id}:{collection}"`.
pub(super) fn policy_key(tenant_id: u64, collection: &str) -> String {
    format!("{tenant_id}:{collection}")
}
