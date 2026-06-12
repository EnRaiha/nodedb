// SPDX-License-Identifier: Apache-2.0

//! Validator input/output types.

use crate::dead_letter::CompensationHint;
use loro::LoroValue;
use nodedb_types::Surrogate;

/// Outcome of validating a proposed change against constraints.
#[derive(Debug)]
pub enum ValidationOutcome {
    /// All constraints satisfied — safe to commit.
    Accepted,
    /// One or more constraints violated — delta rejected.
    Rejected(Vec<Violation>),
}

/// A single constraint violation.
#[derive(Debug, Clone)]
pub struct Violation {
    /// The constraint that was violated.
    pub constraint_name: String,
    /// Human-readable reason.
    pub reason: String,
    /// Suggested fix.
    pub hint: CompensationHint,
}

/// A proposed row change to validate.
#[derive(Debug, Clone)]
pub struct ProposedChange {
    /// Target collection.
    pub collection: String,
    /// Row ID being inserted/updated.
    pub row_id: String,
    /// Stable cross-engine identity assigned at the boundary.
    ///
    /// `Surrogate::ZERO` is the in-test sentinel and is also accepted
    /// for legacy callers that have not yet plumbed an assigner. UNIQUE
    /// and FK checks key on this when non-zero, so cross-engine bitmap
    /// joins reference the same row identity.
    pub surrogate: Surrogate,
    /// Field values being set.
    ///
    /// For bitemporal collections, the reserved columns `_ts_valid_from`
    /// and `_ts_valid_until` appear in this vec alongside user fields;
    /// constraint validation inspects them via
    /// [`crate::validator::bitemporal`] helpers.
    pub fields: Vec<(String, LoroValue)>,
}
