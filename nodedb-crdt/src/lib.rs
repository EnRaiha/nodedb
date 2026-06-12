// SPDX-License-Identifier: Apache-2.0

//! # nodedb-crdt
//!
//! CRDT engine with SQL constraint validation for NodeDB.
//!
//! ## The CRDT / SQL Paradox
//!
//! CRDTs are AP (Available + Partition-tolerant): agents compute optimistic
//! deltas offline and sync later. SQL constraints are CP (Consistent +
//! Partition-tolerant): UNIQUE indexes, foreign keys, etc. must hold globally.
//!
//! This crate bridges the gap:
//!
//! 1. **Optimistic local writes** — agents apply deltas to their local `LoroDoc`
//!    without constraint checks (AP behavior for availability).
//! 2. **Constraint validation at commit** — when deltas sync to the leader,
//!    constraints are validated against the committed state.
//! 3. **Dead-letter queue** — rejected deltas are routed to a DLQ with
//!    compensation hints so the application can recover gracefully.
//! 4. **Pre-validation** — optional fast-reject against the leader's state
//!    before the full Raft round-trip, reducing wasted consensus bandwidth.

/// Authentication context threaded through CRDT validation.
///
/// Carries the identity of who submitted the delta so the DLQ and deferred
/// queue can attribute entries to the correct user/tenant.
///
/// ## Replay protection
///
/// `device_id` and `seq_no` close the replay vulnerability: a captured
/// delta cannot be resubmitted because `seq_no` must be strictly greater
/// than `last_seen[(user_id, device_id)]` on the server. The HMAC input
/// binds the seq_no and device_id so they cannot be altered after signing.
///
/// ## Required fields
///
/// All fields, including `device_id` and `seq_no`, must be present in the
/// serialized form. Missing fields are a hard decode error.
#[derive(Debug, Clone, Copy, Default, serde::Serialize, serde::Deserialize)]
pub struct CrdtAuthContext {
    /// Authenticated user_id (0 = unauthenticated).
    pub user_id: u64,
    /// Tenant this operation belongs to.
    pub tenant_id: u64,
    /// Unix timestamp (milliseconds) when this auth session expires.
    /// 0 = no expiry (trust mode).
    /// Agents accumulating deltas offline must re-authenticate before
    /// syncing if their auth context has expired.
    pub auth_expires_at: u64,
    /// HMAC signature over delta bytes (optional delta signing).
    /// All-zeros = unsigned. When non-zero, the validator verifies this
    /// before accepting, and also enforces replay protection.
    pub delta_signature: [u8; 32],
    /// Stable per-device identifier assigned by the server on first bind.
    pub device_id: u64,
    /// Monotonically increasing per-device sequence number.
    /// Must be strictly greater than last_seen[(user_id, device_id)] on the server.
    pub seq_no: u64,
}

pub mod constraint;
pub mod constraint_checks;
pub mod dead_letter;
pub mod deferred;
pub mod error;
pub mod list_ops;
pub mod policy;
pub mod pre_validate;
pub mod signing;
pub mod state;
pub mod validator;

pub use constraint::{Constraint, ConstraintKind, ConstraintSet};
pub use dead_letter::{CompensationHint, DeadLetterQueue};
pub use deferred::DeferredQueue;
pub use error::{CrdtError, Result};
pub use policy::{
    CollectionPolicy, ConflictPolicy, PolicyRegistry, PolicyResolution, ResolvedAction,
};
pub use signing::{DeltaSigner, DeviceRegistry};
pub use state::CrdtState;
pub use validator::{ValidationOutcome, Validator};
