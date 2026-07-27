// SPDX-License-Identifier: Apache-2.0

//! Machine-matchable structured error details.

use serde::{Deserialize, Serialize};

/// Structured error details for programmatic matching.
///
/// Clients match on the variant to determine the error category, then
/// extract structured fields. The `message` on [`crate::error::NodeDbError`]
/// carries the human-readable explanation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ErrorDetails {
    // Write path
    #[serde(rename = "constraint_violation")]
    ConstraintViolation { collection: String },
    #[serde(rename = "write_conflict")]
    WriteConflict {
        collection: String,
        document_id: String,
    },
    #[serde(rename = "deadline_exceeded")]
    DeadlineExceeded,
    #[serde(rename = "prevalidation_rejected")]
    PrevalidationRejected { constraint: String },
    #[serde(rename = "append_only_violation")]
    AppendOnlyViolation { collection: String },
    #[serde(rename = "balance_violation")]
    BalanceViolation { collection: String },
    #[serde(rename = "period_locked")]
    PeriodLocked { collection: String },
    #[serde(rename = "state_transition_violation")]
    StateTransitionViolation { collection: String },
    #[serde(rename = "transition_check_violation")]
    TransitionCheckViolation { collection: String },
    #[serde(rename = "type_guard_violation")]
    TypeGuardViolation { collection: String },
    #[serde(rename = "retention_violation")]
    RetentionViolation { collection: String },
    #[serde(rename = "legal_hold_active")]
    LegalHoldActive { collection: String },
    #[serde(rename = "type_mismatch")]
    TypeMismatch { collection: String },
    #[serde(rename = "overflow")]
    Overflow { collection: String },
    #[serde(rename = "insufficient_balance")]
    InsufficientBalance { collection: String },
    #[serde(rename = "rate_exceeded")]
    RateExceeded { gate: String },

    // Read path
    #[serde(rename = "collection_not_found")]
    CollectionNotFound { collection: String },
    #[serde(rename = "document_not_found")]
    DocumentNotFound {
        collection: String,
        document_id: String,
    },
    #[serde(rename = "collection_draining")]
    CollectionDraining { collection: String },
    #[serde(rename = "collection_deactivated")]
    CollectionDeactivated {
        collection: String,
        /// Wall-clock nanoseconds when retention elapses and the
        /// collection becomes unrecoverable. Clients can render a
        /// human-readable countdown.
        retention_expires_at_ns: u64,
        /// Copy-pasteable SQL the user can run to restore the
        /// collection. Populated with the actual name, so the error
        /// is actionable without further lookup.
        undrop_hint: String,
    },

    // Query
    #[serde(rename = "plan_error")]
    PlanError { phase: String, detail: String },
    #[serde(rename = "fan_out_exceeded")]
    FanOutExceeded { shards_touched: u16, limit: u16 },
    #[serde(rename = "sql_not_enabled")]
    SqlNotEnabled,
    /// A function call names no registered scalar/aggregate/window function.
    #[serde(rename = "undefined_function")]
    UndefinedFunction { name: String },
    /// Expression evaluation divided or took a modulus by zero.
    #[serde(rename = "division_by_zero")]
    DivisionByZero,

    // Auth
    #[serde(rename = "authorization_denied")]
    AuthorizationDenied { resource: String },
    #[serde(rename = "auth_expired")]
    AuthExpired,
    /// Tenant quota: vector dimension exceeds `max_vector_dim`.
    #[serde(rename = "tenant_vector_dim_exceeded")]
    TenantVectorDimExceeded { dim: u32, limit: u32 },
    /// Tenant quota: graph traversal depth exceeds `max_graph_depth`.
    #[serde(rename = "tenant_graph_depth_exceeded")]
    TenantGraphDepthExceeded { depth: u32, limit: u32 },

    // Protocol handshake
    #[serde(rename = "handshake_failed")]
    HandshakeFailed {
        /// Numeric error code sent by the server (0=BadMagic, 1=VersionMismatch, 2=Malformed).
        server_code: u8,
    },

    // Sync
    #[serde(rename = "sync_connection_failed")]
    SyncConnectionFailed,
    #[serde(rename = "sync_delta_rejected")]
    SyncDeltaRejected {
        compensation: Option<crate::sync::compensation::CompensationHint>,
    },
    #[serde(rename = "shape_subscription_failed")]
    ShapeSubscriptionFailed { shape_id: String },

    // Storage (opaque infrastructure)
    #[serde(rename = "storage")]
    Storage {
        component: String,
        op: String,
        detail: String,
    },
    #[serde(rename = "segment_corrupted")]
    SegmentCorrupted {
        segment_id: u64,
        corruption: String,
        detail: String,
    },
    #[serde(rename = "cold_storage")]
    ColdStorage {
        backend: String,
        op: String,
        detail: String,
    },
    #[serde(rename = "wal")]
    Wal { stage: String, detail: String },

    // Serialization
    #[serde(rename = "serialization")]
    Serialization { format: String },
    #[serde(rename = "codec")]
    Codec {
        codec: String,
        op: String,
        detail: String,
    },

    // Config
    #[serde(rename = "config")]
    Config,
    #[serde(rename = "bad_request")]
    BadRequest,

    // Cluster
    #[serde(rename = "no_leader")]
    NoLeader,
    #[serde(rename = "not_leader")]
    NotLeader { leader_addr: String },
    #[serde(rename = "migration_in_progress")]
    MigrationInProgress,
    #[serde(rename = "node_unreachable")]
    NodeUnreachable,
    #[serde(rename = "cluster")]
    Cluster,

    // Memory
    #[serde(rename = "memory_exhausted")]
    MemoryExhausted { engine: String },

    // Encryption
    #[serde(rename = "encryption")]
    Encryption { cipher: String, detail: String },

    // Engine ops
    #[serde(rename = "array")]
    Array { array: String },

    // Quota
    #[serde(rename = "quota_overcommit")]
    QuotaOvercommit { field: String },
    #[serde(rename = "quota_exceeded")]
    QuotaExceeded { scope: String },
    #[serde(rename = "server_overload")]
    ServerOverload,

    // Clone DDL
    #[serde(rename = "clone_depth_exceeded")]
    CloneDepthExceeded { depth: u32, limit: u32 },
    #[serde(rename = "cannot_clone_mirror")]
    CannotCloneMirror { database: String },
    #[serde(rename = "clone_dependency")]
    CloneDependency { dependents: Vec<String> },
    #[serde(rename = "clone_predates_query_time")]
    ClonePredatesQueryTime { as_of_lsn: u64, created_at_lsn: u64 },

    // Mirror DDL
    /// Write attempted on a mirror database that has not been promoted.
    #[serde(rename = "mirror_read_only")]
    MirrorReadOnly { database: String },
    /// Strong read requested on a mirror; the client should contact the source.
    #[serde(rename = "stale_read_not_leader")]
    StaleReadNotLeader {
        database: String,
        /// Hint: source cluster endpoint the client should redirect to.
        source_cluster: String,
    },
    /// Operation requires the database to be a promoted mirror.
    #[serde(rename = "mirror_not_promoted")]
    MirrorNotPromoted { database: String },

    // Move Tenant DDL
    #[serde(rename = "move_tenant_drain_timeout")]
    MoveTenantDrainTimeout { tenant: String, source_db: String },
    #[serde(rename = "move_tenant_preflight_failed")]
    MoveTenantPreflightFailed { tenant: String, detail: String },
    #[serde(rename = "move_tenant_snapshot_failed")]
    MoveTenantSnapshotFailed { tenant: String, detail: String },
    #[serde(rename = "move_tenant_cutover_failed")]
    MoveTenantCutoverFailed { tenant: String, detail: String },
    #[serde(rename = "move_tenant_already_at_target")]
    MoveTenantAlreadyAtTarget { tenant: String, target_db: String },

    // Bridge / Dispatch / Internal
    #[serde(rename = "bridge")]
    Bridge {
        plane: String,
        op: String,
        detail: String,
    },
    #[serde(rename = "dispatch")]
    Dispatch { stage: String, detail: String },
    #[serde(rename = "internal")]
    Internal { component: String, detail: String },
}
