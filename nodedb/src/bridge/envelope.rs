// SPDX-License-Identifier: BUSL-1.1

use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

/// Response payload: heap-allocated bytes behind an `Arc<[u8]>`.
///
/// The `Deref<Target=[u8]>` impl provides transparent byte access.
/// Slab-backed zero-copy transport is defined in `super::slab` and will be
/// wired in once the Data Plane slab pool is integrated.
#[derive(Debug, Clone)]
pub enum Payload {
    /// Heap-allocated payload.
    Heap(Arc<[u8]>),
}

impl Payload {
    /// Create a heap-backed payload from a Vec.
    pub fn from_vec(v: Vec<u8>) -> Self {
        Self::Heap(Arc::from(v.into_boxed_slice()))
    }

    /// Create an empty payload.
    pub fn empty() -> Self {
        Self::Heap(Arc::from([].as_slice()))
    }

    /// Get the payload bytes.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Heap(a) => a,
        }
    }

    /// Whether this payload is empty.
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Convert to Vec<u8>.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Deref for Payload {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Payload {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(v: Vec<u8>) -> Self {
        Self::from_vec(v)
    }
}

impl From<Arc<[u8]>> for Payload {
    fn from(a: Arc<[u8]>) -> Self {
        Self::Heap(a)
    }
}
use crate::event::types::EventSource;
use crate::types::{
    DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
};

/// Request envelope: Control Plane -> Data Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Request {
    /// Globally unique request identifier (monotonic per connection).
    pub request_id: RequestId,

    /// Tenant scope — all data access is tenant-scoped by construction.
    pub tenant_id: TenantId,

    /// Database scope — identifies which catalog namespace this request targets.
    /// `DatabaseId::DEFAULT` (0) is the built-in `default` database.
    pub database_id: DatabaseId,

    /// Target virtual shard.
    pub vshard_id: VShardId,

    /// Opaque plan digest identifying the physical operation to execute.
    pub plan: PhysicalPlan,

    /// Absolute deadline. Data Plane MUST stop at next safe point after expiry.
    pub deadline: Instant,

    /// Request priority for scheduling on the Data Plane.
    pub priority: Priority,

    /// Distributed trace identifier for cross-plane observability.
    pub trace_id: TraceId,

    /// Read consistency level for this request.
    pub consistency: ReadConsistency,

    /// Optional idempotency key for non-idempotent writes.
    /// If present, the Data Plane deduplicates by skipping execution
    /// when the same key has already been processed (returns the
    /// cached response status).
    pub idempotency_key: Option<u64>,

    /// Origin of this DML request. Propagated to the Data Plane so that
    /// emitted WriteEvents carry the correct source tag. Trigger-generated
    /// writes use `EventSource::Trigger` to prevent cascade re-triggering.
    pub event_source: EventSource,

    /// Roles held by the authenticated user. Propagated to the Data Plane
    /// for role-guarded state transition enforcement (`BY ROLE 'manager'`).
    /// Empty for system-generated writes (triggers, CRDT sync, etc.).
    pub user_roles: Vec<String>,

    /// Authenticated user ID. Propagated to WriteEvents for DML audit attribution.
    /// `None` for system-generated writes (triggers, CRDT sync, Raft follower).
    pub user_id: Option<Arc<str>>,

    /// SQL plan digest identifying the statement that produced this request.
    /// Reuses the plan digest already computed by nodedb-sql. `None` for
    /// non-user writes.
    pub statement_digest: Option<Arc<str>>,

    /// Set when this write originates inside a session transaction block;
    /// keys the per-transaction staging overlay. `None` for autocommit /
    /// non-transactional / system requests.
    pub txn_id: Option<TxnId>,
}

/// Response envelope: Data Plane -> Control Plane.
///
/// Every field is mandatory.
#[derive(Debug, Clone)]
pub struct Response {
    /// Echoed request identifier for correlation.
    pub request_id: RequestId,

    /// Outcome status.
    pub status: Status,

    /// Attempt number (for retry tracking).
    pub attempt: u32,

    /// Whether this is a partial result (more coming).
    pub partial: bool,

    /// Payload bytes produced by this response chunk.
    pub payload: Payload,

    /// Watermark LSN at the time of read (for snapshot consistency tracking).
    pub watermark_lsn: Lsn,

    /// Error code if status is not Ok.
    pub error_code: Option<ErrorCode>,
}

pub use nodedb_physical::physical_plan::PhysicalPlan;

/// Request priority. Higher priority requests are scheduled first on the Data Plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Background tasks (compaction, GC).
    Background = 0,
    /// Normal query traffic.
    Normal = 1,
    /// Elevated (e.g., interactive queries with tight deadlines).
    High = 2,
    /// System-critical (WAL replay, leader election responses).
    Critical = 3,
}

/// Response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Success.
    Ok,
    /// Partial success — more response chunks follow.
    Partial,
    /// Request failed with error.
    Error,
}

/// Deterministic error codes returned by the Data Plane.
///
/// Final outcomes are explicit, never opaque strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// Constraint violation at commit time.
    ///
    /// `constraint` names the kind (`unique`, `not_null`, ...). `detail`
    /// carries the human-readable explanation (e.g. which primary-key
    /// value conflicted) so pgwire drivers can surface it to the user.
    RejectedConstraint { constraint: String, detail: String },
    /// Pre-validation fast-reject.
    RejectedPrevalidation { reason: String },
    /// Document/collection not found.
    NotFound,
    /// Authorization failure.
    RejectedAuthz,
    /// Write conflict — client should retry.
    ConflictRetry,
    /// Fan-out limit exceeded for graph/scatter queries.
    FanOutExceeded,
    /// Memory budget exhausted — DataFusion should spill.
    ResourcesExhausted,
    /// Edge creation rejected: source or destination node does not exist.
    RejectedDanglingEdge { missing_node: String },
    /// Duplicate write detected via idempotency key.
    DuplicateWrite,
    /// Append-only collection: UPDATE/DELETE not allowed.
    AppendOnlyViolation { collection: String },
    /// BALANCED constraint: debit/credit sums don't match.
    BalanceViolation { collection: String, detail: String },
    /// Period is closed/locked: writes rejected.
    PeriodLocked { collection: String },
    /// Retention period not expired: DELETE rejected.
    RetentionViolation { collection: String },
    /// Legal hold active: DELETE rejected.
    LegalHoldActive { collection: String },
    /// State transition not in allowed list.
    StateTransitionViolation { collection: String, detail: String },
    /// Transition check predicate returned false.
    TransitionCheckViolation { collection: String },
    /// Type guard violation: field type mismatch or REQUIRED absent.
    TypeGuardViolation { collection: String, detail: String },
    /// Value type does not match expected type for operation (e.g. INCR on a string).
    TypeMismatch { collection: String, detail: String },
    /// Arithmetic overflow (e.g. i64::MAX + 1 on INCR).
    OverflowError { collection: String },
    /// Insufficient balance for transfer (source lacks required amount).
    InsufficientBalance { collection: String, detail: String },
    /// Rate limit exceeded for a rate gate / cooldown.
    RateExceeded { gate: String, retry_after_ms: u64 },
    /// The collection is currently draining for hard-delete. New scans
    /// are refused until the drain resolves (or is cleared). Maps to
    /// `NodeDbError::collection_draining` (code 1102) at the
    /// Control-Plane boundary.
    CollectionDraining { collection: String },
    /// WITH RECURSIVE CTE exceeded the configured maximum recursion depth.
    /// The client should either add a stricter termination condition or
    /// increase `max_recursion_depth` in the server configuration.
    RecursionDepthExceeded { cte_name: String, max_depth: usize },
    /// Internal error (io_uring failure, corruption, etc.)
    Internal { detail: String },
    /// Operation is not supported on this engine, or not yet implemented for
    /// this op-type. Distinguished from `Internal` so pgwire surfaces it as
    /// `0A000` (feature_not_supported) rather than `XX000`.
    Unsupported { detail: String },
    /// Transaction rollback failed: at least one undo entry could not be
    /// applied. The shard state is unknown — the client must treat this as a
    /// fatal error and the operator must restart the shard (WAL replay restores
    /// correct state on startup). Never silently continues.
    RollbackFailed { entry_index: usize, detail: String },
    /// The active Calvin executor detected that the declared predicate no
    /// longer matches the engine state at execution time (OLLP mismatch).
    /// No write was applied. The OLLP orchestrator retries with a fresh
    /// pre-execution scan.
    ///
    /// Numeric value: `OLLP_RETRY_REQUIRED_CODE` (0xCAAD) — single source of
    /// truth defined in `control/cluster/calvin/executor/ollp/orchestrator.rs`.
    OllpRetryRequired,
}

impl From<crate::Error> for ErrorCode {
    fn from(e: crate::Error) -> Self {
        match e {
            crate::Error::DeadlineExceeded { .. } => Self::DeadlineExceeded,
            crate::Error::RejectedConstraint {
                constraint, detail, ..
            } => Self::RejectedConstraint { constraint, detail },
            crate::Error::RejectedPrevalidation { reason, .. } => {
                Self::RejectedPrevalidation { reason }
            }
            crate::Error::CollectionNotFound { .. } | crate::Error::DocumentNotFound { .. } => {
                Self::NotFound
            }
            crate::Error::RejectedAuthz { .. } => Self::RejectedAuthz,
            crate::Error::ConflictRetry { .. } => Self::ConflictRetry,
            crate::Error::FanOutExceeded { .. } => Self::FanOutExceeded,
            crate::Error::MemoryExhausted { .. } => Self::ResourcesExhausted,
            crate::Error::Backpressure { .. } => Self::ResourcesExhausted,
            crate::Error::AppendOnlyViolation { collection, .. } => {
                Self::AppendOnlyViolation { collection }
            }
            crate::Error::BalanceViolation {
                collection, detail, ..
            } => Self::BalanceViolation { collection, detail },
            crate::Error::PeriodLocked { collection, .. } => Self::PeriodLocked { collection },
            crate::Error::RetentionViolation { collection, .. } => {
                Self::RetentionViolation { collection }
            }
            crate::Error::LegalHoldActive { collection, .. } => {
                Self::LegalHoldActive { collection }
            }
            crate::Error::StateTransitionViolation {
                collection, detail, ..
            } => Self::StateTransitionViolation { collection, detail },
            crate::Error::TransitionCheckViolation { collection, .. } => {
                Self::TransitionCheckViolation { collection }
            }
            crate::Error::TypeGuardViolation {
                collection, detail, ..
            } => Self::TypeGuardViolation { collection, detail },
            crate::Error::TypeMismatch {
                collection, detail, ..
            } => Self::TypeMismatch { collection, detail },
            crate::Error::OverflowError { collection, .. } => Self::OverflowError { collection },
            crate::Error::InsufficientBalance {
                collection, detail, ..
            } => Self::InsufficientBalance { collection, detail },
            crate::Error::RateExceeded {
                gate,
                retry_after_ms,
                ..
            } => Self::RateExceeded {
                gate,
                retry_after_ms,
            },
            other => Self::Internal {
                detail: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
    use std::time::Duration;

    fn sample_request() -> Request {
        Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: "users".into(),
                document_id: "doc-1".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::generate(),
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
        }
    }

    #[test]
    fn request_fields_accessible() {
        let req = sample_request();
        assert_eq!(req.request_id, RequestId::new(1));
        assert_eq!(req.tenant_id, TenantId::new(1));
        assert_ne!(req.trace_id, TraceId::ZERO);
    }

    #[test]
    fn response_ok() {
        let resp = Response {
            request_id: RequestId::new(1),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(b"result".to_vec()),
            watermark_lsn: Lsn::new(42),
            error_code: None,
        };
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(resp.watermark_lsn, Lsn::new(42));
        assert_eq!(&*resp.payload, b"result");
    }

    #[test]
    fn response_error() {
        let resp = Response {
            request_id: RequestId::new(2),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: Some(ErrorCode::DeadlineExceeded),
        };
        assert_eq!(resp.error_code, Some(ErrorCode::DeadlineExceeded));
    }

    #[test]
    fn priority_ordering() {
        assert!(Priority::Background < Priority::Normal);
        assert!(Priority::Normal < Priority::High);
        assert!(Priority::High < Priority::Critical);
    }

    #[test]
    fn cancel_plan() {
        let req = Request {
            request_id: RequestId::new(99),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Cancel {
                target_request_id: RequestId::new(42),
            }),
            deadline: Instant::now() + Duration::from_secs(1),
            priority: Priority::Critical,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
        };
        match req.plan {
            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
