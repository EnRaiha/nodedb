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

    /// WAL LSN the Control Plane allocated for this write at wal-dispatch time.
    /// The committed write-LSN is part of the cross-plane write contract: the
    /// Data Plane copies it onto the [`ExecutionTask`] so the apply chokepoint
    /// records the per-key / per-collection write version (see
    /// `data::executor::core_loop::write_index`). `None` for reads, control
    /// ops, and writes whose LSN is not (yet) threaded — the version index is
    /// skipped rather than advanced with a wrong value.
    ///
    /// [`ExecutionTask`]: crate::data::executor::task::ExecutionTask
    pub wal_lsn: Option<Lsn>,

    /// Wall-clock instant (ms since epoch) the Control Plane resolved at
    /// WAL-append time for a TTL-bearing KV write. The durable WAL record and
    /// the live Data-Plane apply MUST use this same instant for `expire_at_ms`
    /// — resolving it independently at apply time would let live state
    /// disagree with the durable record by the dispatch latency, and on a
    /// crash-then-replay, replay would recompute `now_ms` at restart time
    /// instead of installing the original instant, pushing the TTL forward by
    /// the crash-to-restart delay. `None` for reads, non-TTL writes, and
    /// writes whose resolved instant is not (yet) threaded — the live apply
    /// falls back to `epoch_system_ms` (Calvin) or the wall clock, same as
    /// before this field existed.
    pub resolved_now_ms: Option<u64>,

    /// Write-admission decision for this request.
    ///
    /// Every write-class [`PhysicalPlan`] MUST pass the neutral write-admission
    /// gate (`crate::control::server::shared::write_admission`) before it is
    /// enqueued to a Data-Plane core; the gate stamps [`Admission::Admitted`].
    /// Requests that do not re-enter the gate carry [`Admission::Exempt`] with
    /// an [`ExemptReason`] — [`ExemptReason::Read`] for reads / savepoint /
    /// overlay meta ops, [`ExemptReason::AlreadyOrdered`] for writes already
    /// serialized elsewhere (Calvin-scheduled applies, Raft-follower / replay /
    /// clone / checkpoint).
    ///
    /// The field is REQUIRED (no `Default`, no `#[serde(default)]`) so every
    /// `Request` construction site makes an explicit choice — that is the
    /// write-ingress completeness enforcement. The SPSC enqueue chokepoint
    /// (`crate::bridge::dispatch`) asserts no write-class plan reaches a core
    /// with the decision unmade.
    pub admission: Admission,
}

/// Write-admission marker carried by every [`Request`].
///
/// A write-class plan becomes [`Admission::Admitted`] only by passing the
/// neutral write-admission gate. Everything that does not re-enter the gate's
/// OCC fence carries [`Admission::Exempt`] with an explicit [`ExemptReason`].
/// There is intentionally no "unresolved" variant: the required field makes an
/// unmade decision unrepresentable, so a missed write path is a compile error
/// at the construction site rather than a silent serializability hole at
/// runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Passed the write-admission gate.
    Admitted,
    /// Does not re-enter the write-admission gate — either a non-write, or a
    /// write whose ordering was already decided elsewhere. The [`ExemptReason`]
    /// records which, so the SPSC chokepoint can tell a legitimately exempt
    /// write apart from a base-state write that bypassed the gate.
    Exempt(ExemptReason),
}

/// Why a [`Request`] is exempt from the write-admission gate.
///
/// The distinction is load-bearing at the SPSC chokepoint: a write-class plan
/// marked [`ExemptReason::Read`] is a bug (a write that bypassed the gate),
/// whereas [`ExemptReason::AlreadyOrdered`] is a legitimately exempt write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExemptReason {
    /// The plan is not a base-state write — a read / query, or an overlay /
    /// savepoint meta-op. It never needs the write fence.
    Read,
    /// A write-class plan whose ordering was ALREADY decided elsewhere and does
    /// NOT re-enter the OCC fence: Calvin-scheduler applies (the scheduler
    /// already holds the locks), replicated / Raft-follower applies, recovery /
    /// replay, clone / copy-up materialization, and checkpoint. These are
    /// legitimately exempt writes.
    AlreadyOrdered,
}

impl Admission {
    /// Whether this marker is [`Admission::Exempt`] with reason
    /// [`ExemptReason::Read`] — i.e. claims the plan is not a base-state write.
    ///
    /// The SPSC chokepoint uses this to catch a write-class plan wrongly marked
    /// exempt-as-read: such a plan bypassed the write-admission gate.
    pub fn is_exempt_as_read(&self) -> bool {
        matches!(self, Admission::Exempt(ExemptReason::Read))
    }
}

/// One row-level effect of an applied write, carried back from the Data Plane
/// so the Control Plane can mint a durable redo record *after* apply.
///
/// Populated only by write handlers whose autocommit path mints no WAL redo of
/// its own but whose effect must still survive a WAL-only restart — today, a
/// `PointUpdate` on a document collection carrying a secondary vector (HNSW)
/// index (see `data::executor::handlers::point::update`). `value` is the
/// post-image body for a put; empty and ignored when `is_delete`.
#[derive(Debug, Clone)]
pub struct WriteSetEntry {
    /// The row's stable global surrogate.
    pub surrogate: u32,
    /// `true` for a delete effect (no body), `false` for a put (post-image in
    /// `value`).
    pub is_delete: bool,
    /// Post-image body for a put; empty for a delete.
    pub value: Vec<u8>,
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

    /// Per-collection read-version LSN (the scanned collection's `coll_write_lsn`
    /// at read time, in its Raft-group index space) — the sound comparand for
    /// cross-shard OCC read validation. Distinct from `watermark_lsn`
    /// (core-global max, used for snapshot/SI reporting). `Lsn::ZERO` for
    /// non-read responses.
    pub read_version_lsn: Lsn,

    /// Error code if status is not Ok.
    pub error_code: Option<Box<ErrorCode>>,

    /// Whether this response's originating transaction found its slice of the
    /// versioned read-set still current against the local write versions.
    /// `Some(true)` = still current (or no reads observed for this slice);
    /// `Some(false)` = at least one read was superseded; `None` = the response
    /// did not carry a read-set check (reads, control ops, and every
    /// non-transaction response).
    ///
    /// For the direct-apply (dependent/active, fast-path) path this is reporting
    /// only — the apply commits regardless. For a staged static Calvin
    /// transaction it is the LOCAL COMMIT VOTE: the scheduler flushes the staged
    /// buffer to base on `Some(true)` and drops it on `Some(false)`.
    pub read_set_valid: Option<bool>,

    /// Row-level effects the Control Plane must turn into durable redo records
    /// *after* the Data Plane applied them. Empty for every response that owns
    /// its durability on the pre-dispatch WAL path (the common case); non-empty
    /// only for post-apply-redo writes (see [`WriteSetEntry`]).
    pub write_set: Vec<WriteSetEntry>,
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
    /// The per-transaction staging overlay exceeded its per-core byte budget.
    /// Surfaces as `program_limit_exceeded` (54000) so clients know the
    /// transaction is too large to stage rather than that it hit an internal
    /// fault.
    TxnOverlayMemoryExceeded { limit: usize },
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
            crate::Error::TxnOverlayMemoryExceeded { limit } => {
                Self::TxnOverlayMemoryExceeded { limit }
            }
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
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
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
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
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
            error_code: Some(Box::new(ErrorCode::DeadlineExceeded)),
            read_set_valid: None,
            read_version_lsn: crate::types::Lsn::ZERO,
            write_set: Vec::new(),
        };
        assert_eq!(
            resp.error_code.as_deref(),
            Some(&ErrorCode::DeadlineExceeded)
        );
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
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        match req.plan {
            PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
                assert_eq!(target_request_id, RequestId::new(42));
            }
            _ => panic!("expected Cancel plan"),
        }
    }
}
