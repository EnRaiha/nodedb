//! StatementExecutor struct, construction, and cross-shard/mutation state.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::state::SharedState;
use crate::control::system_txn::OpenSystemTxn;
use crate::types::{DatabaseId, TenantId};
use crate::wal::CrossShardAppliedKey;

use super::super::transaction::ProcedureTransactionCtx;
use super::body::AtomicBody;

/// Maximum trigger cascade depth (trigger A fires trigger B fires trigger A).
pub const MAX_CASCADE_DEPTH: u32 = 16;

/// Source-write context propagated into a trigger body's executor so that DML
/// targeting a remote-homed collection is sent to the owning node through the
/// cross-shard event subsystem, after the body's local commit succeeds.
///
/// Populated ONLY by the Event-Plane AFTER-trigger fire path (from the source
/// `WriteEvent`). Stored procedures and normal client SQL leave it `None`; its
/// presence is the gate that enables cross-shard routing in `execute_sql`.
///
/// The receiver deduplicates on `(source_vshard, source_lsn, source_sequence)`
/// plus the emitting body and target vShard, so a resend of one body's writes
/// applies once.
#[derive(Debug, Clone)]
pub struct CrossShardOrigin {
    /// The source event's replicated identity: its position's index, which
    /// every replica shares (`event::trigger::lane::action_identity`).
    pub source_lsn: u64,
    /// The source event's position sequence within its write.
    pub source_sequence: u64,
    /// vShard that owns the source collection.
    pub source_vshard: u32,
    /// Collection whose write fired the trigger.
    pub source_collection: String,
}

/// Statement executor: steps through procedural SQL blocks with DML.
pub struct StatementExecutor<'a> {
    pub(super) state: &'a SharedState,
    pub(super) identity: AuthenticatedIdentity,
    pub(super) tenant_id: TenantId,
    /// Database scope fixed for this executor's lifetime.
    pub(super) database_id: DatabaseId,
    pub(super) cascade_depth: u32,
    pub(super) event_source: crate::event::EventSource,
    /// Arc<Mutex> required (not RefCell) because execute_statement returns `+ Send` futures.
    pub(super) new_mutations: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    /// The open transaction every statement stages into, begun by the first
    /// statement after the previous COMMIT or ROLLBACK.
    pub(super) txn: tokio::sync::Mutex<Option<OpenSystemTxn<'a>>>,
    /// Effects held until the open transaction commits.
    pub(super) tx_ctx: Arc<Mutex<ProcedureTransactionCtx>>,
    /// `Some` for a server-run body, which refuses COMMIT and ROLLBACK and
    /// commits once at its end. `None` for a stored procedure.
    pub(super) body: Option<AtomicBody>,
    pub(super) out_values: Arc<Mutex<HashMap<String, nodedb_types::Value>>>,
    /// Cross-shard origin context; `Some` only in the Event-Plane trigger fire
    /// path. Gates remote-write dispatch in `execute_sql`.
    pub(super) cross_shard_origin: Option<CrossShardOrigin>,
    /// The cross-shard request this body applies, with the vShard it
    /// addresses. Its commit records the key in the same redo record as that
    /// vShard's writes.
    pub(super) applied_key: Option<(CrossShardAppliedKey, u32)>,
    /// The triggering statement's transaction, for a BEFORE, INSTEAD OF or
    /// SYNC AFTER body. The body's writes stage into it and commit with the
    /// statement.
    pub(super) joined: Option<&'a DmlTxnCtx<'a>>,
}

/// Control flow signal from statement execution.
pub(in crate::control::planner::procedural::executor) enum Flow {
    Continue,
    Break,
    LoopContinue,
}

impl<'a> StatementExecutor<'a> {
    pub fn new(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            crate::event::EventSource::User,
        )
    }

    pub fn with_source(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        let database_id = identity.default_database.unwrap_or(DatabaseId::DEFAULT);
        Self::with_source_in_database(
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
        )
    }

    /// Construct an executor in an explicit database scope when the caller
    /// carries definition or event database context independent of identity.
    pub fn with_source_in_database(
        state: &'a SharedState,
        identity: AuthenticatedIdentity,
        tenant_id: TenantId,
        database_id: DatabaseId,
        cascade_depth: u32,
        event_source: crate::event::EventSource,
    ) -> Self {
        Self {
            state,
            identity,
            tenant_id,
            database_id,
            cascade_depth,
            event_source,
            new_mutations: Arc::new(Mutex::new(HashMap::new())),
            txn: tokio::sync::Mutex::new(None),
            tx_ctx: Arc::new(Mutex::new(ProcedureTransactionCtx::new())),
            body: None,
            out_values: Arc::new(Mutex::new(HashMap::new())),
            cross_shard_origin: None,
            applied_key: None,
            joined: None,
        }
    }

    /// Run as a server-run body: one transaction, committed at the end of
    /// the block, with COMMIT and ROLLBACK refused.
    pub fn with_atomic_body(mut self, body: AtomicBody) -> Self {
        self.body = Some(body);
        self
    }

    /// Attach cross-shard origin context (Event-Plane AFTER-trigger fire path).
    ///
    /// When set, `execute_sql` route-resolves every statement: one led by a
    /// remote node is held and sent there once the body's local commit
    /// succeeds.
    pub fn with_cross_shard_origin(mut self, origin: CrossShardOrigin) -> Self {
        self.cross_shard_origin = Some(origin);
        self
    }

    /// Apply a cross-shard request addressed to `target_vshard`: the commit
    /// writes `key` into that vShard's redo record, so the key is recorded
    /// exactly when the writes are.
    pub fn with_applied_key(mut self, key: CrossShardAppliedKey, target_vshard: u32) -> Self {
        self.applied_key = Some((key, target_vshard));
        self
    }

    /// Join the triggering statement's transaction `ctx`: the body's writes
    /// stage into it, and the statement's COMMIT commits them.
    pub fn joined_into(mut self, ctx: &'a DmlTxnCtx<'a>) -> Self {
        self.joined = Some(ctx);
        self
    }

    pub fn take_new_mutations(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.new_mutations.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn take_out_values(&self) -> HashMap<String, nodedb_types::Value> {
        let mut guard = self.out_values.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *guard)
    }

    pub fn cascade_depth(&self) -> u32 {
        self.cascade_depth
    }
}
