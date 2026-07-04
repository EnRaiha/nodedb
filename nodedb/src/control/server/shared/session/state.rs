// SPDX-License-Identifier: BUSL-1.1

//! Per-connection session state types.

use std::collections::HashMap;

use crate::types::{DatabaseId, Lsn, TenantId, TxnId, VShardId};
use nodedb_physical::physical_task::PhysicalTask;

/// PostgreSQL transaction state for ReadyForQuery status byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 'I' — not in a transaction block.
    Idle,
    /// 'T' — in a transaction block (after BEGIN).
    InBlock,
    /// 'E' — in a failed transaction block (error occurred after BEGIN).
    Failed,
}

/// Server-side cursor state.
pub struct CursorState {
    /// Pre-fetched result rows as JSON strings.
    pub rows: Vec<String>,
    /// Current position (next row to return).
    pub position: usize,
    /// Whether this cursor supports backward fetching (SCROLL).
    pub scrollable: bool,
    /// Whether this cursor survives transaction commit (WITH HOLD).
    pub with_hold: bool,
}

/// Per-connection session state.
pub struct ConnSession {
    pub tx_state: TransactionState,
    /// Database bound to this connection session.
    ///
    /// Set at startup from the `database` parameter in the PostgreSQL StartupMessage
    /// (i.e. `psql -d <name>` or `dbname=<name>` in the connection string). If the
    /// client sends no database parameter, falls back to the resolution chain:
    /// user-default → tenant-default → `DatabaseId::DEFAULT` ("default").
    ///
    /// Mutable only via `USE DATABASE <name>`, which issues a full session reset.
    pub current_database: Option<DatabaseId>,
    /// Per-session tenant override applied only to superuser connections.
    ///
    /// `None` means queries route to the identity-bound tenant
    /// (`AuthenticatedIdentity::tenant_id`). When set — only ever by
    /// `SET TENANT = '<name>' | <id> | DEFAULT` / `SET nodedb.tenant_id = <id>`
    /// from a superuser session — `resolve_identity` overlays this value onto
    /// the resolved identity for every subsequent request on the connection.
    /// Cleared by `RESET TENANT`, `SET TENANT = DEFAULT`, or `DISCARD ALL`.
    ///
    /// Non-superuser sessions never carry an override (the SET handler rejects
    /// with `42501` before this field is written), so the identity-bound
    /// invariant continues to hold for tenant-scoped users.
    pub effective_tenant_id: Option<TenantId>,
    /// Session parameters set via SET commands.
    pub parameters: HashMap<String, String>,
    /// Buffered write tasks accumulated between BEGIN and COMMIT.
    /// Dispatched atomically on COMMIT, discarded on ROLLBACK.
    pub tx_buffer: Vec<PhysicalTask>,
    /// Snapshot LSN captured at BEGIN for snapshot isolation.
    /// All reads within the transaction see data as of this LSN.
    /// Concurrent writes after this point are invisible to the transaction.
    pub tx_snapshot_lsn: Option<Lsn>,
    /// Identity of the current session transaction block, minted on `BEGIN`
    /// and cleared on `COMMIT`/`ROLLBACK`. Keys the per-transaction staging
    /// overlay. `None` outside a transaction block.
    pub tx_id: Option<TxnId>,
    /// vShard this transaction's staged writes were homed on, captured on the
    /// first staged/buffered write. Under the single-vshard-per-transaction
    /// assumption, this is where the staging overlay lives — used to target
    /// `MetaOp::DropTxnOverlay` at COMMIT/ROLLBACK. `None` until the first write.
    pub tx_vshard: Option<VShardId>,
    /// Read-set: (collection, document_id, read_lsn) tuples for write
    /// conflict detection. At COMMIT, each entry is checked — if the
    /// document's current LSN > read_lsn, a concurrent write occurred
    /// and the transaction is rejected with SERIALIZATION_FAILURE.
    pub tx_read_set: Vec<(String, String, Lsn)>,
    /// Savepoint stack: each entry is (name, tx_buffer_len_at_savepoint).
    /// On ROLLBACK TO, truncate tx_buffer to the saved length.
    pub savepoints: Vec<(String, usize)>,
    /// Pending consumer offset commits deferred until COMMIT.
    /// Each entry: (tenant_id, stream_name, group_name, partition_id, lsn).
    /// Flushed atomically on COMMIT, discarded on ROLLBACK.
    pub pending_offset_commits: Vec<(u64, String, String, u32, u64)>,
    /// Server-side cursors: name → (cached result rows as JSON strings, current position).
    pub cursors: HashMap<String, CursorState>,
    /// LIVE SELECT subscriptions: active change stream subscriptions for this connection.
    /// Each subscription receives filtered change events from the broadcast channel.
    /// Drained between queries to deliver pgwire NotificationResponse messages.
    pub live_subscriptions: Vec<(String, crate::control::change_stream::Subscription)>,
    /// Active LISTEN subscriptions for this session: (channel, session_id, receiver).
    /// Drained between queries to deliver pgwire NotificationResponse messages.
    pub listen_handles: Vec<crate::control::notify_bus::ListenHandle>,
    /// NOTIFY messages buffered inside an open transaction (COMMIT fires them).
    /// Each entry is (channel, payload).
    pub pending_notifies: Vec<(String, String)>,
    /// Pending pgwire NOTICE messages queued during query execution.
    /// Drained between query and response delivery so the client receives a
    /// `NoticeResponse` for warnings raised by the response shaper (e.g. an
    /// array slice request whose `system_as_of` fell below the oldest tile
    /// version). Populated by `payload_to_response` when the decoded
    /// `ArraySliceResponse` carries `truncated_before_horizon = true`.
    pub pending_notices: Vec<String>,
    /// SQL-level prepared statements: PREPARE name(types) AS query.
    /// Separate from pgwire wire-level prepared statements (managed by pgwire crate).
    pub prepared_stmts: super::prepared_cache::PreparedStatementCache,
    /// Temporary tables: per-session, auto-dropped on disconnect.
    pub temp_tables: super::temp_tables::TempTableRegistry,
    /// Per-session plan cache for prepared statement execution.
    /// Keyed by (sql_hash, schema_version) — auto-invalidates on DDL.
    pub plan_cache: crate::control::server::shared::session::plan_cache::PlanCache,
    /// GAP_FREE sequence reservations pending commit/rollback.
    /// On COMMIT: each reservation is finalized. On ROLLBACK: counter decremented.
    pub pending_sequence_reservations: Vec<crate::control::sequence::gap_free::ReservationHandle>,
}

impl ConnSession {
    pub(super) fn new() -> Self {
        let mut parameters = HashMap::new();
        // Default session parameters (PostgreSQL compatibility).
        parameters.insert("client_encoding".into(), "UTF8".into());
        parameters.insert("server_encoding".into(), "UTF8".into());
        parameters.insert("DateStyle".into(), "ISO, MDY".into());
        parameters.insert("TimeZone".into(), "UTC".into());
        parameters.insert("standard_conforming_strings".into(), "on".into());
        parameters.insert("integer_datetimes".into(), "on".into());
        parameters.insert("search_path".into(), "public".into());
        parameters.insert("transaction_isolation".into(), "read committed".into());
        // Version info (PostgreSQL compatibility — tools like psql check this).
        parameters.insert(
            "server_version".into(),
            format!("NodeDB {}", crate::version::VERSION),
        );
        parameters.insert(
            "server_version_num".into(),
            nodedb_types::pg_compat::PG_COMPAT_VERSION_NUM.into(),
        );
        // NodeDB-specific defaults.
        parameters.insert("nodedb.consistency".into(), "strong".into());
        parameters.insert("rounding_mode".into(), "HALF_EVEN".into());
        Self {
            tx_state: TransactionState::Idle,
            current_database: None,
            effective_tenant_id: None,
            parameters,
            tx_buffer: Vec::new(),
            tx_snapshot_lsn: None,
            tx_id: None,
            tx_vshard: None,
            tx_read_set: Vec::new(),
            savepoints: Vec::new(),
            pending_offset_commits: Vec::new(),
            cursors: HashMap::new(),
            live_subscriptions: Vec::new(),
            listen_handles: Vec::new(),
            pending_notifies: Vec::new(),
            pending_notices: Vec::new(),
            prepared_stmts: super::prepared_cache::PreparedStatementCache::new(256),
            temp_tables: super::temp_tables::TempTableRegistry::new(),
            plan_cache: crate::control::server::shared::session::plan_cache::PlanCache::new(128),
            pending_sequence_reservations: Vec::new(),
        }
    }
}
