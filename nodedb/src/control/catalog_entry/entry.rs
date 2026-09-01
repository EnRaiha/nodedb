// SPDX-License-Identifier: BUSL-1.1

//! The `CatalogEntry` enum itself.
//!
//! Each variant is one mutation on the host-side `SystemCatalog` redb
//! and/or an in-memory registry on `SharedState`. The apply, post_apply,
//! and test modules match exhaustively, so adding a variant forces every
//! consumer to handle it.
//!
//! `Put*` variants carry the full updated record, so followers apply
//! verbatim without a diff. Leader-side preparation (hashing, compiling,
//! validating) happens before the proposal; apply writes what consensus
//! accepted. Variants appended at the end of the enum stay there to keep
//! MessagePack discriminants stable across rolling upgrades.

use crate::control::security::catalog::{
    StoredCollection, StoredContinuousAggregate, StoredCustomType, StoredIndexRecord,
    StoredMaterializedView, StoredOidcProvider, StoredRedactionPolicy, StoredRlsPolicy,
    StoredScopeGrant, StoredSynonymGroup,
    auth_types::{
        StoredApiKey, StoredAuthUser, StoredOwner, StoredPermission, StoredRole, StoredScopeQuota,
        StoredTenant, StoredUser,
    },
    function_types::StoredFunction,
    procedure_types::StoredProcedure,
    sequence_types::{SequenceState, StoredSequence},
    trigger_types::StoredTrigger,
};
use crate::event::cdc::stream_def::ChangeStreamDef;
use crate::event::scheduler::types::ScheduleDef;
use crate::types::DatabaseId;

#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub enum CatalogEntry {
    // ── Collection ─────────────────────────────────────────────────
    /// CREATE COLLECTION, and every ALTER COLLECTION path that ships a
    /// full updated record.
    PutCollection(Box<StoredCollection>),
    /// Create-only: applies iff the collection is absent, never clobbering an
    /// existing schema. Used by CRDT sync to materialize announced collections.
    PutCollectionIfAbsent(Box<StoredCollection>),
    /// Soft delete: sets `is_active = false`, keeping the row for audit and
    /// undrop. Step one of DROP → retention-expiry → PURGE.
    DeactivateCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Prior committed version + 1. `0` is the pre-stamping compat sentinel.
        /// Frozen at propose time — an apply-time stamp diverges across replicas.
        descriptor_version: u64,
        /// Drop time, and the instant retention measures from.
        modification_hlc: nodedb_types::Hlc,
    },
    /// Hard delete: removes the `StoredCollection` row, owner row, and cascade
    /// dependents, then dispatches `MetaOp::UnregisterCollection` to every node.
    ///
    /// Reached by `DROP COLLECTION ... PURGE` (superuser / tenant_admin only),
    /// the `CollectionGC` sweeper after
    /// `deactivated_collection_retention_days`, or
    /// `SELECT _system.purge_collection(...)`. After purge the data is
    /// unrecoverable except from backup.
    PurgeCollection {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Sequence ───────────────────────────────────────────────────
    /// CREATE SEQUENCE and ALTER SEQUENCE FORMAT.
    PutSequence(Box<StoredSequence>),
    /// DROP SEQUENCE, and the DROP COLLECTION cascade that removes implicit
    /// `{coll}_{field}_seq` sequences for SERIAL columns.
    DeleteSequence {
        tenant_id: u64,
        name: String,
    },
    /// Runtime state (current value, is_called, epoch, period_key). Used by
    /// ALTER SEQUENCE RESTART to propagate the new counter across nodes.
    PutSequenceState(Box<SequenceState>),

    // ── Trigger ────────────────────────────────────────────────────
    /// CREATE [OR REPLACE] TRIGGER, and ALTER TRIGGER ENABLE/DISABLE.
    PutTrigger(Box<StoredTrigger>),
    DeleteTrigger {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
    },

    // ── Function ───────────────────────────────────────────────────
    /// CREATE [OR REPLACE] FUNCTION. WASM bytes travel in the transient
    /// `StoredFunction::wasm_module` and install before metadata persists.
    PutFunction(Box<StoredFunction>),
    DeleteFunction {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
    },

    // ── Procedure ──────────────────────────────────────────────────
    /// CREATE PROCEDURE. Post-apply clears `block_cache` so the next CALL
    /// re-parses the body.
    PutProcedure(Box<StoredProcedure>),
    DeleteProcedure {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
    },

    // ── Schedule ───────────────────────────────────────────────────
    /// Scheduled-job definition. Post-apply syncs `schedule_registry` so every
    /// node's cron executor picks it up immediately.
    PutSchedule(Box<ScheduleDef>),
    DeleteSchedule {
        database_id: DatabaseId,
        tenant_id: u64,
        name: String,
    },

    // ── Synonym group ──────────────────────────────────────────────
    /// Post-apply syncs the in-memory `synonym_registry`.
    PutSynonymGroup(Box<StoredSynonymGroup>),
    /// Post-apply removes it from `synonym_registry`.
    DeleteSynonymGroup {
        tenant_id: u64,
        name: String,
    },

    // ── Custom type ────────────────────────────────────────────────
    /// Enum or composite type. Post-apply syncs `custom_type_registry`.
    PutCustomType(Box<StoredCustomType>),
    /// Post-apply removes it from `custom_type_registry`.
    DeleteCustomType {
        tenant_id: u64,
        name: String,
    },

    // ── Change stream ──────────────────────────────────────────────
    /// CDC stream definition. Post-apply syncs `stream_registry` so every node
    /// starts buffering matching WriteEvents.
    PutChangeStream(Box<ChangeStreamDef>),
    /// Removes the definition and tears down its buffer via
    /// `cdc_router.remove_buffer`.
    DeleteChangeStream {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── User ───────────────────────────────────────────────────────
    /// The leader builds the full record (Argon2 hash, SCRAM salt, user_id) via
    /// `CredentialStore::prepare_user`; followers bump their `next_user_id`.
    PutUser(Box<StoredUser>),
    /// Removes the identity from every node's cache and redb catalog, freeing
    /// the username for reuse.
    DropUser {
        username: String,
    },

    // ── Role ───────────────────────────────────────────────────────
    /// Custom roles only. Built-in roles are hardcoded in `identity.rs`.
    PutRole(Box<StoredRole>),
    /// Does not cascade to grants that reference the role.
    DeleteRole {
        name: String,
    },

    // ── ApiKey ─────────────────────────────────────────────────────
    /// The leader builds the record (SHA-256 secret_hash) via
    /// `ApiKeyStore::prepare_key`. The plaintext secret NEVER enters raft.
    PutApiKey(Box<StoredApiKey>),
    /// Sets `is_revoked = true` and rewrites the row, preserving it for audit.
    RevokeApiKey {
        key_id: String,
    },

    // ── Auth user ──────────────────────────────────────────────────
    /// Externally-authenticated (`_system.auth_users`) record, proposed by
    /// auto-escalation on a `Suspended` / `Banned` verdict.
    ///
    /// The proposer has already written and installed the record: an
    /// enforcement decision must hold even if replication is unavailable.
    /// Apply is an idempotent upsert on every node, proposer included.
    PutAuthUser(Box<StoredAuthUser>),

    // ── Materialized View ──────────────────────────────────────────
    /// The Data Plane refresh loop picks up the definition on its next tick.
    PutMaterializedView(Box<StoredMaterializedView>),
    /// Removes the definition and its implementation-owned target collection as
    /// one mutation. Post-apply waits for Data Plane reclaim before advancing
    /// the applied index, so a same-name re-CREATE starts fresh.
    DeleteMaterializedView {
        tenant_id: u64,
        name: String,
    },
    // ── Continuous Aggregate ───────────────────────────────────────
    /// Writes the catalog row plus the owner row. Post-apply re-dispatches
    /// `MetaOp::RegisterContinuousAggregate` to the local Data Plane.
    PutContinuousAggregate(Box<StoredContinuousAggregate>),
    /// The target collection holding materialized rows is NOT deleted —
    /// operators drop it separately, mirroring the materialized-view contract.
    DeleteContinuousAggregate {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Tenant ─────────────────────────────────────────────────────
    /// Quotas are not part of `StoredTenant` and replicate separately.
    /// Post-apply seeds default quota so reads work right after creation.
    PutTenant(Box<StoredTenant>),
    /// Atomically create a tenant and its authoritative administrator.
    PutTenantWithAdmin {
        tenant: Box<StoredTenant>,
        admin: Box<StoredUser>,
    },
    /// Hard-deletes the identity record only. Tenant data is purged separately
    /// by the `PURGE TENANT CONFIRM` Data Plane meta op.
    DeleteTenant {
        tenant_id: u64,
    },

    // ── RLS policy ─────────────────────────────────────────────────
    /// The leader serializes the runtime `RlsPolicy` into `StoredRlsPolicy`;
    /// followers re-hydrate via `to_runtime()` in post_apply.
    PutRlsPolicy(Box<StoredRlsPolicy>),
    /// Keyed by `(tenant_id, collection, name)`.
    DeleteRlsPolicy {
        tenant_id: u64,
        collection: String,
        name: String,
    },

    // ── Redaction policy ──────────────────────────────────────────
    /// The leader flattens the runtime `RedactionPolicy` rule list into
    /// `StoredRedactionPolicy`; followers re-hydrate via `to_runtime()`.
    PutRedactionPolicy(Box<StoredRedactionPolicy>),
    /// Keyed by `(tenant_id, collection, for_role)`.
    DeleteRedactionPolicy {
        tenant_id: u64,
        collection: String,
        for_role: String,
    },

    // ── Permission grant ───────────────────────────────────────────
    /// `GRANT <perm> ON <target> TO <grantee>`. The catalog row is
    /// authoritative; `PermissionStore.grants` is rebuilt from it on apply.
    PutPermission(Box<StoredPermission>),
    /// Keyed by `(target, grantee, permission)`. `permission` is the lowercase
    /// canonical name (`read|write|create|drop|alter|admin|monitor|execute`).
    DeletePermission {
        target: String,
        grantee: String,
        permission: String,
    },

    // ── Database lifecycle ─────────────────────────────────────────
    /// `CREATE DATABASE`, `ALTER DATABASE RENAME`, `SET QUOTA`, `MATERIALIZE`,
    /// `PROMOTE`.
    PutDatabase(Box<crate::control::security::catalog::database_types::DatabaseDescriptor>),
    /// `DROP DATABASE`: removes the descriptor and its `_system.databases_by_name`
    /// row. Does not touch collection rows — cascade those before proposing.
    DeleteDatabase {
        /// Numeric database id.
        db_id: u64,
    },
    /// Database-level grant in `_system.database_grants`, keyed by
    /// `(db_id, user_id, privilege)`.
    PutDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },
    DeleteDatabaseGrant {
        db_id: u64,
        user_id: u64,
        privilege: String,
    },

    // ── Index registry ─────────────────────────────────────────────
    /// Written by every `CREATE [<kind>] INDEX` path, whatever engine backs it,
    /// so the index is listable and droppable by name on every node.
    PutIndexRecord(Box<StoredIndexRecord>),
    /// Keyed by `(database_id, tenant_id, name)`. The DROP handler performs the
    /// kind-specific teardown before proposing this.
    DeleteIndexRecord {
        database_id: u64,
        tenant_id: u64,
        name: String,
        /// Not needed to locate the record. It lets post-apply invalidate exactly
        /// the cached plans still holding an `IndexLookup` on the dropped index.
        collection: String,
    },

    // ── Object ownership ───────────────────────────────────────────
    /// Orphan path only: objects with no replicated parent variant (indexes,
    /// spatial indexes, `ALTER OBJECT OWNER`). A parent `Stored*` carrying an
    /// `owner` field replicates ownership through its own post_apply instead.
    PutOwner(Box<StoredOwner>),
    /// Keyed by database-scoped object identity.
    DeleteOwner {
        object_type: String,
        database_id: u64,
        tenant_id: u64,
        object_name: String,
    },

    // ── Move Tenant lifecycle ──────────────────────────────────────
    /// The single proposal that makes the `MOVE TENANT` cutover atomic: writes
    /// each collection to `target_db_id`, then deletes it from `source_db_id`.
    ///
    /// Built after snapshot succeeds, and self-contained so any follower
    /// replays it without external lookups.
    MoveTenantCutover {
        tenant_id: u64,
        source_db_id: u64,
        target_db_id: u64,
        /// Collections serialized at their source state. Each is re-keyed to
        /// `target_db_id` on apply.
        collections: Vec<StoredCollection>,
    },

    // ── OIDC provider lifecycle ────────────────────────────────────
    /// `CREATE / ALTER OIDC PROVIDER`. Post-apply refreshes
    /// `oidc_provider_cache`.
    PutOidcProvider(Box<StoredOidcProvider>),
    DeleteOidcProvider {
        name: String,
    },

    // ── WAL replay tombstone ───────────────────────────────────────
    /// Records (or raises) a per-(database, tenant, collection) replay barrier.
    /// Replicated on RESTORE so `purge_lsn` matches everywhere — otherwise
    /// purged writes resurrect on follower restart. Idempotent and monotone.
    RecordWalTombstone {
        database_id: u64,
        tenant_id: u64,
        collection: String,
        purge_lsn: u64,
    },

    // ── Clone lifecycle ────────────────────────────────────────────
    /// Records a new CoW clone database as one unit: writes the target
    /// descriptor (`status = Cloning`, `parent_clone` set) into
    /// `_system.databases`, and adds `target_db_id` to the `clone_lineage`
    /// children of `source_db_id`.
    ///
    /// Built after `as_of_lsn` resolves and `target_db_id` is allocated, so any
    /// follower replays it without external lookups.
    CloneDatabase {
        /// The descriptor for the newly created target database.
        target_descriptor:
            Box<crate::control::security::catalog::database_types::DatabaseDescriptor>,
        /// Numeric id of the source database (for lineage update).
        source_db_id: u64,
    },

    /// Streaming MV definition plus its database-scoped owner row.
    PutStreamingMaterializedView(Box<crate::event::streaming_mv::StreamingMvDef>),
    /// Streaming MVs are Event-Plane objects, so this removes both the
    /// database-scoped catalog record and the in-memory registry entry.
    DeleteStreamingMaterializedView {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },

    // ── Scope grant ────────────────────────────────────────────────
    /// `GRANT SCOPE`, and `RENEW SCOPE` as the same upsert with a later
    /// `expires_at`. The catalog row is authoritative; `ScopeGrantStore` is
    /// installed from it on apply, so the grant authorizes identically
    /// everywhere.
    PutScopeGrant(Box<StoredScopeGrant>),
    /// Keyed by `(scope_name, grantee_type, grantee_id)`. `grantee_type` is the
    /// lowercase form (`user|role|org|team`).
    DeleteScopeGrant {
        scope_name: String,
        grantee_type: String,
        grantee_id: String,
    },

    // ── Resource quota ─────────────────────────────────────────────
    /// Database quota row in `_system.database_quotas`. The leader checks the
    /// global ceiling before proposing; post-apply pushes it into enforcement.
    PutDatabaseQuota {
        db_id: u64,
        record: Box<nodedb_types::QuotaRecord>,
    },
    /// Enforcement falls back to `QuotaRecord::DEFAULT` on every node.
    DeleteDatabaseQuota {
        db_id: u64,
    },
    /// Tenant quota row in `_system.tenant_quotas`, keyed by
    /// `(db_id, tenant_id)`.
    PutTenantQuota {
        db_id: u64,
        tenant_id: u64,
        record: Box<nodedb_types::QuotaRecord>,
    },
    /// Keyed by `(db_id, tenant_id)`.
    DeleteTenantQuota {
        db_id: u64,
        tenant_id: u64,
    },

    // ── Scope token quota ──────────────────────────────────────────
    /// Per-scope token quota row in `_system.scope_quotas`. The leader parses
    /// and range-checks it; post-apply installs it in every `QuotaManager`.
    PutScopeQuota(Box<StoredScopeQuota>),
    /// Drops the row and the in-memory definition on every node.
    DeleteScopeQuota {
        scope_name: String,
    },
}
