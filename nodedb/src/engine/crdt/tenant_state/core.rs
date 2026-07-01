// SPDX-License-Identifier: BUSL-1.1

//! TenantCrdtEngine core: construction, per-collection state access, delta
//! apply, DLQ, row purge.
//!
//! Each `(tenant, collection)` owns its own `LoroDoc` (one [`CrdtState`] per
//! collection). The validator, dead-letter queue and the cross-engine array
//! surrogate registry stay tenant-wide because UNIQUE / FK constraints are
//! cross-collection (and FK referents may be array-engine rows).

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;

use loro::LoroValue;

use nodedb_crdt::constraint::ConstraintSet;
use nodedb_crdt::pre_validate::{self, PreValidationResult};
use nodedb_crdt::row_lookup::RowLookup;
use nodedb_crdt::state::CrdtState;
use nodedb_crdt::validator::{ProposedChange, Validator};

use crate::types::TenantId;

/// Tenant-wide row/field lookup view passed to the constraint validator.
///
/// Row existence (FK / BiTemporalFK) is satisfied by ANY collection's doc OR
/// by the tenant's array-surrogate registry (cross-engine FK referents).
/// Field-value uniqueness probes are per-collection only — array surrogates are
/// not document rows and never participate in UNIQUE checks.
pub(super) struct TenantRowLookup<'a> {
    pub(super) collections: &'a HashMap<String, CrdtState>,
    pub(super) array_surrogate_ids: &'a HashSet<String>,
}

impl RowLookup for TenantRowLookup<'_> {
    fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.row_exists(collection, row_id))
            || self.array_surrogate_ids.contains(row_id)
    }

    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.field_value_exists(collection, field, value, exclude_row_id))
    }

    fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|s| s.field_value_exists_live(collection, field, value, exclude_row_id))
    }
}

/// Per-tenant CRDT engine state.
pub struct TenantCrdtEngine {
    pub(super) tenant_id: TenantId,

    /// Peer ID used to construct each per-collection [`CrdtState`] lazily.
    pub(super) peer_id: u64,

    /// Tenant-wide cross-engine FK registry: array-engine surrogate IDs that
    /// count as live referents for `ForeignKey` / `BiTemporalFK` checks.
    pub(super) array_surrogate_ids: HashSet<String>,

    /// Constraint validator with DLQ and policy registry (tenant-wide).
    pub(crate) validator: Validator,

    /// Per-collection committed CRDT state — one `LoroDoc` per collection.
    pub(super) collections: HashMap<String, CrdtState>,

    /// Last constraint-set version installed per collection. Acts as a
    /// monotonic fence on constraint installs: a constraint change is applied
    /// only when its `constraint_version` is `>=` the version last installed
    /// for the collection. This makes proposer-ordering races harmless — a
    /// stale set re-proposed at a higher data-log index can never clobber a
    /// newer one. Collections absent from the map are treated as version `0`.
    pub(super) constraint_versions: HashMap<String, u64>,
}

impl TenantCrdtEngine {
    /// Create a new engine for a tenant with the given peer ID and constraints.
    pub fn new(
        tenant_id: TenantId,
        peer_id: u64,
        constraints: ConstraintSet,
    ) -> crate::Result<Self> {
        Ok(Self {
            tenant_id,
            peer_id,
            array_surrogate_ids: HashSet::new(),
            validator: Validator::new(constraints, 1000),
            collections: HashMap::new(),
            constraint_versions: HashMap::new(),
        })
    }

    /// Get the peer ID for this CRDT engine.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }

    /// Lazily get (creating if absent) the per-collection state. Propagates the
    /// `CrdtState::new` error rather than panicking.
    pub(super) fn state_mut(&mut self, collection: &str) -> crate::Result<&mut CrdtState> {
        match self.collections.entry(collection.to_string()) {
            Entry::Occupied(e) => Ok(e.into_mut()),
            Entry::Vacant(e) => {
                let state = CrdtState::new(self.peer_id).map_err(crate::Error::Crdt)?;
                Ok(e.insert(state))
            }
        }
    }

    /// Names of every collection that currently has local CRDT state.
    pub fn collection_names(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Register an array-engine surrogate ID as a valid cross-engine FK
    /// referent for this tenant.
    pub fn register_array_surrogate(&mut self, id: impl Into<String>) {
        self.array_surrogate_ids.insert(id.into());
    }

    /// Borrow the `LoroDoc` for a collection, creating empty state if absent.
    ///
    /// Used by the block-list (LoroList) operations, which mutate containers
    /// directly through the doc handle.
    pub fn collection_doc(&mut self, collection: &str) -> crate::Result<&loro::LoroDoc> {
        Ok(self.state_mut(collection)?.doc())
    }

    /// Export one collection's CRDT state as binary snapshot bytes.
    ///
    /// Returns `None` when the collection has no local state.
    pub fn export_snapshot_bytes(&self, collection: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) => state
                .export_snapshot()
                .map(Some)
                .map_err(crate::Error::Crdt),
            None => Ok(None),
        }
    }

    /// Export every collection's snapshot as `(collection, bytes)` pairs.
    pub fn export_all_snapshots(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::with_capacity(self.collections.len());
        for (collection, state) in &self.collections {
            let bytes = state.export_snapshot().map_err(crate::Error::Crdt)?;
            out.push((collection.clone(), bytes));
        }
        Ok(out)
    }

    /// Read a document's CRDT state, returning the raw snapshot bytes for the
    /// document's collection. `None` when the collection or row is absent.
    pub fn read_snapshot(&self, collection: &str, row_id: &str) -> crate::Result<Option<Vec<u8>>> {
        match self.collections.get(collection) {
            Some(state) if state.row_exists(collection, row_id) => {
                Ok(Some(state.export_snapshot().map_err(crate::Error::Crdt)?))
            }
            _ => Ok(None),
        }
    }

    /// Read a single row's fields as a `LoroValue`, or `None` if absent.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row(collection, row_id))
    }

    /// Pre-validate a proposed change (fast-reject before Raft).
    pub fn pre_validate(&self, change: &ProposedChange) -> PreValidationResult {
        let view = TenantRowLookup {
            collections: &self.collections,
            array_surrogate_ids: &self.array_surrogate_ids,
        };
        pre_validate::pre_validate(&self.validator, &view, change)
    }

    /// Import a full CRDT snapshot for a single collection (snapshot restore).
    pub fn import_snapshot_bytes(&mut self, collection: &str, bytes: &[u8]) -> crate::Result<()> {
        self.state_mut(collection)?
            .import(bytes)
            .map_err(crate::Error::Crdt)
    }

    /// Apply a validated delta for a collection from Raft commit.
    ///
    /// This is called AFTER Raft consensus — the delta has been committed
    /// to the Raft log and now needs to be applied to the local state.
    pub fn apply_committed_delta(&mut self, collection: &str, delta: &[u8]) -> crate::Result<()> {
        self.state_mut(collection)?
            .import(delta)
            .map_err(crate::Error::Crdt)
    }

    /// Validate and attempt to apply a delta from a peer.
    ///
    /// If constraints are violated, the delta is routed to the DLQ.
    /// Returns `Ok(())` on success, or the constraint violation error.
    ///
    /// For bitemporal collections, `_ts_system` is always stamped with the
    /// receiving node's clock, overwriting any value the sender supplied.
    /// This keeps system-time receiver-authoritative so convergence does
    /// not depend on clock agreement between peers.
    pub fn validate_and_apply(
        &mut self,
        peer_id: u64,
        auth: nodedb_crdt::CrdtAuthContext,
        change: &ProposedChange,
        delta_bytes: Vec<u8>,
    ) -> crate::Result<()> {
        // Tenant-wide view over all collections + array surrogates. The view
        // borrows `self.collections` / `self.array_surrogate_ids` immutably
        // while `self.validator` is borrowed mutably — disjoint fields, so both
        // borrows coexist. The view borrow ends before the upsert below.
        {
            let view = TenantRowLookup {
                collections: &self.collections,
                array_surrogate_ids: &self.array_surrogate_ids,
            };
            self.validator
                .validate_or_reject(&view, peer_id, auth, change, delta_bytes)
                .map_err(crate::Error::Crdt)?;
        }

        let is_bitemporal = self.validator.is_bitemporal(&change.collection);
        // no-determinism: peer delta validation path, not Calvin apply_committed_delta path
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let mut fields: Vec<(&str, LoroValue)> = change
            .fields
            .iter()
            .filter(|(k, _)| !(is_bitemporal && k == "_ts_system"))
            .map(|(k, v)| (k.as_str(), v.clone()))
            .collect();

        let state = self.state_mut(&change.collection)?;
        if is_bitemporal {
            fields.push(("_ts_system", LoroValue::I64(now_ms)));
            state
                .upsert_versioned(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        } else {
            state
                .upsert(&change.collection, &change.row_id, &fields)
                .map_err(crate::Error::Crdt)
        }
    }

    /// Drop archived bitemporal versions older than `cutoff_system_ms`
    /// for the given collection. The live row is never touched. Called
    /// from the Data Plane purge handler. A collection with no local state
    /// purges nothing.
    pub fn purge_history_before(
        &self,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<usize> {
        match self.collections.get(collection) {
            Some(state) => state
                .purge_history_before(collection, cutoff_system_ms)
                .map_err(crate::Error::Crdt),
            None => Ok(0),
        }
    }

    /// Set the conflict-resolution policy for a collection from a typed
    /// `CollectionPolicy`. The JSON-accepting variant in `policy.rs` is the
    /// DDL-facing path; this one is for in-process callers (tests, engine
    /// setup).
    pub fn set_collection_policy_typed(
        &mut self,
        collection: &str,
        policy: nodedb_crdt::policy::CollectionPolicy,
    ) {
        self.validator.policies_mut().set(collection, policy);
    }

    /// Checks whether `constraint_version >= installed` for `collection` and,
    /// if so, advances the stored version to `constraint_version`. Returns
    /// `true` when the caller should proceed with the constraint mutation,
    /// `false` when the incoming version is stale and the call should be
    /// ignored.
    fn advance_constraint_version(&mut self, collection: &str, constraint_version: u64) -> bool {
        let installed = self
            .constraint_versions
            .get(collection)
            .copied()
            .unwrap_or(0);
        if constraint_version >= installed {
            self.constraint_versions
                .insert(collection.to_owned(), constraint_version);
            true
        } else {
            false
        }
    }

    /// Install the constraint set for `collection` into this tenant's
    /// validator, replacing any constraints previously scoped to it. Mutates
    /// only the validator — no per-collection CRDT state is created, since
    /// constraints govern future writes rather than existing rows.
    ///
    /// Fenced by `constraint_version`: the install proceeds only when the
    /// incoming version is `>=` the version last installed for `collection`.
    /// An older version is rejected as stale and the existing constraints are
    /// left untouched. The `>=` (rather than `>`) lets an idempotent
    /// re-delivery of the same version harmlessly re-apply. Returns `true`
    /// when the change was applied, `false` when rejected as stale.
    pub fn set_collection_constraints(
        &mut self,
        collection: &str,
        constraint_version: u64,
        constraints: Vec<nodedb_crdt::Constraint>,
    ) -> bool {
        if !self.advance_constraint_version(collection, constraint_version) {
            return false;
        }
        self.validator
            .set_collection_constraints(collection, constraints);
        true
    }

    /// Remove every constraint scoped to `collection` from this tenant's
    /// validator. Fenced identically to [`set_collection_constraints`]:
    /// applies only when `constraint_version` is `>=` the version last
    /// installed for `collection`. Returns `true` when applied, `false` when
    /// rejected as stale.
    pub fn drop_collection_constraints(
        &mut self,
        collection: &str,
        constraint_version: u64,
    ) -> bool {
        if !self.advance_constraint_version(collection, constraint_version) {
            return false;
        }
        self.validator.clear_collection_constraints(collection);
        true
    }

    /// Clone the constraints currently scoped to `collection` from this
    /// tenant's validator. Empty when the collection has no constraints.
    pub fn constraints_for_collection(&self, collection: &str) -> Vec<nodedb_crdt::Constraint> {
        self.validator
            .constraints_for(collection)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Register a collection as bitemporal on this tenant's validator.
    ///
    /// Bitemporal collections get (a) UNIQUE constraints scoped to live
    /// rows only and (b) receiver-stamped `_ts_system` on apply.
    pub fn mark_bitemporal(&mut self, collection: impl Into<String>) {
        self.validator.mark_bitemporal(collection);
    }

    /// Is the named collection bitemporal?
    pub fn is_bitemporal(&self, collection: &str) -> bool {
        self.validator.is_bitemporal(collection)
    }

    /// Number of entries in the dead-letter queue.
    pub fn dlq_len(&self) -> usize {
        self.validator.dlq().len()
    }

    /// Purge all CRDT state for a single collection.
    ///
    /// Four things happen:
    /// 1. Every row in the collection's Loro doc is cleared.
    /// 2. The collection's conflict-resolution policy is removed from
    ///    the policy registry.
    /// 3. The collection's installed constraints and their version fence
    ///    are cleared — otherwise a re-created collection of the same name
    ///    would be validated against the dropped collection's constraints,
    ///    and (because its descriptor version restarts at 1) its fresh
    ///    constraint install would be rejected as stale by the fence.
    /// 4. Any dead-letter entries (rejected deltas) scoped to this
    ///    collection are dropped — otherwise a re-created collection
    ///    of the same name would inherit unrelated rejected deltas.
    ///
    /// Returns the number of CRDT rows removed. Idempotent.
    pub fn purge_collection(&mut self, collection: &str) -> crate::Result<usize> {
        let removed = match self.collections.get(collection) {
            Some(state) => state
                .clear_collection(collection)
                .map_err(crate::Error::Crdt)?,
            None => 0,
        };
        self.validator.policies_mut().remove(collection);
        self.validator.clear_collection_constraints(collection);
        self.constraint_versions.remove(collection);
        let dlq_dropped = self
            .validator
            .dlq_mut()
            .purge_collection(self.tenant_id.as_u64(), collection);
        if dlq_dropped > 0 {
            tracing::debug!(
                tenant = self.tenant_id.as_u64(),
                collection,
                dlq_dropped,
                "crdt: dropped DLQ entries scoped to purged collection"
            );
        }
        Ok(removed)
    }

    /// Count archived (superseded) bitemporal versions for a row.
    /// Returns `0` when the collection has no local state.
    pub fn archive_version_count(&self, collection: &str, row_id: &str) -> usize {
        self.collections
            .get(collection)
            .map(|state| state.archive_version_count(collection, row_id))
            .unwrap_or(0)
    }

    /// Read the row as it was at `asof_ms` (system-time). Returns `None` when
    /// the collection is absent or no version existed at or before that time.
    pub fn read_row_as_of(
        &self,
        collection: &str,
        row_id: &str,
        asof_ms: i64,
    ) -> Option<LoroValue> {
        self.collections
            .get(collection)
            .and_then(|state| state.read_row_as_of(collection, row_id, asof_ms))
    }

    /// Check if a row exists in a collection's document store.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|state| state.row_exists(collection, row_id))
    }

    pub fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }
}
