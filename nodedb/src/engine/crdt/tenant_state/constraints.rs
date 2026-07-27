// SPDX-License-Identifier: BUSL-1.1

//! Constraint-set installation, version fencing, and bitemporal registration.

use super::core::TenantCrdtEngine;

impl TenantCrdtEngine {
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

    /// The constraint-set version this replica has installed for `collection`
    /// (via `SetConstraints`/`DropConstraints` on the per-vshard data Raft
    /// log). `0` means no constraints are installed. The apply-time write-gate
    /// compares a delta's admitted `constraint_version_required` against this
    /// to fence a delta that outran its constraint install.
    pub fn installed_constraint_version(&self, collection: &str) -> u64 {
        self.constraint_versions
            .get(collection)
            .copied()
            .unwrap_or(0)
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
    /// validator. Fenced identically to [`TenantCrdtEngine::set_collection_constraints`]:
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

    /// Names of collections that currently have an installed constraint set
    /// (constraint_version > 0). Used by the snapshot builder to capture
    /// constraint state so a snapshot-installed follower reconstructs its
    /// validator instead of coming up empty.
    pub fn collections_with_constraints(&self) -> Vec<String> {
        self.constraint_versions
            .iter()
            .filter(|&(_, &v)| v > 0)
            .map(|(c, _)| c.clone())
            .collect()
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

    /// Require signed, replay-protected peer deltas for this tenant's
    /// collection. The caller must also install a tenant signing verifier.
    pub fn require_delta_signing(&mut self, collection: impl Into<String>) {
        self.validator.require_delta_signing(collection);
    }

    /// Install the tenant's registered user/device signing keys.
    pub fn set_delta_verifier(&mut self, verifier: nodedb_crdt::DeltaSigner) {
        self.validator.set_delta_verifier(verifier);
    }
}
