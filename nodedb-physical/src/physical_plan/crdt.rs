// SPDX-License-Identifier: Apache-2.0

//! CRDT engine operations dispatched to the Data Plane.

use nodedb_types::Surrogate;

/// CRDT engine physical operations.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum CrdtOp {
    /// CRDT state read for a document.
    Read {
        collection: String,
        document_id: String,
    },

    /// CRDT delta application (write path).
    ///
    /// Binds the user-visible `document_id` to a stable cross-engine
    /// `Surrogate`. UPSERT-aware: if the document already has a surrogate,
    /// the assigner returns the existing one. `Surrogate::ZERO` only
    /// appears in test fixtures.
    Apply {
        collection: String,
        document_id: String,
        delta: Vec<u8>,
        peer_id: u64,
        /// Per-mutation unique ID for deduplication and compensation tracking.
        mutation_id: u64,
        /// Stable cross-engine identity for the document this delta targets.
        surrogate: Surrogate,
        /// Sync provenance: identifies the originating peer and sequence for idempotency.
        #[serde(default)]
        provenance: Option<nodedb_types::sync::wire::SyncProvenance>,
        /// Descriptor version of the collection's constraint set that this
        /// delta was admitted against. Stamped at admission (Control Plane,
        /// which can read the catalog) so the apply-time write-gate (U5e) can
        /// reject a delta that reaches a replica before that replica has
        /// installed the matching constraint version. `0` means "no fence"
        /// (gate open): used by replay/native/internal construction sites that
        /// do not admit against a catalog descriptor.
        #[serde(default)]
        descriptor_version_required: u64,
    },

    /// Import a per-collection Loro snapshot into the tenant CRDT engine.
    ///
    /// Used by the durable RESTORE re-issue path: the snapshot is replicated
    /// to every Raft replica of a data group, and each replica calls
    /// `import_snapshot_bytes` (a monotonic, idempotent, commutative Loro
    /// merge — deterministic across replicas). Carries no surrogate or
    /// provenance: it is a collection-doc import, not a per-document op.
    ImportSnapshot {
        tenant_id: u64,
        collection: String,
        bytes: Vec<u8>,
    },

    /// Install a collection's constraint set into every replica's CRDT
    /// validator. Carried opaquely as zerompk-encoded constraint blobs so this
    /// crate stays decoupled from the constraint wire layout (mirroring the
    /// opaque payload precedent of other replicated ops). Decoded into typed
    /// constraints inside the Data Plane handler. Applied deterministically on
    /// every replica from the per-vshard data Raft log.
    /// `descriptor_version` fences the install: a replica applies only when it
    /// is `>=` the version last installed for the collection, so a stale set
    /// cannot clobber a newer one regardless of data-log apply order.
    SetConstraints {
        collection: String,
        descriptor_version: u64,
        constraints: Vec<Vec<u8>>,
    },

    /// Remove every constraint scoped to `collection` from the CRDT validator.
    /// `descriptor_version` fences the drop identically to `SetConstraints`.
    DropConstraints {
        collection: String,
        descriptor_version: u64,
    },

    /// Read the constraint set currently installed in this replica's per-core
    /// CRDT validator for `collection`. Read-only observability op: returns the
    /// installed `Vec<Constraint>` zerompk-encoded in the response payload. It
    /// is never replicated or logged — it is constructed directly against a
    /// single node's data core to inspect that replica's validator state.
    ReadConstraints { collection: String },

    /// Set conflict resolution policy for a CRDT collection (DDL).
    SetPolicy {
        collection: String,
        /// JSON-serialized `CollectionPolicy` from nodedb-crdt.
        policy_json: String,
    },

    /// Read the current conflict resolution policy for a CRDT collection.
    /// Returns the JSON-serialized `CollectionPolicy`, falling back to the
    /// ephemeral default when no explicit policy has been registered.
    GetPolicy { collection: String },

    /// Read a document at a specific historical version.
    /// Returns the document state as JSON bytes.
    ReadAtVersion {
        collection: String,
        document_id: String,
        /// JSON-serialized `HashMap<String, i64>` of {peer_id_hex: counter}.
        version_vector_json: String,
    },

    /// Get the current oplog version vector for a tenant's CRDT state.
    /// Returns version vector as JSON string.
    GetVersionVector { collection: String },

    /// Export oplog delta from a version to current.
    /// Returns raw Loro delta bytes.
    ExportDelta {
        collection: String,
        /// JSON-serialized version vector to start from.
        from_version_json: String,
    },

    /// Restore a document to a historical version (forward mutation).
    /// Returns the delta bytes for the restore operation.
    RestoreToVersion {
        collection: String,
        document_id: String,
        /// JSON-serialized version vector of the target version.
        target_version_json: String,
        /// Stable cross-engine identity for the document being restored.
        surrogate: Surrogate,
    },

    /// Compact history at a specific version.
    CompactAtVersion {
        collection: String,
        /// JSON-serialized version vector. Oplog before this is discarded.
        target_version_json: String,
    },

    // ─── Block Document (LoroList) Operations ───────────────────────
    /// Insert a block (LoroMap) into a document's block list.
    /// `fields_json` contains the block's fields as a JSON object.
    ///
    /// The inserted list element is a sub-document of the existing CRDT
    /// document — it does not allocate a new top-level surrogate. The
    /// `surrogate` field carries the parent document's surrogate.
    ListInsert {
        collection: String,
        document_id: String,
        list_path: String,
        index: usize,
        fields_json: String,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },

    /// Delete a block from a document's block list.
    ListDelete {
        collection: String,
        document_id: String,
        list_path: String,
        index: usize,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },

    /// Move a block within a document's block list (reorder).
    ListMove {
        collection: String,
        document_id: String,
        list_path: String,
        from_index: usize,
        to_index: usize,
        /// Surrogate of the parent document hosting this block list.
        surrogate: Surrogate,
    },
}
