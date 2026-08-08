// SPDX-License-Identifier: BUSL-1.1

//! Which encoding a collection's sparse-store rows use, resolved from the
//! collection's registered kind.
//!
//! Three encodings share the sparse store: schemaless document bodies (standard
//! MessagePack), strict document bodies (Binary Tuples), and vector-primary
//! metadata sidecars (`zerompk` TAGGED `HashMap<String, Value>`). A tagged map
//! and a plain document map are both valid MessagePack maps and begin with the
//! same map header, so no inspection of the stored bytes can separate them: a
//! reader that sniffs necessarily mis-decodes one of them, and returns
//! `[4,"alice"]` where the client asked for `alice`.
//!
//! So the decision is made once, here, from `doc_configs` — the registry the
//! DDL register broadcast and the boot seed both populate — and every reader
//! of a sparse body takes the answer as a parameter.

use nodedb_physical::physical_plan::StorageMode;

use super::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

/// How the bytes of a sparse-store row are encoded. See the module docs for
/// why this is never derived from the bytes themselves.
pub(in crate::data::executor) enum SparseBodyFormat {
    /// Schemaless document body: standard msgpack, or legacy JSON that the
    /// normalizer transcodes.
    Document,
    /// Strict document body: a Binary Tuple decoded against this schema.
    Strict(nodedb_types::columnar::StrictSchema),
    /// Vector-primary metadata sidecar: `zerompk` TAGGED
    /// `HashMap<String, Value>`, written verbatim by the vector upsert handler.
    VectorSidecar,
}

impl CoreLoop {
    /// Resolve the sparse-body encoding a collection's rows use.
    ///
    /// Vector-primary is checked first: such a collection also carries a
    /// storage mode (its sidecar rows are not strict tuples), and the sidecar
    /// encoding is the one that actually describes the bytes on disk.
    ///
    /// An unregistered collection resolves to `Document`, which is what the
    /// read path did before any of these markers existed.
    pub(in crate::data::executor) fn sparse_body_format(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
    ) -> SparseBodyFormat {
        let key = (database_id, tenant_id, collection.to_string());
        let Some(config) = self.doc_configs.get(&key) else {
            return SparseBodyFormat::Document;
        };
        if config.vector_primary.is_some() {
            return SparseBodyFormat::VectorSidecar;
        }
        match config.storage_mode {
            StorageMode::Strict { ref schema } => SparseBodyFormat::Strict(schema.clone()),
            StorageMode::Schemaless => SparseBodyFormat::Document,
        }
    }
}
