// SPDX-License-Identifier: Apache-2.0

//! Engine identification for memory budget tracking.

/// Identifies a subsystem that owns a memory budget.
///
/// Each engine operates within its allocated memory ceiling. The governor
/// tracks allocations per engine and rejects requests that would exceed
/// the budget.
///
/// Covers all eight peer engines (Document schemaless, Document strict, KV,
/// Columnar, Timeseries, Spatial, Vector, Array), plus the two cross-engine
/// overlays (Graph, Fts), plus infrastructure subsystems (Sparse metadata,
/// Crdt, Query, Wal, Bridge).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EngineId {
    /// HNSW vector index, distance computation buffers, quantized caches.
    Vector,

    /// CSR adjacency index, traversal algorithm working sets (cross-engine overlay).
    Graph,

    /// Document (schemaless): MessagePack blobs, secondary index buffers.
    DocumentSchemaless,

    /// Document (strict): Binary Tuple encode/decode buffers, schema metadata.
    DocumentStrict,

    /// Key-Value engine: hash index buckets, TTL expiry wheel.
    Kv,

    /// Columnar engine: compressed segment build buffers, block statistics.
    Columnar,

    /// Gorilla-encoded memtables, Zstd dictionaries, log buffers.
    Timeseries,

    /// R*-tree node pools, geohash / H3 index structures.
    Spatial,

    /// Array engine (ND sparse): tile decompression buffers, coordinate arrays.
    Array,

    /// FTS LSM memtable, posting lists, compaction merge buffers (cross-engine overlay).
    Fts,

    /// redb B-Tree, splintr inverted index, schema metadata (sparse/metadata engine).
    Sparse,

    /// loro CRDT state, merge buffers, operation logs.
    Crdt,

    /// DataFusion query execution: sorts, aggregates, hash tables.
    Query,

    /// WAL write buffers, group commit staging.
    Wal,

    /// SPSC bridge buffers, slab allocator, envelope staging.
    Bridge,
}

impl EngineId {
    /// All known engine identifiers (for iteration in the governor).
    pub const ALL: &'static [EngineId] = &[
        EngineId::Vector,
        EngineId::Graph,
        EngineId::DocumentSchemaless,
        EngineId::DocumentStrict,
        EngineId::Kv,
        EngineId::Columnar,
        EngineId::Timeseries,
        EngineId::Spatial,
        EngineId::Array,
        EngineId::Fts,
        EngineId::Sparse,
        EngineId::Crdt,
        EngineId::Query,
        EngineId::Wal,
        EngineId::Bridge,
    ];

    /// Number of known engine identifiers.
    pub const COUNT: usize = Self::ALL.len();

    /// Dense array index for this engine, for use with fixed-size
    /// per-engine arrays sized `EngineId::COUNT`.
    ///
    /// `EngineId` is a fieldless enum, so the discriminant cast matches the
    /// declaration position, and `ALL` lists variants in that same order —
    /// the inline test below locks the two together.
    pub const fn index(self) -> usize {
        self as usize
    }
}

impl std::fmt::Display for EngineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineId::Vector => write!(f, "vector"),
            EngineId::Graph => write!(f, "graph"),
            EngineId::DocumentSchemaless => write!(f, "document_schemaless"),
            EngineId::DocumentStrict => write!(f, "document_strict"),
            EngineId::Kv => write!(f, "kv"),
            EngineId::Columnar => write!(f, "columnar"),
            EngineId::Timeseries => write!(f, "timeseries"),
            EngineId::Spatial => write!(f, "spatial"),
            EngineId::Array => write!(f, "array"),
            EngineId::Fts => write!(f, "fts"),
            EngineId::Sparse => write!(f, "sparse"),
            EngineId::Crdt => write!(f, "crdt"),
            EngineId::Query => write!(f, "query"),
            EngineId::Wal => write!(f, "wal"),
            EngineId::Bridge => write!(f, "bridge"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_matches_all_declaration_order() {
        assert_eq!(EngineId::ALL.len(), EngineId::COUNT);
        for id in EngineId::ALL {
            assert_eq!(EngineId::ALL[id.index()], *id);
        }
    }
}
