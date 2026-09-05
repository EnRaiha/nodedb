// SPDX-License-Identifier: BUSL-1.1

use nodedb_mem::{EngineId, EngineLimits};
use serde::{Deserialize, Serialize};

/// Per-engine memory budget allocation as fractions of the global ceiling.
///
/// Every `nodedb_mem::EngineId` — the eight peer engines, the Graph and
/// Full-Text-Search overlays, the sparse/metadata store, the CRDT engine,
/// query execution, the WAL, and the SPSC bridge — gets an explicit
/// fraction. The memory governor maps these to per-engine byte budgets at
/// startup; a zero limit reports *Emergency* pressure and rejects the
/// engine's very first write with `resources exhausted`, so every fraction
/// must be strictly positive.
///
/// Fractions must each be a positive, finite number and together sum to
/// `<= 1.0`. Any remainder is unallocated headroom for transient
/// allocations (sort buffers, network buffers, etc.). All fields default
/// via `Default`, so a `[engines]` table that omits any key is still valid.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineConfig {
    /// Vector engine: HNSW graphs, distance buffers, quantized caches.
    pub vector_budget_fraction: f64,
    /// Graph overlay: CSR adjacency index, traversal working sets.
    pub graph_budget_fraction: f64,
    /// Document (schemaless): MessagePack blobs, secondary-index buffers.
    pub document_schemaless_budget_fraction: f64,
    /// Document (strict): Binary Tuple encode/decode buffers, schema metadata.
    pub document_strict_budget_fraction: f64,
    /// Key-Value engine: hash index buckets, TTL expiry wheel.
    pub kv_budget_fraction: f64,
    /// Columnar engine: compressed segment build buffers, block statistics.
    pub columnar_budget_fraction: f64,
    /// Timeseries engine: Gorilla memtables, Zstd dictionaries, log buffers.
    pub timeseries_budget_fraction: f64,
    /// Spatial engine: R*-tree node pools, geohash / H3 index structures.
    pub spatial_budget_fraction: f64,
    /// Array engine (ND sparse): tile decompression buffers, coordinate arrays.
    pub array_budget_fraction: f64,
    /// Full-Text Search overlay: LSM memtable, posting lists, compaction buffers.
    pub fts_budget_fraction: f64,
    /// Sparse / metadata engine: redb B-Tree, inverted index, schema metadata.
    pub sparse_budget_fraction: f64,
    /// CRDT engine: loro state, merge buffers, operation logs, DLQ.
    pub crdt_budget_fraction: f64,
    /// Query execution: DataFusion sorts, aggregates, hash tables.
    pub query_budget_fraction: f64,
    /// WAL: write buffers, group-commit staging.
    pub wal_budget_fraction: f64,
    /// SPSC bridge: ring buffers, slab allocator, envelope staging.
    pub bridge_budget_fraction: f64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        // Sum = 0.94; the remaining 0.06 is unallocated headroom.
        Self {
            vector_budget_fraction: 0.25,
            graph_budget_fraction: 0.02,
            document_schemaless_budget_fraction: 0.02,
            document_strict_budget_fraction: 0.02,
            kv_budget_fraction: 0.02,
            columnar_budget_fraction: 0.03,
            timeseries_budget_fraction: 0.09,
            spatial_budget_fraction: 0.02,
            array_budget_fraction: 0.02,
            fts_budget_fraction: 0.03,
            sparse_budget_fraction: 0.14,
            crdt_budget_fraction: 0.09,
            query_budget_fraction: 0.16,
            wal_budget_fraction: 0.02,
            bridge_budget_fraction: 0.01,
        }
    }
}

impl EngineConfig {
    /// The configured budget fraction for `engine`.
    ///
    /// Exhaustive `match`: adding a new `EngineId` is a compile error here
    /// until it is given a fraction, which guarantees `to_byte_budgets`
    /// (and therefore the governor) can never silently omit an engine.
    fn fraction_for(&self, engine: EngineId) -> f64 {
        match engine {
            EngineId::Vector => self.vector_budget_fraction,
            EngineId::Graph => self.graph_budget_fraction,
            EngineId::DocumentSchemaless => self.document_schemaless_budget_fraction,
            EngineId::DocumentStrict => self.document_strict_budget_fraction,
            EngineId::Kv => self.kv_budget_fraction,
            EngineId::Columnar => self.columnar_budget_fraction,
            EngineId::Timeseries => self.timeseries_budget_fraction,
            EngineId::Spatial => self.spatial_budget_fraction,
            EngineId::Array => self.array_budget_fraction,
            EngineId::Fts => self.fts_budget_fraction,
            EngineId::Sparse => self.sparse_budget_fraction,
            EngineId::Crdt => self.crdt_budget_fraction,
            EngineId::Query => self.query_budget_fraction,
            EngineId::Wal => self.wal_budget_fraction,
            EngineId::Bridge => self.bridge_budget_fraction,
        }
    }

    /// Sum of all engine fractions. Must be `<= 1.0`.
    pub fn total_fraction(&self) -> f64 {
        EngineId::ALL.iter().map(|&e| self.fraction_for(e)).sum()
    }

    /// Validate that every fraction is positive and finite and that they
    /// sum to no more than the whole.
    ///
    /// Every engine must get a strictly-positive budget: a zero budget is
    /// reported by the governor as 100% utilization → Emergency pressure,
    /// which rejects writes to that engine with `resources exhausted`.
    pub fn validate(&self) -> crate::Result<()> {
        for &engine in EngineId::ALL {
            let f = self.fraction_for(engine);
            if !f.is_finite() || f <= 0.0 {
                return Err(crate::Error::Config {
                    detail: format!(
                        "engine budget fraction for {engine} must be a positive finite \
                         number, got {f}"
                    ),
                });
            }
        }
        let total = self.total_fraction();
        if total > 1.0 {
            return Err(crate::Error::Config {
                detail: format!("engine budget fractions sum to {total:.3}, must be <= 1.0"),
            });
        }
        Ok(())
    }

    /// Convert fractions to absolute byte budgets for every `EngineId`,
    /// given the global memory limit.
    pub fn to_byte_budgets(&self, global_limit: usize) -> EngineByteBudgets {
        let gl = global_limit as f64;
        let limits = EngineId::ALL
            .iter()
            .map(|&e| (e, (gl * self.fraction_for(e)) as usize))
            .collect();
        EngineByteBudgets { limits }
    }
}

/// Absolute byte budgets for every `EngineId`, derived from fractional config.
///
/// Wraps [`EngineLimits`], which is total by construction — every
/// `EngineId::ALL` member always has an entry, so there is no runtime check
/// to skip or forget.
#[derive(Debug, Clone)]
pub struct EngineByteBudgets {
    limits: EngineLimits,
}

impl EngineByteBudgets {
    /// Construct directly from an [`EngineLimits`] map.
    pub fn from_limits(limits: EngineLimits) -> Self {
        Self { limits }
    }

    /// Byte budget for a single engine.
    pub fn get(&self, engine: EngineId) -> usize {
        self.limits.get(engine)
    }

    /// Sum of all engine byte budgets.
    pub fn total(&self) -> usize {
        self.limits.total()
    }

    /// The underlying `EngineLimits`, ready to hand to `nodedb_mem::GovernorConfig`.
    pub fn into_engine_limits(self) -> EngineLimits {
        self.limits
    }

    /// Borrow the underlying `EngineLimits`.
    pub fn as_engine_limits(&self) -> &EngineLimits {
        &self.limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_fractions_valid() {
        let cfg = EngineConfig::default();
        cfg.validate().unwrap();
        assert!(cfg.total_fraction() <= 1.0);
    }

    #[test]
    fn default_covers_every_engine_with_a_positive_fraction() {
        let cfg = EngineConfig::default();
        for &engine in EngineId::ALL {
            assert!(
                cfg.fraction_for(engine) > 0.0,
                "{engine} has a non-positive default budget fraction"
            );
        }
    }

    #[test]
    fn over_budget_rejected() {
        let cfg = EngineConfig {
            vector_budget_fraction: 0.90,
            ..EngineConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn negative_budget_rejected() {
        let cfg = EngineConfig {
            crdt_budget_fraction: -0.1,
            ..EngineConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_budget_rejected() {
        let cfg = EngineConfig {
            kv_budget_fraction: 0.0,
            ..EngineConfig::default()
        };
        assert!(
            cfg.validate().is_err(),
            "a zero budget would put the engine at Emergency pressure"
        );
    }

    #[test]
    fn byte_budgets_cover_every_engine() {
        let cfg = EngineConfig::default();
        let budgets = cfg.to_byte_budgets(1024 * 1024 * 1024); // 1 GiB
        for &engine in EngineId::ALL {
            assert!(budgets.get(engine) > 0, "{engine} has a zero byte budget");
        }
        let one_gib = 1024.0 * 1024.0 * 1024.0;
        assert_eq!(
            budgets.get(EngineId::Vector),
            (one_gib * cfg.vector_budget_fraction) as usize
        );
        assert!(budgets.total() <= 1024 * 1024 * 1024);
    }

    #[test]
    fn missing_toml_keys_fall_back_to_defaults() {
        // An empty `[engines]` table — every field comes from `Default`.
        let cfg: EngineConfig = toml::from_str("").unwrap();
        cfg.validate().unwrap();
        let defaults = EngineConfig::default();
        for &engine in EngineId::ALL {
            assert_eq!(cfg.fraction_for(engine), defaults.fraction_for(engine));
        }
    }
}
