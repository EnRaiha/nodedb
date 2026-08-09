// SPDX-License-Identifier: BUSL-1.1

//! `GatewayVersionSet` — deterministic ordered set of (collection, version)
//! pairs used as a plan cache key and as the payload for
//! `DescriptorVersionEntry` in `ExecuteRequest`.
//!
//! Collected from a `PhysicalPlan` by walking every variant and extracting
//! the collection name.

use std::hash::{DefaultHasher, Hash, Hasher};

use nodedb_physical::physical_plan::PhysicalPlan;

/// Deterministic ordered set of `(collection_name, descriptor_version)` pairs.
///
/// - Sorted by `collection_name` for stable equality comparisons.
/// - Duplicate names are de-duped (last write wins — within a single plan
///   the version is stable, so duplicates carry the same version).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewayVersionSet(Vec<(String, u64)>);

impl GatewayVersionSet {
    /// Construct from explicit (name, version) pairs.
    pub fn from_pairs(mut pairs: Vec<(String, u64)>) -> Self {
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        Self(pairs)
    }

    /// Re-look-up the current descriptor version for every collection already
    /// in this set, returning a new set with refreshed versions.
    ///
    /// Used to check a cached version set is still current before trusting a
    /// plan-cache hit: if the result equals the original, the cached plan is
    /// still valid. `version_fn` receives a collection name and returns the
    /// current descriptor version (or 0 if unknown).
    pub fn reverify(&self, version_fn: impl Fn(&str) -> u64) -> Self {
        let pairs: Vec<(String, u64)> = self
            .0
            .iter()
            .map(|(name, _)| {
                let v = version_fn(name);
                (name.clone(), v)
            })
            .collect();
        Self::from_pairs(pairs)
    }

    /// Collect all collection names touched by a plan with the provided
    /// version lookup function.
    ///
    /// `version_fn` receives a collection name and returns the current
    /// descriptor version (or 0 if unknown).
    pub fn from_plan(plan: &PhysicalPlan, version_fn: impl Fn(&str) -> u64) -> Self {
        let names = touched_collections(plan);
        let mut pairs: Vec<(String, u64)> = names
            .into_iter()
            .map(|name| {
                let v = version_fn(&name);
                (name, v)
            })
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|a, b| a.0 == b.0);
        Self(pairs)
    }

    /// Iterate over `(collection, version)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = &(String, u64)> {
        self.0.iter()
    }

    /// Returns `true` if the set mentions `name` at any version.
    pub fn contains_collection(&self, name: &str) -> bool {
        self.0.iter().any(|(n, _)| n == name)
    }

    /// Returns `true` if the set mentions `name` at exactly `version`.
    pub fn matches(&self, name: &str, version: u64) -> bool {
        self.0
            .iter()
            .any(|(n, v)| n.as_str() == name && *v == version)
    }

    /// Stable u64 hash of this set, used as part of `PlanCacheKey`.
    pub fn stable_hash(&self) -> u64 {
        let mut h = DefaultHasher::new();
        self.hash(&mut h);
        h.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// Extract every collection name touched by a `PhysicalPlan`.
///
/// Returns a `Vec<String>` that may contain duplicates; callers are
/// responsible for de-duplication (e.g., `GatewayVersionSet::from_plan`).
pub fn touched_collections(plan: &PhysicalPlan) -> Vec<String> {
    use nodedb_physical::physical_plan::*;

    let mut out: Vec<String> = Vec::new();

    match plan {
        // ── KV ──────────────────────────────────────────────────────────
        PhysicalPlan::Kv(op) => {
            use KvOp::*;
            match op {
                Get { collection, .. }
                | Put { collection, .. }
                | Insert { collection, .. }
                | InsertIfAbsent { collection, .. }
                | InsertOnConflictUpdate { collection, .. }
                | Delete { collection, .. }
                | Scan { collection, .. }
                | Expire { collection, .. }
                | Persist { collection, .. }
                | GetTtl { collection, .. }
                | BatchGet { collection, .. }
                | BatchPut { collection, .. }
                | RegisterIndex { collection, .. }
                | DropIndex { collection, .. }
                | FieldGet { collection, .. }
                | FieldSet { collection, .. }
                | Truncate { collection }
                | Incr { collection, .. }
                | IncrFloat { collection, .. }
                | Cas { collection, .. }
                | GetSet { collection, .. }
                | Transfer { collection, .. }
                | RegisterSortedIndex { collection, .. }
                | MaterializeScan { collection, .. } => out.push(collection.clone()),

                // TransferItem touches two collections.
                TransferItem {
                    source_collection,
                    dest_collection,
                    ..
                } => {
                    out.push(source_collection.clone());
                    out.push(dest_collection.clone());
                }

                // Sorted index ops — not per-collection.
                DropSortedIndex { .. }
                | SortedIndexRank { .. }
                | SortedIndexTopK { .. }
                | SortedIndexRange { .. }
                | SortedIndexCount { .. }
                | SortedIndexScore { .. } => {}
            }
        }

        // ── Document ────────────────────────────────────────────────────
        PhysicalPlan::Document(op) => {
            use DocumentOp::*;
            match op {
                PointGet { collection, .. }
                | PointPut { collection, .. }
                | PointInsert { collection, .. }
                | PointDelete { collection, .. }
                | PointUpdate { collection, .. }
                | Scan { collection, .. }
                | BatchInsert { collection, .. }
                | RangeScan { collection, .. }
                | Register { collection, .. }
                | IndexLookup { collection, .. }
                | IndexedFetch { collection, .. }
                | DropIndex { collection, .. }
                | BackfillIndex { collection, .. }
                | Truncate { collection, .. }
                | EstimateCount { collection, .. }
                | Upsert { collection, .. }
                | BulkUpdate { collection, .. }
                | BulkDelete { collection, .. }
                | MaterializeScan { collection, .. }
                // The TARGET collection whose balance this derived write moves.
                // It is the only collection the op touches — the source row that
                // caused it rides a separate task on its own vShard.
                | ApplyBalanceDelta { collection, .. } => out.push(collection.clone()),

                InsertSelect {
                    target_collection,
                    source_collection,
                    ..
                } => {
                    out.push(target_collection.clone());
                    out.push(source_collection.clone());
                }

                UpdateFromJoin {
                    target_collection,
                    source_collection,
                    ..
                } => {
                    out.push(target_collection.clone());
                    out.push(source_collection.clone());
                }

                Merge {
                    target_collection,
                    source_collection,
                    ..
                } => {
                    out.push(target_collection.clone());
                    out.push(source_collection.clone());
                }
            }
        }

        // ── Vector ──────────────────────────────────────────────────────
        PhysicalPlan::Vector(op) => {
            use VectorOp::*;
            match op {
                Search { collection, .. }
                | Insert { collection, .. }
                | BatchInsert { collection, .. }
                | MultiSearch { collection, .. }
                | Delete { collection, .. }
                | SetParams { collection, .. }
                | DropIndex { collection, .. }
                | QueryStats { collection, .. }
                | Seal { collection, .. }
                | CompactIndex { collection, .. }
                | Rebuild { collection, .. }
                | SparseInsert { collection, .. }
                | SparseSearch { collection, .. }
                | SparseDelete { collection, .. }
                | MultiVectorInsert { collection, .. }
                | MultiVectorDelete { collection, .. }
                | MultiVectorScoreSearch { collection, .. }
                | DirectUpsert { collection, .. }
                | DeleteBySurrogate { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Text ────────────────────────────────────────────────────────
        PhysicalPlan::Text(op) => {
            use TextOp::*;
            match op {
                Search { collection, .. }
                | BM25ScoreScan { collection, .. }
                | HybridSearch { collection, .. }
                | HybridSearchTriple { collection, .. }
                | PhraseSearch { collection, .. }
                | FtsIndexDoc { collection, .. }
                | FtsDeleteDoc { collection, .. }
                | SetTextConfig { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Graph ────────────────────────────────────────────────────────
        PhysicalPlan::Graph(op) => {
            use GraphOp::*;
            match op {
                // These ops target a named graph collection.
                RagFusion { collection, .. } => out.push(collection.clone()),
                TemporalNeighbors { collection, .. } => out.push(collection.clone()),
                Stats {
                    collection: Some(c),
                    ..
                } => out.push(c.clone()),

                // Structural ops use node IDs, not a collection name.
                EdgePut { .. }
                | EdgePutBatch { .. }
                | EdgeDelete { .. }
                | EdgeDeleteBatch { .. }
                | Hop { .. }
                | Neighbors { .. }
                | NeighborsMulti { .. }
                | Path { .. }
                | Subgraph { .. }
                | Algo { .. }
                | BspSuperstep(_)
                | WccSuperstep(_)
                | Match { .. }
                | MatchContinuation { .. }
                | MatchVarLenResume { .. }
                | TemporalAlgorithm { .. }
                | Stats {
                    collection: None, ..
                }
                | SetNodeLabels { .. }
                | RemoveNodeLabels { .. } => {}
            }
        }

        // ── Columnar ─────────────────────────────────────────────────────
        PhysicalPlan::Columnar(op) => {
            use ColumnarOp::*;
            match op {
                Scan { collection, .. }
                | Insert { collection, .. }
                | Update { collection, .. }
                | Delete { collection, .. }
                | MaterializeScan { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Timeseries ───────────────────────────────────────────────────
        PhysicalPlan::Timeseries(op) => {
            use TimeseriesOp::*;
            match op {
                Scan { collection, .. } | Ingest { collection, .. } => out.push(collection.clone()),
            }
        }

        // ── Spatial ──────────────────────────────────────────────────────
        PhysicalPlan::Spatial(op) => {
            use SpatialOp::*;
            match op {
                Scan { collection, .. } => out.push(collection.clone()),
                // Sync ingest ops target a collection but do not produce
                // versioned read output — no version-set entry needed.
                Insert { .. } | Delete { .. } => {}
            }
        }

        // ── CRDT ─────────────────────────────────────────────────────────
        PhysicalPlan::Crdt(op) => {
            use CrdtOp::*;
            match op {
                Read { collection, .. }
                | PreviewApply { collection, .. }
                | Apply { collection, .. }
                | ApplyAuthenticated { collection, .. }
                | SetPolicy { collection, .. }
                | GetPolicy { collection, .. }
                | ReadAtVersion { collection, .. }
                | RestoreToVersion { collection, .. }
                | ListInsert { collection, .. }
                | ListDelete { collection, .. }
                | ListMove { collection, .. }
                | DocUpsert { collection, .. }
                | DocDelete { collection, .. } => out.push(collection.clone()),

                // `ImportSnapshot` is a whole-tenant Loro import and
                // `SetConstraints` / `DropConstraints` install validator rules —
                // none produce per-collection versioned read output.
                GetVersionVector { .. }
                | ReadConstraints { .. }
                | ExportDelta { .. }
                | CompactAtVersion { .. }
                | ImportSnapshot { .. }
                | SetConstraints { .. }
                | DropConstraints { .. } => {}
            }
        }

        // ── Query ─────────────────────────────────────────────────────────
        PhysicalPlan::Query(op) => {
            use QueryOp::*;
            match op {
                Aggregate { collection, .. }
                | PartialAggregate { collection, .. }
                | PartialAggregateState { collection, .. }
                | FacetCounts { collection, .. }
                | RecursiveScan { collection, .. } => out.push(collection.clone()),

                HashJoin {
                    left_collection,
                    right_collection,
                    ..
                }
                | NestedLoopJoin {
                    left_collection,
                    right_collection,
                    ..
                }
                | SortMergeJoin {
                    left_collection,
                    right_collection,
                    ..
                } => {
                    out.push(left_collection.clone());
                    out.push(right_collection.clone());
                }

                LateralTopK {
                    inner_collection, ..
                } => {
                    out.push(inner_collection.clone());
                }

                LateralLoop {
                    inner_collection, ..
                } => {
                    out.push(inner_collection.clone());
                }

                // Exchange: recurse into the child plan for collection extraction.
                Exchange(op) => {
                    let child_collections = touched_collections(&op.child);
                    out.extend(child_collections);
                }

                // PostProcess: recurse into the materialized child for the
                // collections it reads.
                PostProcess { input, .. } => {
                    out.extend(touched_collections(input));
                }

                // ProviderScan is a catalog/constant source — no user collection.
                ProviderScan { .. } => {}

                // ShuffleJoinConsume / ShuffleAggregateConsume read node-local
                // staged files keyed by path, not by a catalog collection — no
                // version contribution.
                ShuffleJoinConsume { .. } | ShuffleAggregateConsume { .. } => {}

                // No user-collection field.
                RecursiveValue { .. } => {}
            }
        }

        // ── Meta ─────────────────────────────────────────────────────────
        PhysicalPlan::Meta(nodedb_physical::physical_plan::MetaOp::TransactionBatch {
            plans,
            ..
        }) => {
            for plan in plans {
                out.extend(touched_collections(plan));
            }
        }
        PhysicalPlan::Meta(_) => {
            // Other Meta ops target infrastructure, not user collections.
        }

        // ── Array ────────────────────────────────────────────────────────
        // Arrays use a separate catalog from collection-based engines and
        // do not contribute to the version set.
        PhysicalPlan::Array(_) => {}

        // ClusterArray variants are handled on the Control Plane before reaching
        // the gateway; they carry no collection version set contribution.
        PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {}
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{KvOp, PhysicalPlan};

    #[test]
    fn from_plan_kv_get() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "users".into(),
            key: b"key".to_vec(),
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let vs = GatewayVersionSet::from_plan(&plan, |_| 5);
        assert_eq!(vs.len(), 1);
        assert!(vs.matches("users", 5));
    }

    #[test]
    fn from_plan_deterministic_order() {
        let plan = PhysicalPlan::Kv(KvOp::Get {
            collection: "alpha".into(),
            key: vec![],
            rls_filters: vec![],
            surrogate_ceiling: None,
        });
        let vs1 = GatewayVersionSet::from_plan(&plan, |_| 1);
        let vs2 = GatewayVersionSet::from_plan(&plan, |_| 1);
        assert_eq!(vs1, vs2);
        assert_eq!(vs1.stable_hash(), vs2.stable_hash());
    }

    #[test]
    fn contains_collection() {
        let vs = GatewayVersionSet::from_pairs(vec![("orders".into(), 3), ("users".into(), 7)]);
        assert!(vs.contains_collection("orders"));
        assert!(vs.contains_collection("users"));
        assert!(!vs.contains_collection("products"));
    }

    #[test]
    fn dedup_on_construction() {
        let vs = GatewayVersionSet::from_pairs(vec![
            ("a".into(), 1),
            ("a".into(), 1), // duplicate
        ]);
        assert_eq!(vs.len(), 1);
    }

    #[test]
    fn kv_transfer_item_extracts_both_collections() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "from_col".into(),
            dest_collection: "to_col".into(),
            item_key: vec![],
            dest_key: vec![],
            surrogate: nodedb_types::Surrogate::ZERO,
            source_rls_write_check: Vec::new(),
            dest_rls_write_check: Vec::new(),
        });
        let names = touched_collections(&plan);
        assert!(names.contains(&"from_col".to_string()));
        assert!(names.contains(&"to_col".to_string()));
    }
}
