// SPDX-License-Identifier: BUSL-1.1

//! Calvin transaction class types.
//!
//! Provides [`ReadWriteSet`] and [`TxClass`] — the core transaction
//! representation submitted to the sequencer.

use nodedb_types::TenantId;
use nodedb_types::id::{DatabaseId, VShardId};
use serde::{Deserialize, Serialize};

use crate::error::CalvinError;

use super::primitives::{DependentReadSpec, EngineKeySet, VersionedReadSet};

// ── ReadWriteSet ──────────────────────────────────────────────────────────────

/// A set of keys spanning one or more engines, forming either the read set
/// or the write set of a Calvin transaction.
///
/// Cross-engine atomic transactions — e.g. a Document+Vector insert that must
/// land atomically — require all affected engines to appear in a single
/// `ReadWriteSet`. Decomposing by engine would break atomicity.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ReadWriteSet(pub Vec<EngineKeySet>);

impl ReadWriteSet {
    pub fn new(sets: Vec<EngineKeySet>) -> Self {
        Self(sets)
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|s| s.is_empty())
    }

    /// Derive the set of vShards participating in this read/write set.
    ///
    /// For Document/Vector/KV entries the vshard is derived from the
    /// collection name (collection-level routing, consistent with the
    /// per-vshard Raft groups that own each collection). KV collections
    /// are also assigned a single vshard at creation time.
    ///
    /// For Edge entries the participating vShards are the edge's
    /// `home_vshards` (the `from_key(src)` / `from_key(dst)` key-hashed
    /// homes), NOT the collection name: a graph edge is dual-homed across
    /// its two endpoint vShards so it can be written atomically to both.
    ///
    /// This derivation is re-run on decode rather than serialized, so the
    /// serialized bytes remain deterministic regardless of how `VShardId`
    /// is computed.
    pub fn participating_vshards(&self) -> Vec<VShardId> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for engine_set in &self.0 {
            match engine_set {
                EngineKeySet::Edge { home_vshards, .. } => {
                    for &home in home_vshards.as_slice() {
                        let vshard = VShardId::new(home);
                        if seen.insert(vshard.as_u32()) {
                            result.push(vshard);
                        }
                    }
                }
                EngineKeySet::Document { .. }
                | EngineKeySet::Vector { .. }
                | EngineKeySet::Kv { .. } => {
                    let vshard = VShardId::from_collection_in_database(
                        DatabaseId::DEFAULT,
                        engine_set.collection(),
                    );
                    if seen.insert(vshard.as_u32()) {
                        result.push(vshard);
                    }
                }
            }
        }
        result.sort_by_key(|v| v.as_u32());
        result
    }
}

// ── TxClass ───────────────────────────────────────────────────────────────────

/// A fully-declared Calvin transaction class.
///
/// Constructed via [`TxClass::new`], which validates the write set and caches
/// the participating-vshard set. The `participating_vshards` field is skipped
/// during serialization and re-derived on decode to keep serialized bytes
/// byte-deterministic.
///
/// Map-encoded (`#[msgpack(map)]`) so fields can be added additively: an older
/// serialized `TxClass` that predates a field decodes it to its default (the
/// field carries `#[serde(default)]` + `#[msgpack(default)]`). This is what
/// lets `TxClass` bytes already on the sequencer Raft log survive a schema
/// addition and still replay on restart.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct TxClass {
    /// Keys that must be read (may be empty for pure-write transactions).
    ///
    /// This is the key-IDENTITY set used for locking/routing. The
    /// LSN-versioned read observations used for optimistic-concurrency
    /// validation live in `versioned_reads`.
    pub read_set: ReadWriteSet,
    /// Keys that will be written. Must span at least two vShards.
    pub write_set: ReadWriteSet,
    /// Opaque msgpack-encoded physical plan bytes. Decoded by the executor
    /// in the `nodedb` crate; the sequencer treats this as an opaque blob.
    pub plans: Vec<u8>,
    /// Tenant scope. All keys in `read_set` and `write_set` must belong to
    /// this tenant; cross-tenant transactions are rejected at construction.
    pub tenant_id: TenantId,
    /// Optional dependent-read specification.
    ///
    /// When present, this transaction is a dependent-read Calvin txn: the
    /// passive vshards listed here must read their keys and broadcast the
    /// results (via `ReplicatedWrite::CalvinReadResult`) before the active
    /// participants may write.
    ///
    /// `None` for static-set transactions (the common case).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub dependent_reads: Option<DependentReadSpec>,
    /// LSN-versioned, predicate-aware read-set captured during the session.
    ///
    /// Each entry carries the responding shard's write-LSN watermark at read
    /// time plus the point/predicate identity, so a participant can validate
    /// the read at the commit serialization point. Empty for pure-write and
    /// autocommit transactions. Additive: predates nothing that consumes it
    /// yet — carried here so the version travels on the replicated log.
    #[serde(default)]
    #[msgpack(default)]
    pub versioned_reads: VersionedReadSet,
    /// Cached participating-vshard set. Re-derived on decode; not serialized.
    #[serde(skip)]
    #[msgpack(ignore)]
    participating_vshards: Vec<VShardId>,
}

impl TxClass {
    /// Construct a validated transaction class.
    ///
    /// Rejects:
    /// - An empty write set (nothing to commit).
    /// - A write set that resolves to a single vshard (must use the single-
    ///   shard fast path instead).
    ///
    /// Pass `dependent_reads: None` for static-set transactions (the common
    /// case).  Pass `Some(spec)` for dependent-read (OLLP) transactions.
    ///
    /// `versioned_reads` carries the LSN-versioned read observations; pass
    /// [`VersionedReadSet::default`] (empty) for pure-write / autocommit
    /// transactions that accumulated no session read-set.
    pub fn new(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        if write_set.is_empty() {
            return Err(CalvinError::EmptyWriteSet);
        }
        let mut participating_vshards = write_set.participating_vshards();
        if participating_vshards.len() < 2 {
            let vshard = participating_vshards
                .first()
                .map(|v| v.as_u32())
                .unwrap_or(0);
            return Err(CalvinError::SingleVshardTxn { vshard });
        }
        // Extend participating_vshards with passive vshards from dependent_reads.
        if let Some(ref spec) = dependent_reads {
            for &passive_vshard in spec.passive_reads.keys() {
                let v = VShardId::new(passive_vshard);
                if !participating_vshards
                    .iter()
                    .any(|e| e.as_u32() == passive_vshard)
                {
                    participating_vshards.push(v);
                }
            }
            participating_vshards.sort_by_key(|v| v.as_u32());
        }
        Ok(Self {
            read_set,
            write_set,
            plans,
            tenant_id,
            dependent_reads,
            versioned_reads,
            participating_vshards,
        })
    }

    /// Ergonomic constructor for dependent-read Calvin transactions.
    ///
    /// Equivalent to `TxClass::new(read_set, write_set, plans, tenant_id,
    /// Some(dependent_reads), versioned_reads)`.
    pub fn new_dependent(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: DependentReadSpec,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new(
            read_set,
            write_set,
            plans,
            tenant_id,
            Some(dependent_reads),
            versioned_reads,
        )
    }

    /// The vShards that must receive this transaction's slice.
    ///
    /// Derived from the write set's collection names. Re-derived after
    /// deserialization via [`TxClass::restore_derived`].
    pub fn participating_vshards(&self) -> &[VShardId] {
        &self.participating_vshards
    }

    /// Re-derive fields skipped during serialization.
    ///
    /// Call this immediately after deserializing a `TxClass` that came off
    /// the wire or out of the Raft log.
    pub fn restore_derived(&mut self) {
        let mut vshards = self.write_set.participating_vshards();
        if let Some(ref spec) = self.dependent_reads {
            for &passive_vshard in spec.passive_reads.keys() {
                if !vshards.iter().any(|e| e.as_u32() == passive_vshard) {
                    vshards.push(VShardId::new(passive_vshard));
                }
            }
            vshards.sort_by_key(|v| v.as_u32());
        }
        self.participating_vshards = vshards;
    }
}

#[cfg(test)]
mod tests {
    use super::super::primitives::{
        EngineTag, ReadKeyIdent, SortedVec, VersionedReadEntry, VersionedReadSet,
    };
    use super::*;
    use nodedb_types::{KeyRepr, Lsn};

    fn sample_versioned_reads() -> VersionedReadSet {
        VersionedReadSet::new(vec![
            VersionedReadEntry {
                engine: EngineTag::Kv,
                collection: "kv_col".to_owned(),
                key: ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(&b"k1"[..]))),
                read_lsn: Lsn::new(7),
            },
            VersionedReadEntry {
                engine: EngineTag::Document,
                collection: "doc_col".to_owned(),
                key: ReadKeyIdent::Predicate,
                read_lsn: Lsn::new(11),
            },
        ])
    }

    fn two_home_write_set() -> ReadWriteSet {
        let (_src, _dst, sv, dv) = two_distinct_key_vshards();
        ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![sv, dv]),
        }])
    }

    #[test]
    fn versioned_reads_survive_msgpack_roundtrip() {
        let reads = sample_versioned_reads();
        let tx = TxClass::new(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x09, 0x09],
            TenantId::new(1),
            None,
            reads.clone(),
        )
        .expect("valid TxClass");

        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode TxClass");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode TxClass");
        decoded.restore_derived();

        // Every read_lsn and the Point/Predicate distinction survive exactly.
        assert_eq!(decoded.versioned_reads, reads);
        assert_eq!(decoded.versioned_reads.len(), 2);
        let point = decoded
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Point(_)))
            .expect("point entry");
        assert_eq!(point.read_lsn, Lsn::new(7));
        assert_eq!(
            point.key,
            ReadKeyIdent::Point(KeyRepr::KvKey(Box::from(&b"k1"[..])))
        );
        let predicate = decoded
            .versioned_reads
            .iter()
            .find(|e| matches!(e.key, ReadKeyIdent::Predicate))
            .expect("predicate entry");
        assert_eq!(predicate.read_lsn, Lsn::new(11));
    }

    /// Mirror of `TxClass`'s wire shape from BEFORE `versioned_reads` existed:
    /// map-encoded with the original fields only. Proves an old serialized
    /// `TxClass` (no `versioned_reads` key) still decodes — the field defaults
    /// to empty — so Raft-logged transactions survive the schema addition.
    #[derive(zerompk::ToMessagePack)]
    #[msgpack(map)]
    struct LegacyTxClass {
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
    }

    #[test]
    fn decodes_legacy_bytes_without_versioned_reads_field() {
        let legacy = LegacyTxClass {
            read_set: ReadWriteSet::new(vec![]),
            write_set: two_home_write_set(),
            plans: vec![0x01, 0x02],
            tenant_id: TenantId::new(3),
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).expect("encode legacy");

        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode legacy as TxClass");
        decoded.restore_derived();

        assert!(decoded.versioned_reads.is_empty());
        assert!(decoded.dependent_reads.is_none());
        assert_eq!(decoded.tenant_id, TenantId::new(3));
        assert_eq!(decoded.plans, vec![0x01, 0x02]);
        assert_eq!(decoded.participating_vshards().len(), 2);
    }

    /// Find two distinct string keys whose `from_key` vShards differ.
    fn two_distinct_key_vshards() -> (String, String, u32, u32) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let key = format!("node_{i}");
            let v = VShardId::from_key(key.as_bytes()).as_u32();
            if let Some((ref fkey, fv)) = first {
                if fv != v {
                    return (fkey.clone(), key, fv, v);
                }
            } else {
                first = Some((key, v));
            }
        }
        panic!("could not find two distinct-vshard keys in 2048 tries");
    }

    #[test]
    fn edge_keyset_participating_vshards_are_key_homed() {
        // An edge whose endpoints hash to two DISTINCT from_key vShards must
        // contribute exactly those two homes — NOT the collection's vShard.
        let (src_key, dst_key, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        // Pick a collection name whose collection-homed vShard differs from
        // both endpoint homes, to prove routing ignores the collection.
        let coll_v = VShardId::from_collection_in_database(DatabaseId::DEFAULT, "follows").as_u32();

        let ws = ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![src_v, dst_v]),
        }]);

        let mut got: Vec<u32> = ws
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        got.sort();
        let mut want = vec![src_v, dst_v];
        want.sort();
        assert_eq!(got, want, "edge routes to its from_key homes");
        assert!(
            !got.contains(&coll_v) || coll_v == src_v || coll_v == dst_v,
            "edge must NOT route by collection vShard {coll_v}"
        );

        // Sanity: the keys we hashed actually produce these homes.
        assert_eq!(VShardId::from_key(src_key.as_bytes()).as_u32(), src_v);
        assert_eq!(VShardId::from_key(dst_key.as_bytes()).as_u32(), dst_v);
    }

    #[test]
    fn edge_keyset_single_home_when_endpoints_collide() {
        // When src and dst hash to the same vShard, the deduped home set is
        // a single vShard.
        let only = VShardId::from_key(b"same").as_u32();
        let ws = ReadWriteSet::new(vec![EngineKeySet::Edge {
            collection: "follows".to_owned(),
            edges: SortedVec::new(vec![(1u32, 2u32)]),
            home_vshards: SortedVec::new(vec![only, only]),
        }]);
        let got: Vec<u32> = ws
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        assert_eq!(got, vec![only]);
    }
}
