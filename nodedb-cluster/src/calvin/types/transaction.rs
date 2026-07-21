// SPDX-License-Identifier: BUSL-1.1

//! Calvin transaction class types.
//!
//! Provides [`ReadWriteSet`] and [`TxClass`] — the core transaction
//! representation submitted to the sequencer.

use nodedb_types::TenantId;
use nodedb_types::id::{DatabaseId, VShardId};
use serde::{Deserialize, Serialize};

use crate::error::CalvinError;

use super::lock_wire::TxnIdWire;
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
        self.participating_vshards_in_database(DatabaseId::DEFAULT)
    }

    /// Derive participants using database-scoped collection homes.
    pub fn participating_vshards_in_database(&self, database_id: DatabaseId) -> Vec<VShardId> {
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
                    let vshard =
                        VShardId::from_collection_in_database(database_id, engine_set.collection());
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
    /// Database scope used for collection homing, execution, WAL, and CDC.
    #[serde(default)]
    #[msgpack(default)]
    pub database_id: DatabaseId,
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
    /// the read at the commit serialization point (the local commit vote on
    /// `read_set_valid`). Empty for pure-write and autocommit transactions.
    #[serde(default)]
    #[msgpack(default)]
    pub versioned_reads: VersionedReadSet,
    /// Optional lock-table owner id for this transaction, propagated to
    /// `SequencedTxn.lock_owner`. `Some(R)` when the committing session holds
    /// read reservations under `R` — the commit batch then acquires its keys as
    /// `R` and self-upgrades those shared reservations. `None` (default) for
    /// transactions with no reservation. Wire-additive: decodes to `None` on
    /// older log entries.
    #[serde(default)]
    #[msgpack(default)]
    pub lock_owner: Option<TxnIdWire>,
    /// Cached participating-vshard set. Re-derived on decode; not serialized.
    #[serde(skip)]
    #[msgpack(ignore)]
    participating_vshards: Vec<VShardId>,
}

impl TxClass {
    /// Construct a validated **multi-vshard** transaction class.
    ///
    /// Rejects:
    /// - An empty write set (nothing to commit).
    /// - A write set that resolves to a single vshard — a `>=2`-intended
    ///   construction that collapses to one participant is a routing bug, so
    ///   it is rejected here. A transaction that is *legitimately* single-vshard
    ///   (a contended point write that must sequence to join the shared
    ///   per-vShard lock domain) must opt in explicitly via
    ///   [`TxClass::new_single_vshard`].
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
        Self::new_in_database(
            read_set,
            write_set,
            plans,
            tenant_id,
            DatabaseId::DEFAULT,
            dependent_reads,
            versioned_reads,
        )
    }

    /// Construct a validated multi-vshard class in an explicit database.
    pub fn new_in_database(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new_checked(
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
            false,
        )
    }

    /// Construct a validated transaction class that is permitted to resolve to a
    /// **single vshard**.
    ///
    /// This is the explicit opt-in for a contended single-vshard point write:
    /// the write-admission gate returned `RouteToCalvin` because a pending commit
    /// already holds the write's key, so the write must be sequenced through the
    /// deterministic scheduler to serialize on the SAME shared per-vShard
    /// `LockManager` the scheduler uses for multi-vshard transactions. Everything
    /// downstream of construction (sequencer inbox
    /// fan-out bound, per-vshard scheduler acquire/dispatch/commit, staged 2-phase
    /// commit) already tolerates a single participant — the `< 2` reject on
    /// [`TxClass::new`] was the only structural block.
    ///
    /// An empty write set is still rejected (nothing to commit), and a write set
    /// that resolves to *zero* participating vshards is rejected as unroutable.
    /// Signature mirrors [`TxClass::new`].
    pub fn new_single_vshard(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new_single_vshard_in_database(
            read_set,
            write_set,
            plans,
            tenant_id,
            DatabaseId::DEFAULT,
            dependent_reads,
            versioned_reads,
        )
    }

    /// Construct a single-vshard class in an explicit database.
    pub fn new_single_vshard_in_database(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
    ) -> Result<Self, CalvinError> {
        Self::new_checked(
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
            true,
        )
    }

    /// Shared construction body. `allow_single_vshard` relaxes the participant
    /// floor from 2 (multi-vshard) to 1 (single-vshard opt-in); an empty write
    /// set and a zero-participant write set are rejected on both paths.
    #[allow(clippy::too_many_arguments)] // shared validation for both constructor modes
    fn new_checked(
        read_set: ReadWriteSet,
        write_set: ReadWriteSet,
        plans: Vec<u8>,
        tenant_id: TenantId,
        database_id: DatabaseId,
        dependent_reads: Option<DependentReadSpec>,
        versioned_reads: VersionedReadSet,
        allow_single_vshard: bool,
    ) -> Result<Self, CalvinError> {
        if write_set.is_empty() {
            return Err(CalvinError::EmptyWriteSet);
        }
        let mut participating_vshards = write_set.participating_vshards_in_database(database_id);
        let min_participants = if allow_single_vshard { 1 } else { 2 };
        // The participant FLOOR is computed from the WRITE set ONLY, and BEFORE
        // the read-set union below: a txn that writes a single shard but reads N
        // additional shards is a legitimate single-write-shard txn and must not
        // trip the `>= 2` floor.
        if participating_vshards.len() < min_participants {
            let vshard = participating_vshards
                .first()
                .map(|v| v.as_u32())
                .unwrap_or(0);
            return Err(CalvinError::SingleVshardTxn { vshard });
        }
        // Union the read set's participating vShards: a shard that is only READ
        // (never written) still participates so it can validate the read at the
        // commit serialization point. This union MUST be applied identically in
        // `new_checked` and `restore_derived` — `participating_vshards` is
        // `#[serde(skip)]` and re-derived on decode, so an encoded and a decoded
        // `TxClass` would disagree on their participant set if the two diverged.
        for v in read_set.participating_vshards_in_database(database_id) {
            if !participating_vshards
                .iter()
                .any(|e| e.as_u32() == v.as_u32())
            {
                participating_vshards.push(v);
            }
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
        }
        // Stable ordering across encode/decode (participants ride the Raft log):
        // one final sort after ALL unions (write floor + read + passive), kept in
        // lockstep with `restore_derived`.
        participating_vshards.sort_by_key(|v| v.as_u32());
        Ok(Self {
            read_set,
            write_set,
            plans,
            tenant_id,
            database_id,
            dependent_reads,
            versioned_reads,
            lock_owner: None,
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
        let mut vshards = self
            .write_set
            .participating_vshards_in_database(self.database_id);
        // Union the read set's participating vShards — MUST match `new_checked`'s
        // union exactly so a decoded `TxClass` derives the identical participant
        // set the encoder computed (participants are not serialized).
        for v in self
            .read_set
            .participating_vshards_in_database(self.database_id)
        {
            if !vshards.iter().any(|e| e.as_u32() == v.as_u32()) {
                vshards.push(v);
            }
        }
        if let Some(ref spec) = self.dependent_reads {
            for &passive_vshard in spec.passive_reads.keys() {
                if !vshards.iter().any(|e| e.as_u32() == passive_vshard) {
                    vshards.push(VShardId::new(passive_vshard));
                }
            }
        }
        // Final stable sort after all unions — lockstep with `new_checked`.
        vshards.sort_by_key(|v| v.as_u32());
        self.participating_vshards = vshards;
    }

    /// Set the lock-table owner id propagated to `SequencedTxn.lock_owner`.
    pub fn set_lock_owner(&mut self, owner: Option<TxnIdWire>) {
        self.lock_owner = owner;
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
    #[test]
    fn database_scope_survives_msgpack_roundtrip() {
        let tx = TxClass::new_in_database(
            ReadWriteSet::new(vec![]),
            two_home_write_set(),
            vec![0x01],
            TenantId::new(1),
            DatabaseId::new(9),
            None,
            VersionedReadSet::default(),
        )
        .expect("valid TxClass");
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived();
        assert_eq!(decoded.database_id, DatabaseId::new(9));
        assert_eq!(decoded.participating_vshards(), tx.participating_vshards());
    }

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
        assert_eq!(decoded.database_id, DatabaseId::DEFAULT);
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
    fn new_single_vshard_accepts_one_participant_write_set() {
        // A single Document collection resolves to exactly one vshard. `new`
        // rejects it; `new_single_vshard` accepts it and caches the one home.
        let ws = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: "users".to_owned(),
            surrogates: SortedVec::new(vec![7u32]),
        }]);
        let want_vshard =
            VShardId::from_collection_in_database(DatabaseId::DEFAULT, "users").as_u32();

        // Strict path still rejects.
        let strict = TxClass::new(
            ReadWriteSet::new(vec![]),
            ws.clone(),
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        );
        assert!(matches!(strict, Err(CalvinError::SingleVshardTxn { .. })));

        // Opt-in path accepts and produces a single participating vshard.
        let tx = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ws,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("single-vshard TxClass accepted");
        assert_eq!(tx.participating_vshards().len(), 1);
        assert_eq!(tx.participating_vshards()[0].as_u32(), want_vshard);
    }

    #[test]
    fn new_single_vshard_still_rejects_empty_write_set() {
        let err = TxClass::new_single_vshard(
            ReadWriteSet::new(vec![]),
            ReadWriteSet::new(vec![]),
            vec![],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::EmptyWriteSet));
    }

    /// Find two collection names whose default-database vShards differ.
    fn two_distinct_vshard_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let name = format!("coll_{i}");
            let v = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &name).as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != v {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, v));
            }
        }
        panic!("could not find two distinct-vshard collections in 2048 tries");
    }

    #[test]
    fn read_set_vshards_union_into_participants_and_survive_roundtrip() {
        // A single-write-shard txn that READS a second collection homed on a
        // different vShard: the read shard joins the participant set (the
        // write-only floor still passes via the single-vshard opt-in), and the
        // union is reproduced identically on decode.
        let (wcoll, rcoll) = two_distinct_vshard_collections();
        let wv = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &wcoll).as_u32();
        let rv = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &rcoll).as_u32();
        assert_ne!(wv, rv);

        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: wcoll,
            surrogates: SortedVec::new(vec![1]),
        }]);
        // Read-set keyset carries no identity (empty surrogates) — homed by
        // collection, exactly as the builders' `read_set_from` constructs it.
        let read_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: rcoll,
            surrogates: SortedVec::new(vec![]),
        }]);

        let tx = TxClass::new_single_vshard(
            read_set,
            write_set,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .expect("single-write-shard txn with a cross-shard read is valid");

        let mut participants: Vec<u32> = tx
            .participating_vshards()
            .iter()
            .map(|v| v.as_u32())
            .collect();
        participants.sort_unstable();
        let mut want = vec![wv, rv];
        want.sort_unstable();
        assert_eq!(
            participants, want,
            "read shard must union into participants"
        );

        // Encode → decode → restore_derived reproduces the identical participant
        // set (participants are `#[serde(skip)]`, re-derived in lockstep).
        let bytes = zerompk::to_msgpack_vec(&tx).expect("encode");
        let mut decoded: TxClass = zerompk::from_msgpack(&bytes).expect("decode");
        decoded.restore_derived();
        assert_eq!(
            tx.participating_vshards(),
            decoded.participating_vshards(),
            "restore_derived must reproduce new_checked's read∪write participants"
        );
    }

    #[test]
    fn read_only_extra_shard_does_not_trip_write_floor() {
        // `new` (>=2 floor) still rejects a single-WRITE-shard txn even when the
        // read-set adds shards: the floor is computed from the write set only.
        let (wcoll, rcoll) = two_distinct_vshard_collections();
        let write_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: wcoll,
            surrogates: SortedVec::new(vec![1]),
        }]);
        let read_set = ReadWriteSet::new(vec![EngineKeySet::Document {
            collection: rcoll,
            surrogates: SortedVec::new(vec![]),
        }]);
        let err = TxClass::new(
            read_set,
            write_set,
            vec![0x01],
            TenantId::new(1),
            None,
            VersionedReadSet::default(),
        )
        .unwrap_err();
        assert!(matches!(err, CalvinError::SingleVshardTxn { .. }));
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
