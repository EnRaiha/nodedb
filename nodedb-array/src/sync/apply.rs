// SPDX-License-Identifier: Apache-2.0

//! Pure op-application dispatcher for array CRDT sync.
//!
//! [`apply_op`] is the single entry point for merging an incoming [`ArrayOp`]
//! into local engine state. It enforces shape validation, schema-version
//! gating, idempotency, and tile-cache invalidation in a fixed order.
//!
//! Engine implementations live in `nodedb-lite` and `nodedb`; this file
//! ships only the abstract [`ApplyEngine`] trait, the outcome types, and the
//! pure dispatcher function.

#[cfg(test)]
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::sync::Mutex;

use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::sync::hlc::Hlc;
use crate::sync::op::{ArrayOp, ArrayOpKind};
use crate::types::coord::value::CoordValue;

// ─── Outcome types ───────────────────────────────────────────────────────────

/// Outcome returned by [`apply_op`].
#[derive(Clone, Debug, PartialEq)]
pub enum ApplyOutcome {
    /// The op was already present; no state was changed.
    Idempotent,
    /// The op was applied successfully.
    Applied,
    /// The op was rejected; the reason is attached.
    Rejected(ApplyRejection),
}

/// Reason an op was rejected by [`apply_op`].
#[derive(Clone, Debug, PartialEq)]
pub enum ApplyRejection {
    /// The op's `schema_hlc` is strictly newer than the local schema HLC.
    ///
    /// The receiver should request the updated schema and retry.
    SchemaTooNew {
        /// Local schema HLC at the time of rejection.
        local: Hlc,
        /// Schema HLC carried by the op.
        op: Hlc,
    },
    /// The named array is not known to this replica.
    ///
    /// The receiver should request the array schema before retrying.
    ArrayUnknown {
        /// Name of the unknown array.
        name: String,
    },
    /// The op violates the shape contract (e.g. `Put` without attrs).
    ShapeInvalid {
        /// Human-readable description of the violation.
        detail: String,
    },
    /// A `Put` op's attrs do not match the local schema: wrong arity, a
    /// value of another type than the attr declares, or `Null` on a
    /// NOT NULL attr.
    SchemaViolation {
        /// Human-readable description of the violation.
        detail: String,
    },
    /// The engine rejected the op (e.g. a transient storage error).
    ///
    /// The rejection is wrapped here so callers can record it without
    /// propagating a hard error. Only [`ArrayError::SegmentCorruption`] and
    /// [`ArrayError::HlcLockPoisoned`] are considered corruption-grade and
    /// propagated directly; all other engine errors become this variant.
    EngineRejected {
        /// Human-readable description of the engine error.
        detail: String,
    },
}

// ─── ApplyEngine trait ───────────────────────────────────────────────────────

/// Abstract interface to local engine state consumed by [`apply_op`].
///
/// Implementations live in `nodedb-lite` and `nodedb` crates. The trait is
/// intentionally *not* `Send + Sync`-bounded — implementations choose their
/// own threading model.
pub trait ApplyEngine {
    /// Return the current schema HLC for `array`, or `None` if the array is
    /// not known to this replica.
    fn schema_hlc(&self, array: &str) -> ArrayResult<Option<Hlc>>;

    /// Return the current schema of `array`, or `None` if the array is not
    /// known to this replica.
    fn array_schema(&self, array: &str) -> ArrayResult<Option<ArraySchema>>;

    /// Return `true` if `hlc` has already been applied to `array`.
    ///
    /// Used for idempotent re-delivery detection.
    fn already_seen(&self, array: &str, hlc: Hlc) -> ArrayResult<bool>;

    /// Apply a `Put` op to the engine.
    fn apply_put(&mut self, op: &ArrayOp) -> ArrayResult<()>;

    /// Apply a `Delete` op to the engine.
    fn apply_delete(&mut self, op: &ArrayOp) -> ArrayResult<()>;

    /// Apply an `Erase` op to the engine.
    fn apply_erase(&mut self, op: &ArrayOp) -> ArrayResult<()>;

    /// Invalidate any tile-cache entry covering `coord` in `array`.
    ///
    /// Called after every successful op application so that subsequent reads
    /// see the updated state.
    fn invalidate_tile(&mut self, array: &str, coord: &[CoordValue]) -> ArrayResult<()>;
}

// ─── Dispatcher ──────────────────────────────────────────────────────────────

/// Apply `op` to `engine`, returning an outcome without panicking.
///
/// Steps:
/// 1. Shape validation — on error: `Rejected(ShapeInvalid)`.
/// 2. Schema HLC check — `None` → `Rejected(ArrayUnknown)`;
///    `op.header.schema_hlc` strictly later on the clock than `local`
///    → `Rejected(SchemaTooNew)`. Same-instant schemas are admitted.
/// 3. Idempotency — already seen → `Idempotent`.
/// 4. Schema check — a `Put` op's attrs are checked against the local schema
///    with [`ArraySchema::check_cell`] — on error: `Rejected(SchemaViolation)`.
/// 5. Dispatch to `apply_put`/`apply_delete`/`apply_erase`. Engine errors
///    that indicate corruption (`SegmentCorruption`, `HlcLockPoisoned`) are
///    propagated; all others become `Rejected(EngineRejected)`.
/// 6. Tile-cache invalidation.
/// 7. Return `Applied`.
pub fn apply_op<E: ApplyEngine>(engine: &mut E, op: &ArrayOp) -> ArrayResult<ApplyOutcome> {
    // 1. Shape validation.
    if let Err(e) = op.validate_shape() {
        return Ok(ApplyOutcome::Rejected(ApplyRejection::ShapeInvalid {
            detail: e.to_string(),
        }));
    }

    // 2. Schema HLC gating.
    match engine.schema_hlc(&op.header.array)? {
        None => {
            return Ok(ApplyOutcome::Rejected(ApplyRejection::ArrayUnknown {
                name: op.header.array.clone(),
            }));
        }
        // Clock-only comparison: an op whose schema was stamped in the SAME
        // instant as ours is not evidence that ours is stale, so it must be
        // admitted. Using the full `Ord` here would let the arbitrary
        // `replica_id` tiebreak decide admission — see `Hlc::is_later_than`.
        Some(local_schema) if op.header.schema_hlc.is_later_than(&local_schema) => {
            return Ok(ApplyOutcome::Rejected(ApplyRejection::SchemaTooNew {
                local: local_schema,
                op: op.header.schema_hlc,
            }));
        }
        Some(_) => {}
    }

    // 3. Idempotency.
    if engine.already_seen(&op.header.array, op.header.hlc)? {
        return Ok(ApplyOutcome::Idempotent);
    }

    // 4. Schema check of a Put's attrs.
    if let (ArrayOpKind::Put, Some(attrs)) = (op.kind, op.attrs.as_deref()) {
        let Some(schema) = engine.array_schema(&op.header.array)? else {
            return Ok(ApplyOutcome::Rejected(ApplyRejection::ArrayUnknown {
                name: op.header.array.clone(),
            }));
        };
        if let Err(e) = schema.check_cell(attrs) {
            return Ok(ApplyOutcome::Rejected(ApplyRejection::SchemaViolation {
                detail: e.to_string(),
            }));
        }
    }

    // 5. Dispatch.
    let dispatch_result = match op.kind {
        ArrayOpKind::Put => engine.apply_put(op),
        ArrayOpKind::Delete => engine.apply_delete(op),
        ArrayOpKind::Erase => engine.apply_erase(op),
    };

    if let Err(e) = dispatch_result {
        // Corruption-grade errors propagate; everything else becomes a rejection.
        match &e {
            ArrayError::SegmentCorruption { .. } | ArrayError::HlcLockPoisoned => return Err(e),
            _ => {
                return Ok(ApplyOutcome::Rejected(ApplyRejection::EngineRejected {
                    detail: e.to_string(),
                }));
            }
        }
    }

    // 6. Tile invalidation.
    engine.invalidate_tile(&op.header.array, &op.coord)?;

    // 7. Success.
    Ok(ApplyOutcome::Applied)
}

// ─── MockEngine (test / test-utils only) ────────────────────────────────────

/// Minimal in-memory [`ApplyEngine`] for unit tests.
///
/// Tracks:
/// - known arrays with their schema HLCs
/// - seen `(array, hlc_bytes)` pairs for idempotency
/// - successfully applied ops
/// - an optional injected engine error consumed on the next apply call
/// - a log of invalidated tile coords
#[cfg(test)]
pub struct MockEngine {
    schemas: HashMap<String, (Hlc, ArraySchema)>,
    seen: HashSet<(String, [u8; 18])>,
    pub applied: Vec<ArrayOp>,
    pub invalidated: Vec<(String, Vec<CoordValue>)>,
    reject_next_with: Mutex<Option<ArrayError>>,
}

#[cfg(test)]
impl MockEngine {
    /// Create an empty engine with no registered arrays.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            seen: HashSet::new(),
            applied: Vec::new(),
            invalidated: Vec::new(),
            reject_next_with: Mutex::new(None),
        }
    }

    /// Register an array so that ops targeting it can be applied. Its schema
    /// has one `Int64` dim `k` and one nullable `Float64` attr `v`.
    pub fn register_array(&mut self, array: &str, schema_hlc: Hlc) {
        use crate::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
        use crate::types::domain::{Domain, DomainBound};
        let schema = ArraySchemaBuilder::new(array)
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![16])
            .build()
            .expect("invariant: the mock schema is well formed");
        self.register_array_with_schema(schema_hlc, schema);
    }

    /// Register an array under its own schema.
    pub fn register_array_with_schema(&mut self, schema_hlc: Hlc, schema: ArraySchema) {
        self.schemas
            .insert(schema.name.clone(), (schema_hlc, schema));
    }

    /// Inject an error that will be returned by the next `apply_*` call.
    pub fn set_reject_next(&self, err: ArrayError) {
        *self
            .reject_next_with
            .lock()
            .expect("invariant: MockEngine mutex is not poisoned") = Some(err);
    }

    fn take_inject(&self) -> Option<ArrayError> {
        self.reject_next_with
            .lock()
            .expect("invariant: MockEngine mutex is not poisoned")
            .take()
    }
}

#[cfg(test)]
impl Default for MockEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl ApplyEngine for MockEngine {
    fn schema_hlc(&self, array: &str) -> ArrayResult<Option<Hlc>> {
        Ok(self.schemas.get(array).map(|(hlc, _)| *hlc))
    }

    fn array_schema(&self, array: &str) -> ArrayResult<Option<ArraySchema>> {
        Ok(self.schemas.get(array).map(|(_, schema)| schema.clone()))
    }

    fn already_seen(&self, array: &str, hlc: Hlc) -> ArrayResult<bool> {
        Ok(self.seen.contains(&(array.to_string(), hlc.to_bytes())))
    }

    fn apply_put(&mut self, op: &ArrayOp) -> ArrayResult<()> {
        if let Some(err) = self.take_inject() {
            return Err(err);
        }
        self.seen
            .insert((op.header.array.clone(), op.header.hlc.to_bytes()));
        self.applied.push(op.clone());
        Ok(())
    }

    fn apply_delete(&mut self, op: &ArrayOp) -> ArrayResult<()> {
        if let Some(err) = self.take_inject() {
            return Err(err);
        }
        self.seen
            .insert((op.header.array.clone(), op.header.hlc.to_bytes()));
        self.applied.push(op.clone());
        Ok(())
    }

    fn apply_erase(&mut self, op: &ArrayOp) -> ArrayResult<()> {
        if let Some(err) = self.take_inject() {
            return Err(err);
        }
        self.seen
            .insert((op.header.array.clone(), op.header.hlc.to_bytes()));
        self.applied.push(op.clone());
        Ok(())
    }

    fn invalidate_tile(&mut self, array: &str, coord: &[CoordValue]) -> ArrayResult<()> {
        self.invalidated.push((array.to_string(), coord.to_vec()));
        Ok(())
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::hlc::Hlc;
    use crate::sync::op::{ArrayOpHeader, ArrayOpKind};
    use crate::sync::replica_id::ReplicaId;
    use crate::types::cell_value::value::CellValue;
    use crate::types::coord::value::CoordValue;

    fn replica() -> ReplicaId {
        ReplicaId::new(42)
    }

    fn hlc(ms: u64) -> Hlc {
        Hlc::new(ms, 0, replica()).unwrap()
    }

    fn header(array: &str, op_ms: u64, schema_ms: u64) -> ArrayOpHeader {
        ArrayOpHeader {
            array: array.into(),
            hlc: hlc(op_ms),
            schema_hlc: hlc(schema_ms),
            valid_from_ms: 0,
            valid_until_ms: -1,
            system_from_ms: op_ms as i64,
        }
    }

    fn put_op(array: &str, op_ms: u64, schema_ms: u64) -> ArrayOp {
        ArrayOp {
            header: header(array, op_ms, schema_ms),
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(1)],
            attrs: Some(vec![CellValue::Null]),
        }
    }

    fn delete_op(array: &str, op_ms: u64, schema_ms: u64) -> ArrayOp {
        ArrayOp {
            header: header(array, op_ms, schema_ms),
            kind: ArrayOpKind::Delete,
            coord: vec![CoordValue::Int64(1)],
            attrs: None,
        }
    }

    #[test]
    fn apply_put_succeeds() {
        let mut engine = MockEngine::new();
        engine.register_array("a", hlc(100));

        let op = put_op("a", 50, 100);
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert_eq!(engine.applied.len(), 1);
        assert_eq!(engine.invalidated.len(), 1);
    }

    #[test]
    fn apply_idempotent_on_replay() {
        let mut engine = MockEngine::new();
        engine.register_array("a", hlc(100));

        let op = put_op("a", 50, 100);
        apply_op(&mut engine, &op).unwrap();
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert_eq!(outcome, ApplyOutcome::Idempotent);
        // Still only one applied op.
        assert_eq!(engine.applied.len(), 1);
    }

    #[test]
    fn apply_rejects_unknown_array() {
        let mut engine = MockEngine::new();
        let op = put_op("unknown", 50, 100);
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::ArrayUnknown { name }) if name == "unknown"
        ));
    }

    #[test]
    fn apply_rejects_schema_too_new() {
        let mut engine = MockEngine::new();
        engine.register_array("a", hlc(50)); // local schema at ms=50
        let op = put_op("a", 10, 100); // op schema_hlc at ms=100 > 50
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::SchemaTooNew { local, op: op_hlc })
            if local == hlc(50) && op_hlc == hlc(100)
        ));
    }

    /// Two replicas that stamp a schema in the same millisecond must not have
    /// admission decided by their replica ids. Before `Hlc::is_later_than` the
    /// gate used the full `Ord`, so an op from a replica with a numerically
    /// larger id was rejected as `SchemaTooNew` while the identical op from a
    /// smaller id applied — a coin flip that showed up as a flaky sync test.
    #[test]
    fn same_instant_schema_is_admitted_regardless_of_replica_id() {
        for (local_replica, op_replica) in [(1u64, 9u64), (9, 1)] {
            let local_schema = Hlc::new(50, 0, ReplicaId::new(local_replica)).unwrap();
            let op_schema = Hlc::new(50, 0, ReplicaId::new(op_replica)).unwrap();

            let mut engine = MockEngine::new();
            engine.register_array("a", local_schema);
            let mut op = put_op("a", 10, 100);
            op.header.schema_hlc = op_schema;

            let outcome = apply_op(&mut engine, &op).unwrap();
            assert_eq!(
                outcome,
                ApplyOutcome::Applied,
                "same-instant schema must apply (local replica {local_replica}, \
                 op replica {op_replica}); the replica_id tiebreak must not gate admission"
            );
        }
    }

    #[test]
    fn apply_rejects_invalid_shape() {
        let mut engine = MockEngine::new();
        engine.register_array("a", hlc(100));
        // Put op with no attrs is invalid.
        let op = ArrayOp {
            header: header("a", 50, 100),
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(1)],
            attrs: None,
        };
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::ShapeInvalid { .. })
        ));
    }

    /// A Put whose attr value is of another type than the local schema
    /// declares is refused before the engine sees it.
    #[test]
    fn apply_rejects_a_put_that_breaks_the_schema() {
        use crate::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec, DimType};
        use crate::types::domain::{Domain, DomainBound};
        let mut engine = MockEngine::new();
        let schema = ArraySchemaBuilder::new("scene")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("b1", AttrType::UInt16, false))
            .tile_extents(vec![16])
            .build()
            .unwrap();
        engine.register_array_with_schema(hlc(100), schema);

        let mut wide = put_op("scene", 50, 100);
        wide.attrs = Some(vec![CellValue::Int64(70_000)]);
        let outcome = apply_op(&mut engine, &wide).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::SchemaViolation { .. })
        ));

        let mut null = put_op("scene", 51, 100);
        null.attrs = Some(vec![CellValue::Null]);
        let outcome = apply_op(&mut engine, &null).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::SchemaViolation { .. })
        ));
        assert!(engine.applied.is_empty());

        let mut typed = put_op("scene", 52, 100);
        typed.attrs = Some(vec![CellValue::UInt16(65_535)]);
        assert_eq!(
            apply_op(&mut engine, &typed).unwrap(),
            ApplyOutcome::Applied
        );
    }

    #[test]
    fn apply_wraps_engine_error_as_rejection() {
        let mut engine = MockEngine::new();
        engine.register_array("a", hlc(100));
        engine.set_reject_next(ArrayError::InvalidOp {
            detail: "simulated engine error".into(),
        });
        let op = put_op("a", 50, 100);
        let outcome = apply_op(&mut engine, &op).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(ApplyRejection::EngineRejected { .. })
        ));
        // No tile invalidation on rejection.
        assert!(engine.invalidated.is_empty());
    }

    #[test]
    fn apply_invalidates_tile_after_success() {
        let mut engine = MockEngine::new();
        engine.register_array("b", hlc(100));
        let op = delete_op("b", 30, 50);
        apply_op(&mut engine, &op).unwrap();
        assert_eq!(engine.invalidated.len(), 1);
        assert_eq!(engine.invalidated[0].0, "b");
    }
}
