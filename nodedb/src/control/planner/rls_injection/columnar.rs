// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for the three peer engines sharing the columnar storage
//! core: plain columnar, timeseries, and spatial.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TimeseriesOp};

use super::context::RlsCtx;

/// Exhaustive over [`ColumnarOp`].
pub(super) fn inject_columnar(ctx: &RlsCtx<'_>, op: &mut ColumnarOp) -> crate::Result<()> {
    match op {
        // Inject: the policy occupies the dedicated post-scan slot, applied
        // after block pruning and before rows are returned.
        ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: the clone materializer streams raw `(surrogate, row bytes)`
        // pairs through a cursor payload that carries no row filter.
        ColumnarOp::MaterializeScan { collection, .. } => ctx.refuse_if_policy(
            collection,
            "the materializing scan streams raw stored rows through a cursor payload that carries \
             no row filter",
        ),

        // A plain insert carries every row it will persist, as one MessagePack
        // array of per-row objects, so the write policy decides them here and
        // the first violation fails the statement before anything is
        // dispatched — no row of the batch can already be durable when the
        // refusal happens.
        //
        // The conflict branch persists something the plan does not hold: the
        // stored row with `on_conflict_updates` applied to it, which exists
        // only after the handler has read that stored row. Admitting on the
        // incoming body alone would clear a write whose actual post-image the
        // policy never saw, so the compiled predicate travels with the plan and
        // the handler decides the merged row just before persisting it.
        //
        // The read filter is not redundant with either of those. It gates a
        // different thing: a `RETURNING` clause on this insert ships rows back,
        // and that output is a read, so a row a read-only policy hides must not
        // become visible just because the statement wrote it. The two policies
        // are independent — a collection can carry a `FOR SELECT` policy and no
        // write policy at all, in which case the write is unrestricted and the
        // returned row set still shrinks.
        ColumnarOp::Insert {
            collection,
            payload,
            on_conflict_updates,
            rls_write_check,
            rls_filters,
            ..
        } => {
            if on_conflict_updates.is_empty() {
                ctx.admit_write_batch(collection, payload)?;
            } else {
                ctx.set_write_check(collection, rls_write_check)?;
            }
            ctx.set_post_filters(collection, rls_filters)
        }

        // Ship the write predicate: an update's post-image exists only after
        // the handler has scanned the matching row and applied the assignments,
        // and a delete's image only after it has read the row being removed.
        // The handler evaluates the predicate against those exact rows and
        // rejects the whole statement when one fails.
        ColumnarOp::Update {
            collection,
            rls_write_check,
            ..
        }
        | ColumnarOp::Delete {
            collection,
            rls_write_check,
            ..
        } => ctx.set_write_check(collection, rls_write_check),
    }
}

/// Exhaustive over [`TimeseriesOp`].
pub(super) fn inject_timeseries(ctx: &RlsCtx<'_>, op: &mut TimeseriesOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied after time-range pruning, on the rows
        // the scan actually produced.
        TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Ship the write predicate, for every payload shape without exception.
        //
        // A timeseries row does not exist until the handler has normalized the
        // payload into line protocol and parsed it — and that normalization
        // changes values: a numeric-looking string is stored as a number, and
        // the time column is rewritten into milliseconds under the collection's
        // declared `TIME_KEY`. A structured MessagePack batch is carried in the
        // plan in full, so it could be decided here, but only against the
        // values as SUBMITTED. That is a different image from the one that will
        // be stored, and a policy naming one of those columns would then be
        // decided against a value the collection never holds. So the decision
        // belongs at the one point every format funnels through, after
        // normalization — where it also still fails the whole batch before any
        // row reaches the memtable.
        // The read filter is independent of that write predicate. A `RETURNING`
        // clause on an ingest ships rows back, and that output is a read, so a
        // row a read-only policy hides must not become visible just because the
        // statement wrote it. A collection can carry a `FOR SELECT` policy and
        // no write policy at all, in which case the ingest is unrestricted and
        // only the returned row set shrinks. The raw ILP listener and the
        // Prometheus remote-write endpoint reach this arm too — both run
        // `inject_rls` over their tasks — and both carry no projection, so the
        // filter they receive is simply never consulted.
        TimeseriesOp::Ingest {
            collection,
            rls_write_check,
            rls_filters,
            ..
        } => {
            ctx.set_write_check(collection, rls_write_check)?;
            ctx.set_post_filters(collection, rls_filters)
        }
    }
}

/// Exhaustive over [`SpatialOp`].
pub(super) fn inject_spatial(ctx: &RlsCtx<'_>, op: &mut SpatialOp) -> crate::Result<()> {
    match op {
        // Inject: the policy is applied to the R-tree candidates before they
        // are returned, alongside the query's own attribute filters.
        SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        } => ctx.set_post_filters(collection, rls_filters),

        // Refuse: these carry a geometry and a surrogate and no column values
        // at all, so a predicate naming columns has nothing to test. They are
        // not user SQL — an `INSERT` / `UPDATE` / `DELETE` on a spatial-engine
        // collection routes through `ColumnarOp::*`, which is gated. This is
        // the edge-to-origin sync path for rows already decided by the policy
        // where they are stored, so refusing here loses no user-facing write
        // while keeping the pass from admitting an image it cannot see.
        SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. } => ctx
            .refuse_if_write_policy(
                collection,
                "the R-tree entry carries a geometry and a surrogate rather than the column values \
                 a policy predicate names",
            ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        ColumnarInsertIntent, ColumnarOp, SpatialOp, TimeseriesOp, UpdateValue,
    };

    use super::super::plan::test_support::{
        assert_write_refused, inject, inject_without_policy, store_with_read_policy,
        store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;

    /// `alice` in the shared fixture has user id 42, so a row carrying
    /// `owner_id = "42"` satisfies `owner_id = $auth.id` and any other does not.
    fn rows(owner_ids: &[&str]) -> Vec<u8> {
        let rows: Vec<serde_json::Value> = owner_ids
            .iter()
            .map(|owner| serde_json::json!({ "owner_id": owner, "amount": 100 }))
            .collect();
        nodedb_types::json_to_msgpack_or_empty(&serde_json::Value::Array(rows))
    }

    fn columnar_insert(collection: &str, owner_ids: &[&str]) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: collection.into(),
            payload: rows(owner_ids),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    fn columnar_update(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: collection.into(),
            filters: Vec::new(),
            updates: Vec::new(),
            rls_write_check: Vec::new(),
        })
    }

    fn ingest(collection: &str, format: &str) -> PhysicalPlan {
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.into(),
            payload: Vec::new(),
            format: format.into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        })
    }

    /// The compiled write predicate a plan carries into the Data Plane.
    fn write_check(plan: &PhysicalPlan) -> &[u8] {
        match plan {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Update {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                rls_write_check, ..
            }) => rls_write_check,
            other => panic!("plan shape changed: {other:?}"),
        }
    }

    /// The plan carries every row a plain insert will persist, so the policy
    /// decides them at plan time: a conforming batch is admitted and a batch
    /// holding one violating row fails the whole statement.
    #[test]
    fn columnar_insert_is_admitted_or_rejected_on_its_own_rows() {
        let store = store_with_write_policy("events");

        let mut conforming = columnar_insert("events", &["42", "42"]);
        assert!(inject(&mut conforming, &store).is_ok());

        let mut violating = columnar_insert("events", &["42", "99"]);
        assert!(matches!(
            inject(&mut violating, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// A payload that is not a decodable row batch fails closed rather than
    /// being waved through as "not rows".
    #[test]
    fn an_undecodable_insert_payload_is_rejected_under_a_write_policy() {
        let store = store_with_write_policy("events");
        let mut plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "events".into(),
            payload: vec![0xC1],
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// The merged row an ON CONFLICT DO UPDATE stores exists only in the
    /// handler, so the predicate travels with the plan instead of the incoming
    /// body being admitted in its place.
    #[test]
    fn on_conflict_update_carries_the_write_predicate() {
        let store = store_with_write_policy("events");
        let mut plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "events".into(),
            payload: rows(&["42"]),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Put,
            on_conflict_updates: vec![("amount".into(), UpdateValue::Literal(Vec::new()))],
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(inject(&mut plan, &store).is_ok());
        assert!(
            !write_check(&plan).is_empty(),
            "write policy must reach the Data-Plane gate"
        );
    }

    /// An update's post-image and a delete's pre-image both exist only inside
    /// the handler, so both ship the compiled predicate.
    #[test]
    fn columnar_update_and_delete_carry_the_write_predicate() {
        let store = store_with_write_policy("events");

        let mut update = columnar_update("events");
        assert!(inject(&mut update, &store).is_ok());
        assert!(!write_check(&update).is_empty());

        let mut delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: "events".into(),
            filters: Vec::new(),
            rls_write_check: Vec::new(),
        });
        assert!(inject(&mut delete, &store).is_ok());
        assert!(!write_check(&delete).is_empty());
    }

    /// A structured MessagePack batch is NOT decided here even though the plan
    /// carries its rows: the ingest handler retypes those values before storing
    /// them, so a plan-time decision would judge an image the collection never
    /// holds. The predicate ships instead, and the one gate after normalization
    /// decides the stored rows.
    #[test]
    fn timeseries_msgpack_ingest_carries_the_write_predicate_rather_than_deciding_here() {
        let store = store_with_write_policy("metrics");
        let mut plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: "metrics".into(),
            payload: rows(&["99"]),
            format: "msgpack".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        assert!(
            inject(&mut plan, &store).is_ok(),
            "the violating row must be left for the Data-Plane gate, not refused here"
        );
        assert!(
            !write_check(&plan).is_empty(),
            "the predicate must reach the gate that sees the stored image"
        );
    }

    /// Every payload shape carries the predicate to the handler's per-row gate.
    #[test]
    fn timeseries_ilp_ingest_carries_the_write_predicate() {
        let store = store_with_write_policy("metrics");
        for format in ["ilp", "ilp-msgpack", "json"] {
            let mut plan = ingest("metrics", format);
            assert!(inject(&mut plan, &store).is_ok());
            assert!(
                !write_check(&plan).is_empty(),
                "{format} ingest must carry the predicate to the gate"
            );
        }
    }

    /// …and runs untouched when no policy applies.
    #[test]
    fn timeseries_ingest_without_a_policy_is_untouched() {
        let mut plan = ingest("metrics", "ilp");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A read policy alone must not start rejecting or gating writes.
    #[test]
    fn a_read_policy_alone_leaves_the_columnar_write_gate_empty() {
        let store = store_with_read_policy("events");

        let mut insert = columnar_insert("events", &["99"]);
        assert!(inject(&mut insert, &store).is_ok());

        let mut update = columnar_update("events");
        assert!(inject(&mut update, &store).is_ok());
        assert!(write_check(&update).is_empty());
    }

    /// A spatial write carries geometry and a surrogate, not the row body the
    /// policy names.
    #[test]
    fn spatial_delete_is_refused_under_a_write_policy() {
        let store = store_with_write_policy("places");
        let mut plan = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: "places".into(),
            field: "geom".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            provenance: None,
        });
        assert_write_refused(inject(&mut plan, &store), "places");
    }
}
