// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive accessors for the RLS write check(s) a [`PhysicalPlan`] carries.
//!
//! A write plan is built first, then RLS injection runs over it and turns
//! any [`nodedb_types::RlsWriteCheck::PendingInjection`] into a real
//! decision. A plan that still carries `PendingInjection` when it reaches
//! the Data Plane means injection was skipped. These accessors let a guard
//! check every check slot on a plan, so a new op with its own check field
//! cannot be added without this match being updated too.

use nodedb_types::RlsWriteCheck;

use super::PhysicalPlan;
use super::columnar::ColumnarOp;
use super::document::DocumentOp;
use super::graph::GraphOp;
use super::kv::KvOp;
use super::timeseries::TimeseriesOp;

impl PhysicalPlan {
    /// The single RLS write check this plan carries, or `None` if it carries
    /// no check or more than one.
    ///
    /// The name says `sole` because `KvOp::TransferItem` carries TWO checks
    /// and returns `None` here. A caller that must see every check — any
    /// safety gate — wants [`PhysicalPlan::rls_write_checks`] instead, or it
    /// will wave that op through without looking at either slot.
    pub fn sole_rls_write_check(&self) -> Option<&RlsWriteCheck> {
        match self {
            PhysicalPlan::Columnar(ColumnarOp::Insert {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Update {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Columnar(ColumnarOp::ResolveDml {
                rls_write_check, ..
            })
            | PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                rls_write_check, ..
            })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::PointDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::PointUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::BulkDelete {
                rls_write_check, ..
            })
            | PhysicalPlan::Document(DocumentOp::Merge {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Delete {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Expire {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Persist {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::FieldSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Incr {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::IncrFloat {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Cas {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::GetSet {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::Transfer {
                rls_write_check, ..
            })
            | PhysicalPlan::Kv(KvOp::ResolvedWrite {
                rls_write_check, ..
            }) => Some(rls_write_check),

            // Carries two checks, not one — see `rls_write_checks`.
            PhysicalPlan::Kv(KvOp::TransferItem { .. }) => None,

            // Read-only, and carries no check slot of its own: the wrapped op
            // holds the live predicate, and the resolve handler decides it
            // there against the images it computes.
            PhysicalPlan::Kv(KvOp::ResolveWrite(_)) => None,

            // Every other op is not write-class: it carries no
            // `rls_write_check` field at all.
            PhysicalPlan::Vector(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Document(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => None,
        }
    }

    /// Every RLS write check this plan carries.
    ///
    /// Most write-class ops carry exactly one check, so this returns the
    /// same single check as [`PhysicalPlan::rls_write_check`]. `KvOp::
    /// TransferItem` carries two — the source collection's check and the
    /// destination collection's check — and both come back here. A
    /// non-write plan returns an empty `Vec`.
    ///
    /// Use this, not `sole_rls_write_check`, whenever every check slot on the
    /// plan must be checked — for example the un-injected-write guard at
    /// the Data Plane dispatch boundary.
    pub fn rls_write_checks(&self) -> Vec<&RlsWriteCheck> {
        if let PhysicalPlan::Kv(KvOp::TransferItem {
            source_rls_write_check,
            dest_rls_write_check,
            ..
        }) = self
        {
            return vec![source_rls_write_check, dest_rls_write_check];
        }
        // Every other write-class op carries exactly one check; every
        // non-write op carries none. `rls_write_check` is itself an
        // exhaustive match over `PhysicalPlan`, so this stays exhaustive
        // too rather than falling back on a wildcard here.
        self.sole_rls_write_check().into_iter().collect()
    }
}
