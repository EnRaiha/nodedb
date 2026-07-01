// SPDX-License-Identifier: Apache-2.0

//! Executor parity contract for [`SqlPlan`]: one abstract method per variant.
//! Trait method arity mirrors `SqlPlan` variant field counts and is not a code smell.
#![allow(clippy::too_many_arguments)]

use crate::fts_types::FtsQuery;
use crate::temporal::TemporalScope;
use crate::types::SqlPlan;
use crate::types::filter::Filter;
use crate::types::plan::{
    ArrayPrefilter, KvInsertIntent, MergePlanClause, VectorAnnOptions, VectorPrimaryRow,
};
use crate::types::query::{
    AggregateExpr, EngineType, JoinType, Projection, SortKey, SpatialPredicate, WindowSpec,
};
use crate::types_array::{
    ArrayAttrAst, ArrayBinaryOpAst, ArrayCellOrderAst, ArrayCoordLiteral, ArrayDimAst,
    ArrayInsertRow, ArrayReducerAst, ArraySliceAst, ArrayTileOrderAst,
};
use crate::types_expr::{SqlExpr, SqlPayloadAtom, SqlValue};
use nodedb_types::PayloadIndexKind;
use nodedb_types::VectorQuantization;
use nodedb_types::vector_distance::DistanceMetric;

/// Executor parity contract: every [`SqlPlan`] variant must be handled.
/// Implement this trait and call [`dispatch`](super::dispatch) to route plans.
pub trait PlanVisitor {
    /// The successful result type returned by each visit method.
    type Output;
    /// The error type returned by each visit method.
    type Error;

    /// Handle [`SqlPlan::ConstantResult`].
    fn constant_result(
        &mut self,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Scan`].
    fn scan(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine: EngineType,
        filters: &[Filter],
        projection: &[Projection],
        sort_keys: &[SortKey],
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: &[WindowSpec],
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::PointGet`].
    fn point_get(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine: EngineType,
        key_column: &str,
        key_value: &SqlValue,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DocumentIndexLookup`].
    fn document_index_lookup(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine: EngineType,
        field: &str,
        value: &SqlValue,
        filters: &[Filter],
        projection: &[Projection],
        sort_keys: &[SortKey],
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: &[WindowSpec],
        case_insensitive: bool,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RangeScan`].
    fn range_scan(
        &mut self,
        collection: &str,
        field: &str,
        lower: Option<&SqlValue>,
        upper: Option<&SqlValue>,
        limit: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Insert`].
    fn insert(
        &mut self,
        collection: &str,
        engine: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        column_defaults: &[(String, String)],
        if_absent: bool,
        column_schema: &[(String, String)],
        primary_key: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::KvInsert`].
    fn kv_insert(
        &mut self,
        collection: &str,
        entries: &[(SqlValue, Vec<(String, SqlValue)>)],
        ttl_secs: u64,
        intent: KvInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Upsert`].
    fn upsert(
        &mut self,
        collection: &str,
        engine: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        column_defaults: &[(String, String)],
        on_conflict_updates: &[(String, SqlExpr)],
        column_schema: &[(String, String)],
        primary_key: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::InsertSelect`].
    fn insert_select(
        &mut self,
        target: &str,
        source: &SqlPlan,
        limit: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Update`].
    fn update(
        &mut self,
        collection: &str,
        engine: EngineType,
        assignments: &[(String, SqlExpr)],
        filters: &[Filter],
        target_keys: &[SqlValue],
        returning: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::UpdateFrom`].
    fn update_from(
        &mut self,
        collection: &str,
        engine: EngineType,
        source: &SqlPlan,
        target_join_col: &str,
        source_join_col: &str,
        assignments: &[(String, SqlExpr)],
        target_filters: &[Filter],
        returning: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Delete`].
    fn delete(
        &mut self,
        collection: &str,
        engine: EngineType,
        filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Truncate`].
    fn truncate(
        &mut self,
        collection: &str,
        restart_identity: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Join`].
    ///
    /// `limit` is `None` when the join carries no SQL `LIMIT` clause (output
    /// bounded downstream by the memory byte budget) and `Some(n)` for an
    /// explicit `LIMIT n`.
    fn join(
        &mut self,
        left: &SqlPlan,
        right: &SqlPlan,
        on: &[(String, String)],
        join_type: JoinType,
        condition: Option<&SqlExpr>,
        limit: Option<usize>,
        projection: &[Projection],
        filters: &[Filter],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Aggregate`].
    fn aggregate(
        &mut self,
        input: &SqlPlan,
        group_by: &[SqlExpr],
        aggregates: &[AggregateExpr],
        having: &[Filter],
        limit: usize,
        grouping_sets: Option<&[Vec<usize>]>,
        sort_keys: &[SortKey],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TimeseriesScan`].
    fn timeseries_scan(
        &mut self,
        collection: &str,
        time_range: (i64, i64),
        bucket_interval_ms: i64,
        group_by: &[String],
        aggregates: &[AggregateExpr],
        filters: &[Filter],
        projection: &[Projection],
        gap_fill: &str,
        limit: usize,
        tiered: bool,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TimeseriesIngest`].
    fn timeseries_ingest(
        &mut self,
        collection: &str,
        rows: &[Vec<(String, SqlValue)>],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorSearch`].
    fn vector_search(
        &mut self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: DistanceMetric,
        filters: &[Filter],
        array_prefilter: Option<&ArrayPrefilter>,
        ann_options: &VectorAnnOptions,
        skip_payload_fetch: bool,
        payload_filters: &[SqlPayloadAtom],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::MultiVectorSearch`].
    fn multi_vector_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::TextSearch`].
    fn text_search(
        &mut self,
        collection: &str,
        query: &FtsQuery,
        top_k: usize,
        filters: &[Filter],
        score_alias: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::HybridSearch`].
    fn hybrid_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        ef_search: usize,
        vector_weight: f32,
        fuzzy: bool,
        score_alias: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::HybridSearchTriple`].
    fn hybrid_search_triple(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        graph_seed_id: &str,
        graph_depth: usize,
        graph_edge_label: Option<&str>,
        top_k: usize,
        ef_search: usize,
        fuzzy: bool,
        rrf_k: (f64, f64, f64),
        score_alias: Option<&str>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::SpatialScan`].
    fn spatial_scan(
        &mut self,
        collection: &str,
        field: &str,
        predicate: &SpatialPredicate,
        query_geometry: &nodedb_types::geometry::Geometry,
        distance_meters: f64,
        attribute_filters: &[Filter],
        limit: usize,
        projection: &[Projection],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Union`].
    fn union(&mut self, inputs: &[SqlPlan], distinct: bool) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Intersect`].
    fn intersect(
        &mut self,
        left: &SqlPlan,
        right: &SqlPlan,
        all: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Except`].
    fn except(
        &mut self,
        left: &SqlPlan,
        right: &SqlPlan,
        all: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RecursiveScan`].
    fn recursive_scan(
        &mut self,
        collection: &str,
        base_filters: &[Filter],
        recursive_filters: &[Filter],
        join_link: Option<&(String, String)>,
        max_iterations: usize,
        distinct: bool,
        limit: usize,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::RecursiveValue`].
    fn recursive_value(
        &mut self,
        cte_name: &str,
        columns: &[String],
        init_exprs: &[String],
        step_exprs: &[String],
        condition: Option<&str>,
        max_depth: usize,
        distinct: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Cte`].
    fn cte(
        &mut self,
        definitions: &[(String, SqlPlan)],
        outer: &SqlPlan,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::CreateArray`].
    fn create_array(
        &mut self,
        name: &str,
        dims: &[ArrayDimAst],
        attrs: &[ArrayAttrAst],
        tile_extents: &[i64],
        cell_order: ArrayCellOrderAst,
        tile_order: ArrayTileOrderAst,
        prefix_bits: u8,
        audit_retain_ms: Option<u64>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DropArray`].
    fn drop_array(&mut self, name: &str, if_exists: bool) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::AlterArray`].
    fn alter_array(
        &mut self,
        name: &str,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::InsertArray`].
    fn insert_array(
        &mut self,
        name: &str,
        rows: &[ArrayInsertRow],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DeleteArray`].
    fn delete_array(
        &mut self,
        name: &str,
        coords: &[Vec<ArrayCoordLiteral>],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArraySlice`].
    fn array_slice(
        &mut self,
        name: &str,
        slice: &ArraySliceAst,
        attr_projection: &[String],
        limit: u32,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayProject`].
    fn array_project(
        &mut self,
        name: &str,
        attr_projection: &[String],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayAgg`].
    fn array_agg(
        &mut self,
        name: &str,
        attr: &str,
        reducer: &ArrayReducerAst,
        group_by_dim: Option<&str>,
        temporal: &TemporalScope,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayElementwise`].
    fn array_elementwise(
        &mut self,
        left: &str,
        right: &str,
        op: ArrayBinaryOpAst,
        attr: &str,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayFlush`].
    fn array_flush(&mut self, name: &str) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::ArrayCompact`].
    fn array_compact(&mut self, name: &str) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::Merge`].
    fn merge(
        &mut self,
        target: &str,
        engine: EngineType,
        source: &SqlPlan,
        target_join_col: &str,
        source_join_col: &str,
        source_alias: &str,
        clauses: &[MergePlanClause],
        returning: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::LateralTopK`].
    fn lateral_top_k(
        &mut self,
        outer: &SqlPlan,
        outer_alias: Option<&str>,
        inner_collection: &str,
        inner_filters: &[Filter],
        inner_order_by: &[SortKey],
        inner_limit: usize,
        correlation_keys: &[(String, String)],
        lateral_alias: &str,
        projection: &[Projection],
        left_join: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::LateralLoop`].
    fn lateral_loop(
        &mut self,
        outer: &SqlPlan,
        outer_alias: Option<&str>,
        inner: &SqlPlan,
        correlation_predicates: &[(String, String)],
        lateral_alias: &str,
        projection: &[Projection],
        outer_row_cap: usize,
        left_join: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::VectorPrimaryInsert`].
    fn vector_primary_insert(
        &mut self,
        collection: &str,
        field: &str,
        quantization: &VectorQuantization,
        storage_dtype: &nodedb_types::VectorStorageDtype,
        payload_indexes: &[(String, PayloadIndexKind)],
        rows: &[VectorPrimaryRow],
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::CreateIndex`].
    fn create_index(
        &mut self,
        index_name: Option<&str>,
        collection: &str,
        field: &str,
        unique: bool,
        if_not_exists: bool,
        case_insensitive: bool,
    ) -> Result<Self::Output, Self::Error>;

    /// Handle [`SqlPlan::DropIndex`].
    fn drop_index(
        &mut self,
        index_name: &str,
        collection: Option<&str>,
        if_exists: bool,
    ) -> Result<Self::Output, Self::Error>;
}
