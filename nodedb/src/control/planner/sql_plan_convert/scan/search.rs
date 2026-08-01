// SPDX-License-Identifier: BUSL-1.1

//! Vector / text / hybrid search converters and the array-prefilter plan
//! builder shared across them.

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::*;

use super::super::filter::serialize_filters;
use super::super::scan_params::{
    HybridSearchParams, HybridSearchTripleParams, SparseSearchParams, VectorSearchParams,
};
use super::super::value::sql_value_to_nodedb_value as sql_value_to_value;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

pub(in crate::control::planner::sql_plan_convert) fn convert_vector_search(
    p: VectorSearchParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(p.ctx.database_id, p.collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(p.ctx.database_id, collection);
    let filter_bytes = serialize_filters(p.filters)?;
    let inline_prefilter_plan = match p.array_prefilter {
        Some(pref) => Some(Box::new(build_array_prefilter_plan(
            pref,
            p.tenant_id,
            p.ctx,
        )?)),
        None => None,
    };
    let ann_options = p.ann_options.to_runtime();
    let payload_filters: Vec<nodedb_types::PayloadAtom> = p
        .payload_filters
        .iter()
        .map(sql_atom_to_value_atom)
        .collect();
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        database_id: p.ctx.database_id,
        plan: PhysicalPlan::Vector(VectorOp::Search {
            collection: collection.into(),
            query_vector: p.query_vector.to_vec(),
            top_k: *p.top_k,
            ef_search: *p.ef_search,
            metric: *p.metric,
            filter_bitmap: None,
            field_name: p.field.to_string(),
            rls_filters: filter_bytes,
            inline_prefilter_plan,
            ann_options,
            skip_payload_fetch: p.skip_payload_fetch,
            payload_filters,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_sparse_search(
    p: SparseSearchParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let coll_qualified = super::super::convert::db_qualified(p.database_id, p.collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(p.database_id, collection);
    Ok(vec![PhysicalTask {
        tenant_id: p.tenant_id,
        vshard_id: vshard,
        database_id: p.database_id,
        plan: PhysicalPlan::Vector(VectorOp::SparseSearch {
            collection: collection.into(),
            field_name: p.field.to_string(),
            query_entries: p.query_entries.to_vec(),
            top_k: *p.top_k,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

fn sql_atom_to_value_atom(a: &nodedb_sql::types::SqlPayloadAtom) -> nodedb_types::PayloadAtom {
    use nodedb_sql::types::SqlPayloadAtom;
    match a {
        SqlPayloadAtom::Eq(f, v) => nodedb_types::PayloadAtom::Eq(f.clone(), sql_value_to_value(v)),
        SqlPayloadAtom::In(f, vs) => {
            nodedb_types::PayloadAtom::In(f.clone(), vs.iter().map(sql_value_to_value).collect())
        }
        SqlPayloadAtom::Range {
            field,
            low,
            low_inclusive,
            high,
            high_inclusive,
        } => nodedb_types::PayloadAtom::Range {
            field: field.clone(),
            low: low.as_ref().map(sql_value_to_value),
            low_inclusive: *low_inclusive,
            high: high.as_ref().map(sql_value_to_value),
            high_inclusive: *high_inclusive,
        },
    }
}

/// Lower an `ArrayPrefilter` (array name + slice AST) into the
/// `ArrayOp::SurrogateBitmapScan` sub-plan that the vector search handler
/// runs as its `inline_prefilter_plan`.
fn build_array_prefilter_plan(
    prefilter: &nodedb_sql::types::ArrayPrefilter,
    tenant_id: TenantId,
    ctx: &super::super::convert::ConvertContext,
) -> crate::Result<PhysicalPlan> {
    use nodedb_array::query::slice::{DimRange, Slice};
    use nodedb_array::schema::ArraySchema;
    use nodedb_array::types::ArrayId;

    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "array prefilter: no array catalog wired into convert context".into(),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, &prefilter.array_name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!(
                    "array prefilter: array '{}' not found",
                    prefilter.array_name
                ),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let mut dim_ranges: Vec<Option<DimRange>> = vec![None; schema.dims.len()];
    for r in &prefilter.slice.dim_ranges {
        let idx = schema
            .dims
            .iter()
            .position(|d| d.name == r.dim)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!(
                    "array prefilter: array '{}' has no dim '{}'",
                    prefilter.array_name, r.dim
                ),
            })?;
        let dtype = schema.dims[idx].dtype;
        let lo = super::super::array_fn_convert::helpers::coerce_bound(&r.lo, dtype, &r.dim)?;
        let hi = super::super::array_fn_convert::helpers::coerce_bound(&r.hi, dtype, &r.dim)?;
        dim_ranges[idx] = Some(DimRange::new(lo, hi));
    }
    let slice = Slice::new(dim_ranges);
    let slice_msgpack =
        zerompk::to_msgpack_vec(&slice).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array slice encode: {e}"),
        })?;

    let aid = ArrayId::in_database(tenant_id, ctx.database_id, &prefilter.array_name);
    Ok(PhysicalPlan::Array(
        nodedb_physical::physical_plan::ArrayOp::SurrogateBitmapScan {
            array_id: aid,
            slice_msgpack,
        },
    ))
}

pub(in crate::control::planner::sql_plan_convert) fn convert_text_search(
    collection: &str,
    query: &nodedb_sql::fts_types::FtsQuery,
    top_k: &usize,
    score_alias: Option<&str>,
    tenant_id: TenantId,
    database_id: crate::types::DatabaseId,
) -> crate::Result<Vec<PhysicalTask>> {
    use nodedb_sql::fts_types::FtsQuery;

    let coll_qualified = super::super::convert::db_qualified(database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(database_id, collection);

    // Phrase queries emit a dedicated PhraseSearch op rather than going
    // through the BM25 plain-string path. Score alias is not meaningful
    // for phrase search (no per-row score injection), so it is ignored.
    if let FtsQuery::Phrase(terms) = query {
        let analyzed_terms: Vec<String> =
            terms.iter().flat_map(|t| nodedb_fts::analyze(t)).collect();
        if analyzed_terms.is_empty() {
            // No searchable terms after analysis — return empty result via
            // a standard search that will match nothing.
            return Ok(vec![PhysicalTask {
                tenant_id,
                vshard_id: vshard,
                database_id,
                plan: PhysicalPlan::Text(TextOp::Search {
                    collection: collection.into(),
                    query: String::new(),
                    top_k: *top_k,
                    fuzzy: false,
                    prefilter: None,
                    rls_filters: Vec::new(),
                }),
                post_set_op: PostSetOp::None,
                txn_id: None,
            }]);
        }
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id,
            plan: PhysicalPlan::Text(TextOp::PhraseSearch {
                collection: collection.into(),
                terms: analyzed_terms,
                top_k: *top_k,
                prefilter: None,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    let query_str = query
        .to_plain_string()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "unsupported FTS query form; use plain terms, AND/OR combinations, \
                     or phrase queries with text_match(field, '\"phrase here\"')"
                .into(),
        })?;
    let fuzzy = query.is_fuzzy();

    // When a score alias is present the caller wants a full-collection scan
    // with BM25 scores injected per row (all rows appear, non-matching rows
    // receive a null score). Emit `BM25ScoreScan` for that shape; emit the
    // hit-only `Search` for the WHERE `text_match(...)` shape.
    let op = if let Some(alias) = score_alias {
        TextOp::BM25ScoreScan {
            collection: collection.into(),
            query: query_str,
            score_alias: alias.to_string(),
            fuzzy,
        }
    } else {
        TextOp::Search {
            collection: collection.into(),
            query: query_str,
            top_k: *top_k,
            fuzzy,
            prefilter: None,
            rls_filters: Vec::new(),
        }
    };

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Text(op),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_hybrid_search(
    p: HybridSearchParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let HybridSearchParams {
        collection,
        query_vector,
        query_text,
        top_k,
        ef_search,
        vector_weight,
        fuzzy,
        score_alias,
        tenant_id,
        database_id,
    } = p;
    let coll_qualified = super::super::convert::db_qualified(database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(database_id, collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Text(TextOp::HybridSearch {
            collection: collection.into(),
            query_vector: query_vector.to_vec(),
            query_text: query_text.to_string(),
            top_k: *top_k,
            ef_search: *ef_search,
            fuzzy: *fuzzy,
            vector_weight: *vector_weight,
            filter_bitmap: None,
            rls_filters: Vec::new(),
            score_alias: score_alias.map(|s| s.to_string()),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in crate::control::planner::sql_plan_convert) fn convert_hybrid_search_triple(
    p: HybridSearchTripleParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let HybridSearchTripleParams {
        collection,
        query_vector,
        query_text,
        graph_seed_id,
        graph_depth,
        graph_edge_label,
        top_k,
        ef_search,
        fuzzy,
        rrf_k,
        score_alias,
        tenant_id,
        database_id,
    } = p;
    let coll_qualified = super::super::convert::db_qualified(database_id, collection);
    let collection = coll_qualified.as_str();
    let vshard = VShardId::from_collection_in_database(database_id, collection);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan: PhysicalPlan::Text(TextOp::HybridSearchTriple {
            collection: collection.into(),
            query_vector: query_vector.to_vec(),
            query_text: query_text.to_string(),
            graph_seed_id: graph_seed_id.to_string(),
            graph_depth: *graph_depth,
            graph_edge_label: graph_edge_label.clone(),
            top_k: *top_k,
            ef_search: *ef_search,
            fuzzy: *fuzzy,
            rrf_k: *rrf_k,
            filter_bitmap: None,
            rls_filters: Vec::new(),
            score_alias: score_alias.map(|s| s.to_string()),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
