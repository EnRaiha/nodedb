// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK translation for vector search responses.
//!
//! The Data Plane emits each hit's `id` as the bound `Surrogate.as_u32()`
//! (or the local node id for headless rows) and leaves `doc_id` as
//! `None`. The Control Plane runs this translator at the response
//! boundary so pgwire / HTTP / native clients still see human-readable
//! document identifiers without the engine ever consulting the catalog.
//!
//! Behaviour:
//!  - non-msgpack payloads (already JSON, empty, or non-array) round-
//!    trip unchanged.
//!  - decode failures are non-fatal — the original payload is returned
//!    so the client still sees the raw search hits.

use nodedb_types::DatabaseId;
use nodedb_types::Surrogate;
use nodedb_types::TenantId;
use serde::{Deserialize, Serialize};

use crate::bridge::envelope::PhysicalPlan;
use crate::bridge::scan_filter::ScanFilter;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::VectorOp;

#[derive(Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
struct Hit {
    id: u32,
    distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<Vec<u8>>,
}

/// Apply RLS post-filter at the Control-Plane security boundary, then
/// truncate to `top_k` and strip the body bytes that the Data Plane
/// attached purely for the predicate evaluation. A no-op when
/// `rls_filters` is empty.
fn apply_rls_filter(hits: &mut Vec<Hit>, rls_filters: &[u8], top_k: usize) {
    // When filters are empty the Data Plane never attaches a body, so there
    // is nothing to evaluate or strip — return immediately.
    if rls_filters.is_empty() {
        return;
    }
    let filters: Vec<ScanFilter> = match zerompk::from_msgpack(rls_filters) {
        Ok(f) => f,
        Err(_) => {
            // fail-closed: drop everything if filters are corrupt.
            tracing::warn!("RLS filter decode failed at CP boundary — denying all hits");
            hits.clear();
            return;
        }
    };
    hits.retain(|h| match h.body.as_deref() {
        Some(body) => filters.iter().all(|f| f.matches_binary(body)),
        None => false,
    });
    if hits.len() > top_k {
        hits.truncate(top_k);
    }
    for h in hits.iter_mut() {
        h.body = None;
    }
}

/// Decode the DP-side msgpack array of `VectorSearchHit`, fill each
/// row's `doc_id` from the catalog using `id` as the surrogate, and
/// re-encode. On any decode failure the payload is returned unchanged.
pub fn translate_vector_search_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    rls_filters: &[u8],
    top_k: usize,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }
    let first = payload[0];
    if first == b'[' || first == b'{' || first == b'"' {
        return payload.to_vec();
    }

    let mut hits: Vec<Hit> = match zerompk::from_msgpack(payload) {
        Ok(h) => h,
        Err(_) => return payload.to_vec(),
    };

    apply_rls_filter(&mut hits, rls_filters, top_k);

    if let Some(catalog) = state.credentials.catalog().as_ref() {
        for hit in &mut hits {
            if hit.doc_id.is_some() {
                continue;
            }
            if let Ok(Some(pk_bytes)) = catalog.get_pk_for_surrogate(
                database_id,
                tenant_id,
                collection,
                Surrogate::new(hit.id),
            ) && let Ok(s) = String::from_utf8(pk_bytes)
            {
                hit.doc_id = Some(s);
            }
        }
    }

    // Slow-path column projection: when `body` is a msgpack-encoded payload
    // map, decode it and surface fields alongside `id` / `distance` so client
    // SQL projections like `SELECT id, label, vector_distance(...)` see the
    // payload columns. The base hit fields stay top-level; payload fields are
    // serialized as JSON-style siblings.
    use std::collections::BTreeMap;
    let flattened: Vec<BTreeMap<String, serde_json::Value>> = hits
        .iter()
        .map(|h| {
            let mut obj: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            obj.insert("distance".into(), serde_json::json!(h.distance));
            // Flatten body fields first so the document's own `id` field wins
            // over the internal surrogate integer. Body fields are only present
            // when the Data Plane performed the slow-path fetch.
            if let Some(ref body) = h.body
                && let Ok(map) = zerompk::from_msgpack::<
                    std::collections::HashMap<String, nodedb_types::Value>,
                >(body)
            {
                for (k, v) in map {
                    if let Ok(j) = serde_json::to_value(&v) {
                        obj.insert(k, j);
                    }
                }
            }
            // Fall back to catalog-resolved doc_id or raw surrogate when
            // the body didn't provide an "id" field (e.g. skip_payload_fetch).
            if !obj.contains_key("id") {
                if let Some(ref doc) = h.doc_id {
                    obj.insert("id".into(), serde_json::json!(doc));
                } else {
                    obj.insert("id".into(), serde_json::json!(h.id));
                }
            }
            // Always expose internal surrogate for debugging / join use.
            obj.insert("_surrogate".into(), serde_json::json!(h.id));
            obj
        })
        .collect();
    if let Ok(s) = sonic_rs::to_string(&flattened) {
        return s.into_bytes();
    }
    zerompk::to_msgpack_vec(&hits).unwrap_or_else(|_| payload.to_vec())
}

/// Convenience wrapper: inspect the executed plan; if it produced
/// vector hits, apply surrogate→PK translation. Otherwise return the
/// payload untouched.
pub fn translate_if_vector(
    payload: &[u8],
    plan: &PhysicalPlan,
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
) -> Vec<u8> {
    // The coordinator wraps sharded reads (including vector search) in an
    // `Exchange{Gather}` node; unwrap it so the underlying vector op is visible
    // and surrogate→PK hit translation still runs on the gathered payload.
    if let PhysicalPlan::Query(nodedb_physical::physical_plan::QueryOp::Exchange(op)) = plan {
        return translate_if_vector(payload, &op.child, state, database_id, tenant_id);
    }
    let (collection, rls_filters, top_k): (&str, &[u8], usize) = match plan {
        PhysicalPlan::Vector(VectorOp::Search {
            collection,
            rls_filters,
            top_k,
            ..
        }) => (collection.as_str(), rls_filters.as_slice(), *top_k),
        PhysicalPlan::Vector(VectorOp::MultiSearch {
            collection,
            rls_filters,
            top_k,
            ..
        }) => (collection.as_str(), rls_filters.as_slice(), *top_k),
        PhysicalPlan::Vector(VectorOp::MultiVectorScoreSearch {
            collection, top_k, ..
        }) => (collection.as_str(), &[][..], *top_k),
        _ => return payload.to_vec(),
    };
    translate_vector_search_payload(
        payload,
        state,
        database_id,
        tenant_id,
        collection,
        rls_filters,
        top_k,
    )
}
