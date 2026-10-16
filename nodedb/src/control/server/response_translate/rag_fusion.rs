// SPDX-License-Identifier: BUSL-1.1

//! Surrogate → user-PK translation for `GRAPH RAG FUSION` responses.
//!
//! The Data Plane never consults the catalog. A vector or text hit that is
//! bound to no graph node therefore carries its surrogate's decimal identity
//! (`RowIdentity::for_surrogate`) as its `node_id`, and no `hop_distance`. A
//! node the walk reached carries its own name and a hop distance, seeds
//! included.
//!
//! A result with no `hop_distance` whose `node_id` is the canonical decimal
//! of a `u32` is therefore a document, not a graph node. This translator
//! resolves it through the same [`super::vector::resolve_surrogate_pk`] call
//! the vector, full-text and hybrid translators use, so every search response
//! names its rows by user PK. A result the catalog has no PK for is left
//! untouched: no value is fabricated.

use nodedb_types::{DatabaseId, Surrogate, TenantId};
use serde_json::Value as JsonValue;

use crate::control::state::SharedState;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::vector::resolve_surrogate_pk;

/// Decode the `GraphRagResponse` payload (`{results: [...], metadata: {...}}`)
/// and rewrite the `node_id` of every document result to its user PK.
///
/// The payload is returned byte-for-byte when it does not decode, or when no
/// result needs rewriting. Only a rewritten response is re-encoded, as JSON.
pub fn translate_rag_fusion_payload(
    payload: &[u8],
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> Vec<u8> {
    if payload.is_empty() {
        return payload.to_vec();
    }

    let text = decode_payload_to_json(payload);
    let Ok(mut body) = sonic_rs::from_str::<JsonValue>(&text) else {
        return payload.to_vec();
    };

    let rewritten = rewrite_document_node_ids(&mut body, |surrogate| {
        resolve_surrogate_pk(state, database_id, tenant_id, collection, surrogate)
    });
    if !rewritten {
        return payload.to_vec();
    }

    match sonic_rs::to_string(&body) {
        Ok(s) => s.into_bytes(),
        Err(_) => payload.to_vec(),
    }
}

/// Replace the `node_id` of each document result in `body["results"]` with the
/// PK `resolve` returns for its surrogate. Returns whether any result changed.
fn rewrite_document_node_ids(
    body: &mut JsonValue,
    resolve: impl Fn(Surrogate) -> Option<String>,
) -> bool {
    let Some(JsonValue::Array(results)) = body.get_mut("results") else {
        return false;
    };

    let mut changed = false;
    for result in results {
        let JsonValue::Object(map) = result else {
            continue;
        };
        if matches!(map.get("hop_distance"), Some(hop) if !hop.is_null()) {
            continue;
        }
        let Some(JsonValue::String(node_id)) = map.get("node_id") else {
            continue;
        };
        let Some(surrogate) = parse_decimal_surrogate(node_id) else {
            continue;
        };
        if let Some(pk) = resolve(surrogate) {
            map.insert("node_id".to_owned(), JsonValue::String(pk));
            changed = true;
        }
    }
    changed
}

/// The surrogate a canonical decimal string renders, else `None`. A string
/// `RowIdentity::for_surrogate` cannot produce (leading zero, sign, padding)
/// is not a surrogate rendering.
fn parse_decimal_surrogate(node_id: &str) -> Option<Surrogate> {
    let raw: u32 = node_id.parse().ok()?;
    (raw.to_string() == node_id).then(|| Surrogate::new(raw))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;

    fn resolver(pairs: &[(u32, &str)]) -> impl Fn(Surrogate) -> Option<String> {
        let map: HashMap<u32, String> =
            pairs.iter().map(|(s, pk)| (*s, (*pk).to_owned())).collect();
        move |surrogate| map.get(&surrogate.as_u32()).cloned()
    }

    fn node_ids(body: &JsonValue) -> Vec<String> {
        body["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["node_id"].as_str().unwrap().to_owned())
            .collect()
    }

    #[test]
    fn document_result_gets_its_user_pk() {
        let mut body = json!({
            "results": [{"node_id": "116892", "rrf_score": 0.016, "vector_rank": 0}],
            "metadata": {"graph_expanded": 0}
        });
        assert!(rewrite_document_node_ids(
            &mut body,
            resolver(&[(116892, "lonely")])
        ));
        assert_eq!(node_ids(&body), ["lonely"]);
    }

    #[test]
    fn graph_node_with_a_numeric_name_is_left_alone() {
        // Reached by the walk, so it carries a hop distance and is a graph
        // node even though its name is a decimal.
        let mut body = json!({
            "results": [{"node_id": "42", "rrf_score": 0.5, "hop_distance": 1}]
        });
        assert!(!rewrite_document_node_ids(
            &mut body,
            resolver(&[(42, "other")])
        ));
        assert_eq!(node_ids(&body), ["42"]);
    }

    #[test]
    fn seed_at_hop_zero_is_left_alone() {
        let mut body = json!({
            "results": [{"node_id": "7", "rrf_score": 0.5, "hop_distance": 0}]
        });
        assert!(!rewrite_document_node_ids(
            &mut body,
            resolver(&[(7, "other")])
        ));
        assert_eq!(node_ids(&body), ["7"]);
    }

    #[test]
    fn named_node_without_a_hop_is_left_alone() {
        let mut body = json!({
            "results": [{"node_id": "nd_src . . . insert_edge().", "rrf_score": 0.5}]
        });
        assert!(!rewrite_document_node_ids(&mut body, resolver(&[(1, "x")])));
    }

    #[test]
    fn surrogate_without_a_catalog_pk_is_left_alone() {
        let mut body = json!({"results": [{"node_id": "99", "rrf_score": 0.1}]});
        assert!(!rewrite_document_node_ids(&mut body, resolver(&[])));
        assert_eq!(node_ids(&body), ["99"]);
    }

    #[test]
    fn mixed_results_rewrite_only_the_documents() {
        let mut body = json!({
            "results": [
                {"node_id": "a", "rrf_score": 0.9, "hop_distance": 0},
                {"node_id": "5", "rrf_score": 0.8},
                {"node_id": "b", "rrf_score": 0.7, "hop_distance": 1},
                {"node_id": "6", "rrf_score": 0.6, "hop_distance": null}
            ]
        });
        assert!(rewrite_document_node_ids(
            &mut body,
            resolver(&[(5, "doc5"), (6, "doc6")])
        ));
        assert_eq!(node_ids(&body), ["a", "doc5", "b", "doc6"]);
    }

    #[test]
    fn only_a_canonical_decimal_is_a_surrogate() {
        assert_eq!(parse_decimal_surrogate("0").map(|s| s.as_u32()), Some(0));
        assert_eq!(
            parse_decimal_surrogate("4294967295").map(|s| s.as_u32()),
            Some(u32::MAX)
        );
        for not_a_surrogate in [
            "",
            "007",
            "+7",
            "-7",
            " 7",
            "7 ",
            "4294967296",
            "1.0",
            "0x7",
        ] {
            assert!(
                parse_decimal_surrogate(not_a_surrogate).is_none(),
                "{not_a_surrogate:?} is not a surrogate rendering"
            );
        }
    }

    #[test]
    fn body_without_results_is_untouched() {
        let mut body = json!({"metadata": {}});
        assert!(!rewrite_document_node_ids(&mut body, resolver(&[(1, "x")])));
    }
}
