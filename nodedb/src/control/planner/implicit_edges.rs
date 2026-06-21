// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane implicit graph-edge extraction.
//!
//! A schemaless document carrying the reserved `_from` / `_to` (and optional
//! `_type` / `weight`) fields is mirrored as a graph edge so `MATCH ...` and
//! `GRAPH ALGO ...` see edges inserted via plain document `INSERT`.
//!
//! This extraction runs on the Control Plane — BEFORE dispatch classification —
//! so an implicit edge routes through the SAME path as an explicit
//! `GRAPH INSERT EDGE` (`GraphOp::EdgePut`): single-home Raft dispatch when src
//! and dst share a home vShard, Calvin dual-home when they straddle a shard
//! boundary. The earlier Data-Plane hook homed the edge by the *document's*
//! vShard, which is wrong for cross-shard edges; resolving each endpoint's home
//! vShard and canonical surrogate here makes implicit edges identical to
//! explicit ones.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). `assign_surrogate_routed`
//! performs Control-Plane RPC I/O only — no storage I/O, no io_uring, no
//! Data-Plane access from this module.

use memchr::memmem;
use nodedb_physical::physical_plan::{DocumentOp, GraphOp, PhysicalPlan};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use crate::control::server::surrogate_exchange::assign_surrogate_routed;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};

/// Default edge label when a document omits `_type`. Mirrors the historical
/// Data-Plane `maybe_register_edge` default.
const DEFAULT_EDGE_LABEL: &str = "edge";

/// One implicit edge extracted from a document write.
struct ImplicitEdge {
    collection: String,
    src: String,
    dst: String,
    label: String,
    /// `Some(w)` when the document carried a finite numeric `weight`.
    weight: Option<f64>,
}

/// Scan the current document-write tasks for `_from` / `_to` documents and
/// append a `GraphOp::EdgePut` task per implicit edge.
///
/// Each appended task is built exactly like an explicit `GRAPH INSERT EDGE`:
/// the edge is homed on `from_key(_from)` with both endpoints' canonical
/// surrogates resolved via the routed surrogate exchange, so the downstream
/// classify/Calvin/single-shard logic dual-homes cross-shard edges and
/// single-homes same-shard edges identically to explicit edges.
pub async fn append_implicit_edge_tasks(
    state: &SharedState,
    tasks: &mut Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<()> {
    // Collect a SNAPSHOT of candidate edges first so the immutable scan of
    // `tasks` does not borrow-conflict with the `&mut Vec` we push into below.
    let mut edges: Vec<ImplicitEdge> = Vec::new();
    for task in tasks.iter() {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection, value, ..
            })
            | PhysicalPlan::Document(DocumentOp::Upsert {
                collection, value, ..
            }) => {
                if let Some(edge) = extract_edge(collection, value) {
                    edges.push(edge);
                }
            }
            PhysicalPlan::Document(DocumentOp::BatchInsert {
                collection,
                documents,
                ..
            }) => {
                for (_doc_id, value) in documents {
                    if let Some(edge) = extract_edge(collection, value) {
                        edges.push(edge);
                    }
                }
            }
            // Every other plan (other DocumentOp variants, and non-Document
            // plans) carries no implicit edge — intentionally skipped.
            _ => {}
        }
    }

    for edge in edges {
        let vsrc = VShardId::from_key(edge.src.as_bytes());
        let vdst = VShardId::from_key(edge.dst.as_bytes());

        let src_surrogate = assign_surrogate_routed(
            state,
            vsrc,
            database_id,
            tenant_id,
            &edge.collection,
            edge.src.as_bytes(),
            trace_id,
        )
        .await?;
        let dst_surrogate = assign_surrogate_routed(
            state,
            vdst,
            database_id,
            tenant_id,
            &edge.collection,
            edge.dst.as_bytes(),
            trace_id,
        )
        .await?;

        let properties = match edge.weight {
            Some(w) => weight_properties(w),
            None => Vec::new(),
        };

        tasks.push(PhysicalTask {
            tenant_id,
            vshard_id: vsrc,
            database_id,
            plan: PhysicalPlan::Graph(GraphOp::EdgePut {
                collection: edge.collection,
                src_id: edge.src,
                label: edge.label,
                dst_id: edge.dst,
                properties,
                src_surrogate,
                dst_surrogate,
            }),
            post_set_op: PostSetOp::None,
        });
    }

    Ok(())
}

/// Decode a standard-msgpack document `value` and extract an implicit edge
/// when it carries `_from` and `_to` string fields.
///
/// A cheap byte pre-filter skips the msgpack decode for the overwhelming
/// majority of documents that are not edges. `_type` defaults to `"edge"`;
/// `weight` is carried only when present and finite.
fn extract_edge(collection: &str, value: &[u8]) -> Option<ImplicitEdge> {
    // Pre-filter: an edge document's msgpack always contains the literal key
    // bytes `_from`. Avoid decoding non-edge documents on the hot path.
    memmem::find(value, b"_from")?;

    let decoded = rmpv::decode::read_value(&mut &value[..]).ok()?;
    let rmpv::Value::Map(entries) = decoded else {
        return None;
    };

    let mut src: Option<String> = None;
    let mut dst: Option<String> = None;
    let mut label: Option<String> = None;
    let mut weight: Option<f64> = None;
    for (k, v) in &entries {
        let key = match k {
            rmpv::Value::String(s) => match s.as_str() {
                Some(s) => s,
                None => continue,
            },
            _ => continue,
        };
        match key {
            "_from" => src = v.as_str().map(str::to_string),
            "_to" => dst = v.as_str().map(str::to_string),
            "_type" => label = v.as_str().map(str::to_string),
            "weight" => {
                weight = match v {
                    rmpv::Value::F64(f) => Some(*f),
                    rmpv::Value::F32(f) => Some(*f as f64),
                    rmpv::Value::Integer(i) => i.as_f64(),
                    _ => None,
                }
                .filter(|w| w.is_finite());
            }
            _ => {}
        }
    }

    let src = src?;
    let dst = dst?;
    Some(ImplicitEdge {
        collection: collection.to_string(),
        src,
        dst,
        label: label.unwrap_or_else(|| DEFAULT_EDGE_LABEL.to_string()),
        weight,
    })
}

/// Encode `{"weight": <w>}` as a standard-msgpack map.
///
/// The bytes are a 1-entry msgpack map with a fixstr key `"weight"` and an
/// F64 value, exactly the shape `extract_weight_from_properties`
/// (`nodedb-graph` `csr/weights.rs`, which decodes via `rmpv`) reads to derive
/// the CSR edge weight.
fn weight_properties(weight: f64) -> Vec<u8> {
    let map = rmpv::Value::Map(vec![(
        rmpv::Value::String("weight".into()),
        rmpv::Value::F64(weight),
    )]);
    let mut buf = Vec::new();
    // Writing a fully-owned `rmpv::Value` to a `Vec` is infallible; on the
    // impossible error path emit empty properties (weight defaults to 1.0)
    // rather than panicking in library code.
    if rmpv::encode::write_value(&mut buf, &map).is_err() {
        return Vec::new();
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_graph::csr::extract_weight_from_properties;

    /// Build a standard-msgpack map document from string/number fields, mirroring
    /// the on-wire shape produced by the DML `row_to_msgpack` writer.
    fn doc(fields: &[(&str, rmpv::Value)]) -> Vec<u8> {
        let map = rmpv::Value::Map(
            fields
                .iter()
                .map(|(k, v)| (rmpv::Value::String((*k).into()), v.clone()))
                .collect(),
        );
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, &map).expect("encode test doc");
        buf
    }

    #[test]
    fn non_edge_doc_is_skipped() {
        let v = doc(&[("name", rmpv::Value::String("alice".into()))]);
        assert!(extract_edge("people", &v).is_none());
    }

    #[test]
    fn missing_to_is_skipped() {
        let v = doc(&[("_from", rmpv::Value::String("a".into()))]);
        assert!(extract_edge("e", &v).is_none());
    }

    #[test]
    fn basic_edge_defaults_label_and_no_weight() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.src, "a");
        assert_eq!(e.dst, "b");
        assert_eq!(e.label, "edge");
        assert!(e.weight.is_none());
    }

    #[test]
    fn typed_weighted_edge() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
            ("_type", rmpv::Value::String("ROAD".into())),
            ("weight", rmpv::Value::F64(5.0)),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.label, "ROAD");
        assert_eq!(e.weight, Some(5.0));
    }

    #[test]
    fn weight_properties_round_trip_through_extractor() {
        let props = weight_properties(7.5);
        assert_eq!(extract_weight_from_properties(&props), 7.5);
    }

    #[test]
    fn empty_properties_default_to_unit_weight() {
        assert_eq!(extract_weight_from_properties(&[]), 1.0);
    }

    #[test]
    fn integer_weight_is_carried() {
        let v = doc(&[
            ("_from", rmpv::Value::String("a".into())),
            ("_to", rmpv::Value::String("b".into())),
            ("weight", rmpv::Value::Integer(3.into())),
        ]);
        let e = extract_edge("links", &v).expect("edge");
        assert_eq!(e.weight, Some(3.0));
        let props = weight_properties(e.weight.unwrap());
        assert_eq!(extract_weight_from_properties(&props), 3.0);
    }
}
