// SPDX-License-Identifier: BUSL-1.1

//! Implicit-edge extraction primitives shared by the INSERT / DELETE / UPDATE
//! lifecycle paths: msgpack edge-field decode, label-default resolution, and
//! weight-property encoding.

use memchr::memmem;

/// Default edge label when a document omits `_type`. Mirrors the historical
/// Data-Plane `maybe_register_edge` default.
pub(super) const DEFAULT_EDGE_LABEL: &str = "edge";

/// One implicit edge extracted from a document write.
pub(super) struct ImplicitEdge {
    pub(super) collection: String,
    pub(super) src: String,
    pub(super) dst: String,
    pub(super) label: String,
    /// `Some(w)` when the document carried a finite numeric `weight`.
    pub(super) weight: Option<f64>,
}

/// Decode a standard-msgpack document `value` and extract an implicit edge
/// when it carries `_from` and `_to` string fields.
///
/// A cheap byte pre-filter skips the msgpack decode for the overwhelming
/// majority of documents that are not edges. `_type` defaults to `"edge"`;
/// `weight` is carried only when present and finite.
pub(super) fn extract_edge(collection: &str, value: &[u8]) -> Option<ImplicitEdge> {
    // Pre-filter: an edge document's msgpack always contains the literal key
    // bytes `_from`. Avoid decoding non-edge documents on the hot path.
    memmem::find(value, b"_from")?;

    let decoded = crate::util::bounded_msgpack::read_value(value).ok()?;
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
        label: resolve_edge_label(label.as_deref()),
        weight,
    })
}

/// Resolve the edge label, substituting [`DEFAULT_EDGE_LABEL`] when a document
/// omits `_type`. Shared by the INSERT, DELETE, and UPDATE paths so an
/// `EdgeDelete` / `EdgePut` always uses the same label the matching write
/// created.
pub(super) fn resolve_edge_label(label: Option<&str>) -> String {
    label.unwrap_or(DEFAULT_EDGE_LABEL).to_owned()
}

/// Encode `{"weight": <w>}` as a standard-msgpack map.
///
/// The bytes are a 1-entry msgpack map with a fixstr key `"weight"` and an
/// F64 value, exactly the shape `extract_weight_from_properties`
/// (`nodedb-graph` `csr/weights.rs`, which decodes via `rmpv`) reads to derive
/// the CSR edge weight.
pub(super) fn weight_properties(weight: f64) -> Vec<u8> {
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
    fn label_default_matches_insert_default() {
        assert_eq!(resolve_edge_label(None), "edge");
        assert_eq!(resolve_edge_label(Some("ROAD")), "ROAD");
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
