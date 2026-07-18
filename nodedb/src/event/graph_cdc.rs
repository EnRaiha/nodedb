// SPDX-License-Identifier: BUSL-1.1

//! CDC naming and identity for graph mutations (node labels + edges).
//!
//! Graph writes emit [`crate::event::WriteEvent`]s like every other engine, so
//! triggers, change streams, and materialized views fire on graph mutations.
//! Two graph specifics live here so the forward-path emit (Data Plane) and the
//! WAL-replay reconstruction (Event Plane) cannot drift on the CDC shape:
//!
//! - **Node labels** are tenant-wide and carry no natural collection. The
//!   statement-time overlay stages them under an un-nameable sentinel with a
//!   leading NUL (`GRAPH_LABEL_COLL_KEY`); CDC events carry [`GRAPH_LABEL_STREAM`]
//!   instead — the same text without the NUL, which a SQL CDC subscriber can name.
//! - **Edges** carry a real collection but their identity is the
//!   `(src, label, dst)` triple, not a surrogate row id. [`edge_row_id`] composes
//!   the stable CDC `row_id` from that triple; both the forward emit and the
//!   replay reconstruction call it so the two agree byte-for-byte.

use std::collections::HashMap;

use nodedb_types::Value;

/// Delimiter separating an edge's `(src, label, dst)` components in its CDC
/// `row_id`. `\u{1}` (SOH) is a control character that does not appear in
/// user-facing node ids or edge labels, so the composition round-trips
/// unambiguously.
const EDGE_ID_SEP: char = '\u{1}';

/// Nameable CDC stream carrying tenant-wide node-label mutations.
///
/// The overlay/storage sentinel for the same data is `"\0__graph_node_labels__"`
/// (`GRAPH_LABEL_COLL_KEY` in the stage-write overlay); its leading NUL makes it
/// un-nameable by a SQL CDC subscriber. This NUL-free twin is the `collection`
/// every node-label `WriteEvent` carries and the name a
/// `CREATE CHANGE STREAM ... FOR __graph_node_labels__` subscriber uses.
pub(crate) const GRAPH_LABEL_STREAM: &str = "__graph_node_labels__";

/// Compose the stable CDC `row_id` for an edge from its identity triple.
///
/// Called by both the forward-path emit (Data Plane edge handlers) and the
/// WAL-replay reconstruction (Event Plane), so the two produce byte-identical
/// `row_id`s and the Event-Plane watermark can dedup replayed events against
/// forward events.
pub(crate) fn edge_row_id(src: &str, label: &str, dst: &str) -> String {
    let mut id = String::with_capacity(src.len() + label.len() + dst.len() + 2);
    id.push_str(src);
    id.push(EDGE_ID_SEP);
    id.push_str(label);
    id.push(EDGE_ID_SEP);
    id.push_str(dst);
    id
}

/// Serialize a node-label delta (the labels added or removed) as the CDC event
/// payload: a `{ "labels": [ <label>, ... ] }` object encoded as standard
/// MessagePack (`nodedb_types::value_to_msgpack`), the dialect a CDC subscriber
/// decodes via `deserialize_event_payload`. Shared by the forward emit and the
/// WAL replay so both produce the
/// same bytes.
///
/// Encoding a small, fixed-shape object is infallible in practice; on the
/// impossible serialization failure this yields empty bytes rather than
/// panicking (no `.unwrap()` in library code), and because both the forward and
/// replay paths call this one function they degrade identically.
pub(crate) fn graph_label_delta_value(labels: &[String]) -> Vec<u8> {
    let arr = Value::Array(labels.iter().map(|l| Value::String(l.clone())).collect());
    let mut obj = HashMap::with_capacity(1);
    obj.insert("labels".to_string(), arr);
    // Standard-MessagePack dialect (not zerompk's tagged enum encoding) so a CDC
    // subscriber decoding the event payload via `deserialize_event_payload`
    // (which reads `json_from_msgpack`, the standard-msgpack reader) can read it —
    // the same dialect a document `WriteEvent`'s raw blob carries.
    nodedb_types::value_to_msgpack(&Value::Object(obj)).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edge_row_id_is_stable_and_separated() {
        assert_eq!(edge_row_id("a", "KNOWS", "b"), "a\u{1}KNOWS\u{1}b");
    }

    #[test]
    fn edge_row_id_distinguishes_components() {
        // Same concatenation, different boundaries must not collide.
        assert_ne!(edge_row_id("ab", "K", "c"), edge_row_id("a", "bK", "c"));
    }

    #[test]
    fn label_delta_value_round_trips_as_object() {
        let bytes = graph_label_delta_value(&["Person".to_string(), "User".to_string()]);
        let map = crate::event::deserialize_event_payload(&bytes)
            .expect("label delta must decode as a JSON object");
        let labels = map
            .get("labels")
            .and_then(|v| v.as_array())
            .expect("labels array present");
        let got: Vec<&str> = labels.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(got, vec!["Person", "User"]);
    }

    #[test]
    fn label_stream_name_has_no_nul() {
        assert!(!GRAPH_LABEL_STREAM.contains('\0'));
        assert_eq!(GRAPH_LABEL_STREAM, "__graph_node_labels__");
    }
}
