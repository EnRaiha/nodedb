// SPDX-License-Identifier: BUSL-1.1

//! Pure payload encoder for graph node-label WAL records.
//!
//! Shared by the autocommit `wal_append_if_write` arms (`core.rs`) and the
//! `set_node_labels` DDL handler's own local append (see
//! `control::server::shared::ddl::neutral::graph_ops::edge`), so the
//! producer and `try_replay_graph_node_label` never drift on shape.

/// Encode the payload for a `GraphNodeLabelSet` / `GraphNodeLabelRemove`
/// record: `(node_id, labels)`.
///
/// Set vs. remove is discriminated by the WAL record type, not a field in
/// the payload, so this ONE encoder serves both directions.
pub(crate) fn encode_graph_node_label_payload(
    node_id: &str,
    labels: &[String],
) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&(node_id, labels)).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("wal graph node label: {e}"),
    })
}
