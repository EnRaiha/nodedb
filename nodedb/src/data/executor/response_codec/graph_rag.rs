// SPDX-License-Identifier: BUSL-1.1

//! The GraphRAG fusion response, shared by the Data Plane, the cluster
//! coordinator and the Control-Plane response translator.
//!
//! `GraphRagResult::surrogate` is set on a row named by no graph node. The
//! response translator names that row from the catalog and clears the field.
//! The encoder omits a cleared field, so a client never sees it.

use serde::Serialize;

/// One fused row.
#[derive(Debug, Clone, PartialEq, Serialize, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct GraphRagResult {
    pub node_id: String,
    /// The row's surrogate while `node_id` is its decimal stand-in.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub surrogate: Option<u32>,
    pub rrf_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_distance: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hop_distance: Option<usize>,
}

/// Writes `surrogate` only while it is set. Every other optional field is
/// written as nil when absent.
impl zerompk::ToMessagePack for GraphRagResult {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(5 + usize::from(self.surrogate.is_some()))?;
        writer.write_string("node_id")?;
        writer.write_string(&self.node_id)?;
        if let Some(surrogate) = self.surrogate {
            writer.write_string("surrogate")?;
            writer.write_u32(surrogate)?;
        }
        writer.write_string("rrf_score")?;
        writer.write_f64(self.rrf_score)?;
        writer.write_string("vector_rank")?;
        zerompk::ToMessagePack::write(&self.vector_rank, writer)?;
        writer.write_string("vector_distance")?;
        zerompk::ToMessagePack::write(&self.vector_distance, writer)?;
        writer.write_string("hop_distance")?;
        zerompk::ToMessagePack::write(&self.hop_distance, writer)
    }
}

/// A fusion's rows and the run's metadata.
#[derive(Debug, Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct GraphRagResponse {
    pub results: Vec<GraphRagResult>,
    pub metadata: GraphRagMetadata,
}

/// What a fusion run read and reached.
#[derive(Debug, Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct GraphRagMetadata {
    pub vector_candidates: usize,
    pub graph_expanded: usize,
    pub truncated: bool,
    /// Expanded nodes that carry no surrogate and therefore cannot intersect
    /// another engine's candidates. They are traversed and returned like any
    /// other node. A non-zero count beside few results means missing identity
    /// bindings, not a small graph.
    pub graph_unaddressable: usize,
    /// Snapshot watermark LSN at the time of query execution.
    pub watermark_lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(surrogate: Option<u32>) -> GraphRagResult {
        GraphRagResult {
            node_id: "a".into(),
            surrogate,
            rrf_score: 0.5,
            vector_rank: Some(0),
            vector_distance: None,
            hop_distance: Some(1),
        }
    }

    #[test]
    fn named_row_omits_surrogate_and_round_trips() {
        let named = row(None);
        let bytes = zerompk::to_msgpack_vec(&named).expect("encode");
        let json = nodedb_types::json_from_msgpack(&bytes).expect("json");
        assert!(json.get("surrogate").is_none(), "got {json}");
        assert!(json.get("vector_distance").is_some(), "got {json}");
        let back: GraphRagResult = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, named);
    }

    #[test]
    fn unnamed_row_carries_its_surrogate_across_the_wire() {
        let unnamed = row(Some(7));
        let bytes = zerompk::to_msgpack_vec(&unnamed).expect("encode");
        let back: GraphRagResult = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, unnamed);
    }
}
