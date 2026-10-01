// SPDX-License-Identifier: Apache-2.0

//! The stage of [`super::op::GraphOp::RagFusion`] a core runs.
//!
//! On one node, one core runs the whole fusion over its own partitions. In a
//! cluster the legs live apart: the vector index and the text index sit on
//! the collection's owner, and graph edges sit on their endpoints' key
//! vShards. A coordinator then runs the fusion in stages:
//!
//! 1. The owner exports the raw vector and BM25 hits ([`RagStage::ExportLegs`]).
//! 2. Every graph owner answers which of the hits' surrogates name graph
//!    nodes ([`RagStage::Bindings`]).
//! 3. The coordinator walks the graph from those nodes across shards.
//! 4. Every graph owner answers which reached nodes carry a surrogate
//!    ([`RagStage::Bindings`] again).
//! 5. The coordinator fuses the three lists as the single-core fusion does.

/// Which part of a RAG fusion a core runs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RagStage {
    /// The whole fusion, over this core's own indexes and edges.
    Local,
    /// The vector leg, and the BM25 leg of a three-source fusion, only.
    /// Answered as a one-element msgpack array holding a [`RagLegs`].
    ExportLegs,
    /// This core's graph bindings for the given surrogates and names, and
    /// whether it holds edges of the fusion's collection. Answered as a
    /// msgpack array of [`RagBindingRow`].
    Bindings {
        surrogates: Vec<u32>,
        names: Vec<String>,
    },
}

/// One vector-leg hit, in rank order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RagVectorHit {
    /// The row's global surrogate. `None` for an index entry that predates
    /// surrogates: it ranks under a key that matches nothing else.
    pub surrogate: Option<u32>,
    /// The HNSW entry id, which keys a hit with no surrogate.
    pub entry_id: u32,
    pub distance: f32,
}

/// One BM25-leg hit, in rank order.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RagTextHit {
    pub surrogate: u32,
    pub score: f32,
}

/// The owner's raw legs of a RAG fusion.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct RagLegs {
    pub vector: Vec<RagVectorHit>,
    /// Empty for a two-source fusion.
    pub text: Vec<RagTextHit>,
}

/// One row of a core's [`RagStage::Bindings`] answer.
#[derive(
    Debug,
    Clone,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum RagBindingRow {
    /// Graph node `name` carries `surrogate` in this core's partition.
    Bound { name: String, surrogate: u32 },
    /// This core's partition holds edges of the fusion's collection.
    KnowsCollection,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legs_and_binding_rows_round_trip() {
        let legs = RagLegs {
            vector: vec![RagVectorHit {
                surrogate: Some(7),
                entry_id: 3,
                distance: 0.25,
            }],
            text: vec![RagTextHit {
                surrogate: 9,
                score: 1.5,
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&vec![legs.clone()]).expect("encode legs");
        let decoded: Vec<RagLegs> = zerompk::from_msgpack(&bytes).expect("decode legs");
        assert_eq!(decoded, vec![legs]);

        let rows = vec![
            RagBindingRow::Bound {
                name: "alice".into(),
                surrogate: 7,
            },
            RagBindingRow::KnowsCollection,
        ];
        let bytes = zerompk::to_msgpack_vec(&rows).expect("encode rows");
        let decoded: Vec<RagBindingRow> = zerompk::from_msgpack(&bytes).expect("decode rows");
        assert_eq!(decoded, rows);
    }
}
