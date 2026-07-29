// SPDX-License-Identifier: Apache-2.0

use super::{
    super::statement::{GraphStmt, NodedbStatement},
    fusion_params::{FusionParams, RAG_FUSION_KEYWORDS},
    helpers::{
        direction_after, extract_properties, quoted_after, quoted_list_after, usize_after,
        word_after,
    },
    tokenizer::Tok,
};

pub(super) fn parse_insert_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    let properties = extract_properties(toks);
    Some(NodedbStatement::Graph(GraphStmt::GraphInsertEdge {
        collection,
        src,
        dst,
        label,
        properties,
    }))
}

pub(super) fn parse_delete_edge(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let label = quoted_after(toks, "TYPE")?;
    Some(NodedbStatement::Graph(GraphStmt::GraphDeleteEdge {
        collection,
        src,
        dst,
        label,
    }))
}

pub(super) fn parse_set_labels(toks: &[Tok<'_>], remove: bool) -> Option<NodedbStatement> {
    let keyword = if remove { "UNLABEL" } else { "LABEL" };
    let node_id = quoted_after(toks, keyword)?;
    let labels = quoted_list_after(toks, "AS");
    Some(NodedbStatement::Graph(GraphStmt::GraphSetLabels {
        node_id,
        labels,
        remove,
    }))
}

pub(super) fn parse_traverse(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let start = quoted_after(toks, "FROM")?;
    let depth = usize_after(toks, "DEPTH").unwrap_or(2);
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks);
    Some(NodedbStatement::Graph(GraphStmt::GraphTraverse {
        collection,
        start,
        depth,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_neighbors(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let node = quoted_after(toks, "OF")?;
    let edge_label = quoted_after(toks, "LABEL");
    let direction = direction_after(toks);
    Some(NodedbStatement::Graph(GraphStmt::GraphNeighbors {
        collection,
        node,
        edge_label,
        direction,
    }))
}

pub(super) fn parse_path(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let collection = quoted_after(toks, "IN")?;
    let src = quoted_after(toks, "FROM")?;
    let dst = quoted_after(toks, "TO")?;
    let max_depth = usize_after(toks, "MAX_DEPTH").unwrap_or(10);
    let edge_label = quoted_after(toks, "LABEL");
    Some(NodedbStatement::Graph(GraphStmt::GraphPath {
        collection,
        src,
        dst,
        max_depth,
        edge_label,
    }))
}

pub(super) fn parse_algo(toks: &[Tok<'_>]) -> Option<NodedbStatement> {
    let algorithm =
        super::helpers::find_keyword(toks, "ALGO").and_then(|i| match toks.get(i + 1)? {
            Tok::Word(w) => Some(w.to_ascii_uppercase()),
            _ => None,
        })?;

    // Accept either a bare word (`ON users`) or a quoted literal (`ON 'users'`)
    // so clients can escape collection names safely.
    //
    // Reject the `ON (subquery)` form early: the tokenizer strips `(` and `)`,
    // so `ON (SELECT …)` becomes the token sequence `[ON, SELECT, …]`.
    // `quoted_after("ON")` would return `"SELECT"` which would be silently
    // stored as the collection name and then ignored — producing tenant-wide
    // results. Returning None here causes the statement to be treated as
    // unparseable, surfacing a structured error rather than silent wrong data.
    let collection_raw = quoted_after(toks, "ON")?;
    const SUBQUERY_KEYWORDS: &[&str] = &["SELECT", "WITH", "VALUES", "TABLE"];
    if SUBQUERY_KEYWORDS
        .iter()
        .any(|kw| collection_raw.eq_ignore_ascii_case(kw))
    {
        return None;
    }
    let collection = collection_raw.to_lowercase();

    Some(NodedbStatement::Graph(GraphStmt::GraphAlgo {
        algorithm,
        collection,
        edge_label: quoted_after(toks, "EDGE_LABEL"),
        damping: super::helpers::float_after(toks, "DAMPING"),
        tolerance: super::helpers::float_after(toks, "TOLERANCE"),
        resolution: super::helpers::float_after(toks, "RESOLUTION"),
        max_iterations: usize_after(toks, "ITERATIONS"),
        sample_size: usize_after(toks, "SAMPLE"),
        source_node: quoted_after(toks, "FROM").or_else(|| quoted_after(toks, "SOURCE")),
        direction: word_after(toks, "DIRECTION"),
        mode: word_after(toks, "MODE"),
        personalization: super::helpers::object_after(toks, "PERSONALIZATION"),
    }))
}

/// Parse `GRAPH RAG FUSION ON <collection> QUERY ARRAY[…] [options…]`.
///
/// All fusion parameters are delegated to [`FusionParams::extract`] so
/// every fusion SQL surface shares one typed, quote-aware extractor.
pub(super) fn parse_rag_fusion(toks: &[Tok<'_>], sql: &str) -> Option<NodedbStatement> {
    let collection = word_after(toks, "ON").or_else(|| quoted_after(toks, "ON"))?;
    let params = FusionParams::extract(toks, sql, &RAG_FUSION_KEYWORDS);
    Some(NodedbStatement::Graph(GraphStmt::GraphRagFusion {
        collection,
        params,
    }))
}
