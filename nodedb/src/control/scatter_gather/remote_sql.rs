// SPDX-License-Identifier: BUSL-1.1

//! Construction of the `GRAPH TRAVERSE` text a remote shard is asked to re-plan.
//!
//! Its own file because this is the module's only injection surface: the hop
//! synthesizes SQL from caller-supplied node ids, edge labels, and a collection
//! name, and the owning node re-plans whatever arrives. Keeping every fragment
//! of that text — quoting, the optional LABEL clause, the DIRECTION keyword —
//! in one place is what makes "is any of this unescaped?" answerable by reading
//! a single short file rather than auditing the dispatch loop.

use crate::engine::graph::edge_store::Direction;

/// Fields the remote `GRAPH TRAVERSE` text is built from.
pub(super) struct RemoteTraverseSql<'a> {
    pub(super) collection: &'a str,
    pub(super) node_id: &'a str,
    pub(super) depth: usize,
    pub(super) edge_label: Option<&'a str>,
    pub(super) direction: Direction,
}

/// The traversal direction as its SQL keyword.
///
/// The value comes from a closed enum, never from caller text, so every arm is
/// a fixed keyword and there is nothing to escape.
fn canonical_direction_sql(direction: Direction) -> &'static str {
    match direction {
        Direction::In => "in",
        Direction::Out => "out",
        Direction::Both => "both",
    }
}

/// The optional edge label as a ` LABEL <literal>` clause, or empty.
///
/// The label is caller-supplied text, so it goes through the shared literal
/// quoter — this is the only place the clause is built.
fn canonical_label_sql(edge_label: Option<&str>) -> String {
    match edge_label {
        Some(label) => format!(" LABEL {}", ::nodedb_types::quote_literal(label)),
        None => String::new(),
    }
}

pub(super) fn build_graph_traverse_sql(params: RemoteTraverseSql<'_>) -> String {
    let RemoteTraverseSql {
        collection,
        node_id,
        depth,
        edge_label,
        direction,
    } = params;
    format!(
        "GRAPH TRAVERSE IN {} FROM {} DEPTH {}{} DIRECTION {}",
        ::nodedb_types::quote_literal(collection),
        ::nodedb_types::quote_literal(node_id),
        ::nodedb_types::Value::Integer(if depth == 0 { 0 } else { 1 }).to_sql_literal(),
        canonical_label_sql(edge_label),
        canonical_direction_sql(direction),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graph_traverse_sql_quotes_node_and_label_literals() {
        let sql = build_graph_traverse_sql(RemoteTraverseSql {
            collection: "audit'; --",
            node_id: "node'; DROP GRAPH audit; --",
            depth: 1,
            edge_label: Some("label'; --"),
            direction: Direction::Out,
        });
        assert_eq!(
            sql,
            "GRAPH TRAVERSE IN 'audit''; --' FROM 'node''; DROP GRAPH audit; --' DEPTH 1 \
             LABEL 'label''; --' DIRECTION out"
        );
    }
}
