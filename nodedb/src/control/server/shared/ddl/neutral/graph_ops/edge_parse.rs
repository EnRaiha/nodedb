// SPDX-License-Identifier: BUSL-1.1

//! Validation and normalization of the clauses an edge write carries.
//!
//! Split from the handlers so the label guard and the `PROPERTIES` encoding
//! live next to each other rather than at either end of the dispatch flow.

use nodedb_sql::ddl_ast::GraphProperties;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Maximum byte length for an edge label string. Keeps a single `TYPE`
/// clause from bloating the CSR label table and the msgpack wire payload.
const MAX_EDGE_LABEL_BYTES: usize = 256;

/// Validate a user-supplied edge label. Rejects empty, overlong, and
/// labels containing ASCII control characters (0x00..=0x1F, 0x7F).
///
/// Runs at every DSL ingress so the CSR interner never sees degenerate
/// input — a complement to the `u32` widening of the label id space.
pub(super) fn validate_edge_label(label: &str) -> Result<(), DdlError> {
    if label.is_empty() {
        return Err(ddl_err("42601", "edge TYPE label must not be empty"));
    }
    if label.len() > MAX_EDGE_LABEL_BYTES {
        return Err(ddl_err(
            "42601",
            format!(
                "edge TYPE label is {} bytes; maximum is {MAX_EDGE_LABEL_BYTES}",
                label.len()
            ),
        ));
    }
    if label.chars().any(|c| c.is_control() || c == '\u{007F}') {
        return Err(ddl_err(
            "42601",
            "edge TYPE label must not contain control characters",
        ));
    }
    Ok(())
}

/// Convert a parsed `PROPERTIES` clause to the JSON string stored
/// in `GraphOp::EdgePut`. Object-literal forms go through the shared
/// `nodedb_sql::parser::object_literal::parse_object_literal_complete`
/// so the type coercions (numbers, bools, nested objects) match
/// every other object-literal ingress (INSERT { ... }, UPSERT).
pub(super) fn properties_to_json(properties: GraphProperties) -> Result<String, DdlError> {
    match properties {
        GraphProperties::None => Ok(String::new()),
        GraphProperties::Quoted(s) => Ok(s),
        GraphProperties::Object(obj_str) => {
            // The graph lexer hands over the balanced `{ … }` and nothing else,
            // so the strict form is the right contract: anything trailing means
            // the statement was misparsed upstream, and saying so beats
            // persisting an edge whose properties silently lost a clause.
            match nodedb_sql::parser::object_literal::parse_object_literal_complete(&obj_str) {
                Some(Ok(fields)) => sonic_rs::to_string(&nodedb_types::Value::Object(fields))
                    .map_err(|e| ddl_err("XX000", format!("PROPERTIES serialize error: {e}"))),
                Some(Err(msg)) => Err(ddl_err(
                    "42601",
                    format!("PROPERTIES object literal error: {msg}"),
                )),
                None => Ok(String::new()),
            }
        }
    }
}
