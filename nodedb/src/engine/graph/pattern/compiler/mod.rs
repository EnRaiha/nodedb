// SPDX-License-Identifier: BUSL-1.1

//! MATCH query parser — parses Cypher-style pattern syntax into AST.
//!
//! Supported syntax:
//! ```text
//! MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
//! WHERE a.name = 'Alice'
//! RETURN a.name, b.name, c.name
//! LIMIT 10
//!
//! OPTIONAL MATCH (a)-[:LIKES]->(b)
//!
//! MATCH (a)-[:KNOWS*1..3]->(b)   -- variable-length paths
//!
//! MATCH (a:Person)-[:KNOWS]->(b:Person), (a)-[:KNOWS]->(c:Person)  -- self-join
//! ```

mod bindings;
mod clauses;
mod helpers;
mod parser;

pub use parser::parse;
pub(super) use parser::parse_match_clauses;
