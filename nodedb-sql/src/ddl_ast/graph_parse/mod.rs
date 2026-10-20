// SPDX-License-Identifier: Apache-2.0

//! Typed parsing for the graph DSL (`GRAPH ...`, `MATCH ...`).
//!
//! A quote-, brace- and bracket-aware tokeniser feeds one parser per
//! statement. Each parser reads its clauses left to right and produces a
//! typed [`NodedbStatement`]. A token that is not a clause of the statement
//! is a parse error, so a misspelled clause keyword never runs the
//! statement with a default. Every graph DSL command flows through here
//! before reaching a handler, so the handlers never touch raw SQL.
//!
//! Numeric *ranges* and engine-level caps stay unvalidated here and are
//! enforced at the pgwire boundary, which keeps this module free of
//! `pgwire` dependencies and out of the `nodedb` → `nodedb-sql` edge.
//!
//! Structure is validated here, because the boundary cannot see it: a
//! required clause that is absent, or a clause value outside its own
//! vocabulary, means no statement reaches the boundary at all. Both are
//! reported as a parse error naming what is wrong.
//!
//! Accepted-grammar limitation for the batched edge statements: the tokenizer
//! drops `(`, `)`, and `,`, so `GRAPH INSERT EDGES` reads the `VALUES` tail as
//! a flat field list and only checks that its length is a multiple of three.
//! `VALUES ('a','b','L'), ('c','d','L')` and `VALUES ('a','b','L')
//! ('c','d','L')` produce the same two triples, and a trailing token is read as
//! another triple's field rather than rejected. Closing that needs a tokenizer
//! change, so it is tracked as a follow-up rather than fixed here.

mod cursor;
mod edge_predicate;
mod entry;
pub mod fusion_params;
mod tokenizer;
mod variants;

pub use entry::try_parse;
pub use fusion_params::{
    FusionKeywords, FusionParams, RAG_FUSION_KEYWORDS, SEARCH_FUSION_KEYWORDS,
    parse_search_using_fusion,
};
