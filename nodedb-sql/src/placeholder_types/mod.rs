// SPDX-License-Identifier: Apache-2.0

//! Best-effort type inference for `$N` prepared-statement placeholders.
//!
//! The pgwire extended-query protocol lets a client send `Parse` without
//! declaring any parameter OIDs. The server then has to answer `Describe`
//! with a `ParameterDescription`, and answering "unknown" (OID 0) for every
//! position forces well-behaved clients into a failure: `tokio-postgres`
//! refuses to serialize an `i64` against an unknown OID. This module walks
//! the *unsubstituted* statement and reports the positions whose type the
//! SQL — plus the catalog the SQL names — pins down.
//!
//! # Why a separate walk rather than planner output
//!
//! The planner cannot plan an AST that still contains placeholders — the
//! resolver has no `Value::Placeholder` arm — and the Control Plane's
//! schema-inference pass therefore rewrites `$N` to `NULL` in the SQL text
//! before planning. That rewrite destroys the position → type link, so
//! inference has to happen here, on the parsed-but-unbound AST.
//!
//! # Directionality: under-infer, never over-infer
//!
//! A PostgreSQL client that receives an unknown parameter type sends the
//! value in text format, which the bind layer already handles — so leaving
//! a position unresolved only costs a text round-trip. Reporting a concrete
//! OID, by contrast, makes the client commit to that type's *binary*
//! encoding. Any position whose type is not pinned down unambiguously stays
//! `None`.

mod catalog_free;
mod column_backed;
mod infer;
mod scope;
mod slots;

#[cfg(test)]
mod tests;

pub use infer::infer_placeholder_types;
pub use slots::InferredParamType;
