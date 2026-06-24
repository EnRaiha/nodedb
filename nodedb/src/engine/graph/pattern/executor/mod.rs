// SPDX-License-Identifier: BUSL-1.1

//! MATCH pattern executor — runs pattern matching on the CSR index.
//!
//! Takes a parsed `MatchQuery` and produces a result set of bound variable
//! assignments. Each assignment is a row mapping variable names to node/edge IDs.

pub(super) mod continuation;
pub(super) mod core;
pub(super) mod expansion;
pub(super) mod predicates;
pub(super) mod types;

pub use self::continuation::execute_continuation;
pub use self::core::{execute, rows_to_msgpack};
pub use self::types::{BindingRow, MatchOutcome, UnresolvedExpansion, VarLenResume};
