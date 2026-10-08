// SPDX-License-Identifier: BUSL-1.1

//! `nodedb_graph::GraphError` into the crate error.
//!
//! A refused memory reservation is the graph engine's memory budget. A
//! rebuild that holds the partition journal leaves the index busy. Every
//! other graph error is a storage fault of the engine.

use nodedb_graph::GraphError;

use crate::Error;

impl From<GraphError> for Error {
    fn from(e: GraphError) -> Self {
        match e {
            // The same class a vector or FTS budget refusal has.
            GraphError::MemoryBudget(_) => Self::MemoryExhausted {
                engine: "graph".to_string(),
            },
            // A rebuild already holds the partition's journal.
            GraphError::RebuildInProgress => Self::ObjectNotInPrerequisiteState {
                object: "graph CSR index".to_string(),
                detail: e.to_string(),
            },
            GraphError::LabelOverflow { .. }
            | GraphError::NodeOverflow { .. }
            | GraphError::WithdrawRefused { .. }
            | GraphError::RebuildSuperseded
            | GraphError::RebuildJournalOverflow { .. }
            | GraphError::RebuildReplayDiverged { .. }
            | GraphError::RebuildSnapshotInvalid { .. } => Self::Storage {
                engine: "graph".to_string(),
                detail: e.to_string(),
            },
            // `GraphError` is `#[non_exhaustive]` and lives in another crate,
            // so the compiler requires this arm. A variant this build cannot
            // name is a storage fault.
            _ => Self::Storage {
                engine: "graph".to_string(),
                detail: e.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_busy_index_is_not_in_prerequisite_state() {
        assert!(matches!(
            Error::from(GraphError::RebuildInProgress),
            Error::ObjectNotInPrerequisiteState { .. }
        ));
    }

    #[test]
    fn an_exhausted_id_space_is_a_graph_storage_fault() {
        assert!(matches!(
            Error::from(GraphError::NodeOverflow { used: 1 }),
            Error::Storage { ref engine, .. } if engine == "graph"
        ));
    }
}
