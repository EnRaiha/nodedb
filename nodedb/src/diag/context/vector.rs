// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for vector-index post-apply capture sites.
//!
//! A committed index mutation that never reaches this node's Data Plane
//! leaves the node answering vector search out of state consensus already
//! changed, with nothing in the catalog to show the disagreement.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed vector-index entry whose per-node physical work failed, so
/// this node's Data Plane diverges from the replicated catalog.
pub(in crate::diag) struct VectorIndexNotApplied<'a> {
    /// Stage that failed (`set_params_wal_append`, `set_params_dispatch`,
    /// `drop_index_wal_append`, `drop_index_fsync`, `drop_index_dispatch`).
    pub stage: &'static str,
    pub database_id: u64,
    pub tenant_id: u64,
    /// Collection the index covers.
    pub collection: &'a str,
    /// Indexed column; empty for the collection's default vector field.
    pub field_name: &'a str,
    /// What failed, without the per-occurrence detail.
    pub error_class: &'a str,
}

impl DomainContext for VectorIndexNotApplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.vector_index_not_applied"
    }

    fn grouping_key(&self) -> String {
        // Stage + error class name the bug; the index identity is the
        // occurrence, so one broken node files one report.
        format!("stage={};cause={}", self.stage, self.error_class)
    }

    fn to_json(&self) -> Value {
        json!({
            "stage": self.stage,
            "database_id": self.database_id,
            "tenant_id": self.tenant_id,
            "collection": self.collection,
            "field_name": self.field_name,
            "error_class": self.error_class,
            "why_fatal": "the catalog row is already committed by consensus, so this node \
                          reports the index in SHOW INDEXES while its Data Plane never \
                          built or tore it down. A create that fails here answers vector \
                          search from an index with default parameters, or none at all; a \
                          drop that fails here keeps serving hits from the dropped index \
                          and restores it on the next boot",
            "operator_action": "restart this node — the boot seed rebuilds every vector \
                                 index from the replicated catalog row, which is the \
                                 authority both sides of this failure disagree with. \
                                 Compare vector search results against a healthy replica \
                                 before serving traffic from this one",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VectorIndexNotApplied<'static> {
        VectorIndexNotApplied {
            stage: "set_params_dispatch",
            database_id: 1,
            tenant_id: 2,
            collection: "documents",
            field_name: "embedding",
            error_class: "dispatch",
        }
    }

    #[test]
    fn grouping_ignores_the_index_identity() {
        let first = sample();
        let second = VectorIndexNotApplied {
            database_id: 90,
            tenant_id: 91,
            collection: "other",
            field_name: "other_emb",
            ..first
        };
        assert_eq!(first.grouping_key(), second.grouping_key());
        assert_eq!(
            first.grouping_key(),
            "stage=set_params_dispatch;cause=dispatch"
        );
    }

    #[test]
    fn grouping_separates_create_from_drop() {
        let create = sample();
        let drop = VectorIndexNotApplied {
            stage: "drop_index_dispatch",
            ..create
        };
        assert_ne!(create.grouping_key(), drop.grouping_key());
    }
}
