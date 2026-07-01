// SPDX-License-Identifier: BUSL-1.1

//! CollectionSchema receive handler.
//!
//! When a sync peer announces a [`CollectionSchemaSyncMsg`], the receiving
//! cluster materializes the collection into its system catalog (create-only,
//! via `PutCollectionIfAbsent` — never clobbering an existing collection).
//! The Data-Plane engine register happens in the shared post-apply path on
//! **every** node that applies the Raft entry, exactly as it does for a
//! local `CREATE COLLECTION`. This handler is therefore symmetric with the
//! pgwire CREATE handler: it only `stored_from_descriptor` → propose
//! `PutCollectionIfAbsent` → `apply_locally_if_needed`, and never dispatches
//! the register itself.

use std::sync::Arc;

use tracing::warn;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::state::SharedState;

use super::super::wire::{CollectionSchemaSyncMsg, SyncFrame};
use super::state::SyncSession;

impl SyncSession {
    /// Materialize a peer-announced collection descriptor into the local
    /// catalog. Returns `None`: this is a fire-and-forget announce with no
    /// ack frame (mirrors `ShapeUnsubscribe`).
    pub fn handle_collection_schema(
        &mut self,
        msg: &CollectionSchemaSyncMsg,
        shared: Option<&Arc<SharedState>>,
    ) -> Option<SyncFrame> {
        let Some(shared) = shared else {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                "CollectionSchema received without SharedState (permissive/test path); dropping"
            );
            return None;
        };

        let Some(tenant) = self.tenant_id else {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                "CollectionSchema received before handshake established a tenant; dropping"
            );
            return None;
        };

        // Security: a peer must not materialize a collection into a tenant
        // other than the one it authenticated as.
        if msg.descriptor.tenant_id != tenant.as_u64() {
            warn!(
                session = %self.session_id,
                collection = %msg.descriptor.name,
                descriptor_tenant = msg.descriptor.tenant_id,
                session_tenant = tenant.as_u64(),
                "CollectionSchema tenant mismatch; refusing to materialize"
            );
            return None;
        }

        // Owner is the receiving peer's authenticated principal — the same
        // identity a local CREATE records as owner.
        let owner = self.username.as_deref().unwrap_or("sync");

        let stored =
            crate::control::security::catalog::collection_descriptor_convert::stored_from_descriptor(
                &msg.descriptor,
                owner,
            );

        let entry = CatalogEntry::PutCollectionIfAbsent(Box::new(stored));
        let log_index =
            match crate::control::metadata_proposer::propose_catalog_entry(shared, &entry) {
                Ok(idx) => idx,
                Err(e) => {
                    warn!(
                        session = %self.session_id,
                        collection = %msg.descriptor.name,
                        error = %e,
                        "CollectionSchema: failed to propose PutCollectionIfAbsent; \
                         collection not materialized"
                    );
                    return None;
                }
            };
        crate::control::catalog_entry::apply::local::apply_locally_if_needed(
            shared, &entry, log_index,
        );
        None
    }
}
