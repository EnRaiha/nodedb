// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the durable-topic DDL handlers.
//!
//! Every mutation of `_system.topics_ep` proposes a `CatalogEntry`, so each
//! node writes the row and installs it in its own `EpTopicRegistry`. A topic
//! created on one node is publishable on all.

use crate::control::catalog_entry::apply::topic as apply;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::topic as post_apply;
use crate::control::state::SharedState;
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply;

/// Propose the topic definition. The leader checks the name and the duplicate
/// before proposing, so apply is a create-only write that never rejects.
pub(super) fn propose_create(state: &SharedState, def: &TopicDef) -> Result<(), DdlError> {
    let entry = CatalogEntry::CreateTopicIfAbsent(Box::new(def.clone()));
    propose_and_apply(state, &entry, || {
        apply::create_if_absent(def, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        post_apply::create_if_absent(def, state);
        Ok(())
    })
}

/// Propose removal of the topic row, its retained messages, and every attached
/// consumer group on every node.
///
/// The caller clears the durable offsets first: they live in a separate
/// database this entry cannot carry.
pub(super) fn propose_delete(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> Result<(), DdlError> {
    let database_id = database_id.as_u64();
    let entry = CatalogEntry::DeleteTopicWithConsumerGroups {
        database_id,
        tenant_id,
        name: name.to_string(),
    };
    propose_and_apply(state, &entry, || {
        apply::delete_with_consumer_groups(
            database_id,
            tenant_id,
            name,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog delete: {e}")))?;
        post_apply::delete_with_consumer_groups(database_id, tenant_id, name, state);
        Ok(())
    })
}
