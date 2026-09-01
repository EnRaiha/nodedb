// SPDX-License-Identifier: BUSL-1.1

//! Replicated writes for the consumer-group DDL handlers.
//!
//! Every mutation of `_system.consumer_groups` proposes a `CatalogEntry`, so
//! each node writes the row and installs it in its own `GroupRegistry`. A group
//! created on one node resolves on all.

use crate::control::catalog_entry::apply::consumer_group as apply;
use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::catalog_entry::post_apply::consumer_group as post_apply;
use crate::control::state::SharedState;
use crate::event::cdc::consumer_group::ConsumerGroupDef;
use crate::types::DatabaseId;

use super::super::super::result::DdlError;
use super::super::replicate::propose_and_apply;

/// Propose the group definition. The leader reports the duplicate before
/// proposing, so apply is a create-only write that never rejects.
pub(super) fn propose_create(state: &SharedState, def: &ConsumerGroupDef) -> Result<(), DdlError> {
    let entry = CatalogEntry::PutConsumerGroupIfAbsent(Box::new(def.clone()));
    propose_and_apply(state, &entry, || {
        apply::put_if_absent(def, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        post_apply::put_if_absent(def, state);
        Ok(())
    })
}

/// Propose removal of the group row, its registration, and its durable offsets
/// on every node.
pub(super) fn propose_delete(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: u64,
    stream_name: &str,
    name: &str,
) -> Result<(), DdlError> {
    let database_id = database_id.as_u64();
    let entry = CatalogEntry::DeleteConsumerGroup {
        database_id,
        tenant_id,
        stream_name: stream_name.to_string(),
        name: name.to_string(),
    };
    propose_and_apply(state, &entry, || {
        apply::delete(
            database_id,
            tenant_id,
            stream_name,
            name,
            state.credentials.catalog(),
        )
        .map_err(|e| DdlError::new("XX000", format!("catalog delete: {e}")))?;
        post_apply::delete(database_id, tenant_id, stream_name, name, state);
        Ok(())
    })
}

/// Propose the re-key of a legacy bare-topic group onto its canonical stream.
///
/// The caller moves the durable offsets first: they live in a separate
/// database this entry cannot carry.
pub(super) fn propose_migrate(
    state: &SharedState,
    def: &ConsumerGroupDef,
    legacy_stream: &str,
) -> Result<(), DdlError> {
    let entry = CatalogEntry::MigrateConsumerGroupStream {
        def: Box::new(def.clone()),
        legacy_stream: legacy_stream.to_string(),
    };
    propose_and_apply(state, &entry, || {
        apply::migrate_stream(def, legacy_stream, state.credentials.catalog())
            .map_err(|e| DdlError::new("XX000", format!("catalog write: {e}")))?;
        post_apply::migrate_stream(def, legacy_stream, state);
        Ok(())
    })
}
