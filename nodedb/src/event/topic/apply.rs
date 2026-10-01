// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed topic publication on this replica.
//!
//! In a cluster, `PUBLISH` proposes a `TopicPublish` entry to the data group
//! of the topic's home vShard. Every replica applies the entry in log order:
//! it appends the message to its own catalog at the entry's position, then
//! makes it visible to the topic's change buffer and live subscribers. Every
//! replica therefore holds the same messages with the same sequences and
//! positions, and a home change loses none of them.

use std::sync::Arc;

use crate::control::state::SharedState;
use crate::event::cdc::position::ReplicatedPosition;
use crate::event::topic::types::PublishOrigin;
use crate::types::DatabaseId;

/// One committed topic publication.
pub struct ReplicatedPublish<'a> {
    pub database_id: DatabaseId,
    pub tenant_id: u64,
    pub topic: &'a str,
    pub payload: &'a str,
    pub event_time: u64,
    pub position: ReplicatedPosition,
    /// The committed transaction message it delivers, if it is one.
    pub origin: Option<PublishOrigin>,
}

/// What applying a publication did on this replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatedPublishOutcome {
    /// The message was appended with this topic sequence.
    Appended(u64),
    /// An entry at or below this position was appended before, so this is a
    /// re-delivered entry. Or the topic already holds the committed message
    /// of the entry's origin.
    AlreadyApplied,
}

/// Append a committed publication and deliver it.
///
/// Fails with `UndefinedObject` when the topic does not exist here. Every
/// replica that applies the entry after the topic's drop reaches the same
/// verdict, so the caller treats it as the entry's final outcome.
pub fn apply_replicated_publish(
    state: &SharedState,
    publish: ReplicatedPublish<'_>,
) -> crate::Result<ReplicatedPublishOutcome> {
    let ReplicatedPublish {
        database_id,
        tenant_id,
        topic,
        payload,
        event_time,
        position,
        origin,
    } = publish;
    let Some(definition) = state.ep_topic_registry.get(database_id, tenant_id, topic) else {
        return Err(crate::Error::UndefinedObject {
            kind: "topic",
            name: topic.to_owned(),
        });
    };
    let Some(message) = state
        .credentials
        .catalog()
        .append_replicated_topic_message(
            (database_id, tenant_id, topic),
            payload,
            event_time,
            (position.epoch, position.log_index),
            origin.as_ref(),
        )?
    else {
        return Ok(ReplicatedPublishOutcome::AlreadyApplied);
    };
    let committed = Arc::new(message);
    state
        .cdc_router
        .ensure_buffer(
            database_id,
            tenant_id,
            &format!("topic:{topic}"),
            &definition.retention,
        )
        .push(Arc::new(committed.to_cdc_event()));
    if let Some(sender) = state
        .ep_topic_registry
        .sender(database_id, tenant_id, topic)
    {
        // No receivers is a normal durable-only publication.
        let _ = sender.send(Arc::clone(&committed));
    }
    state
        .ep_topic_registry
        .broadcast_committed(Arc::clone(&committed));
    Ok(ReplicatedPublishOutcome::Appended(committed.sequence))
}
