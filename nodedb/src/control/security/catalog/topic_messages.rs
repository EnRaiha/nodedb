// SPDX-License-Identifier: BUSL-1.1

//! Durable topic messages: append, load, and retention pruning.
//!
//! Every replica of the topic's home vShard appends each published message at
//! apply, at the Raft entry's position, so every replica holds the same
//! messages with the same sequences.
//!
//! A committed transaction's message carries its origin. The append claims
//! the origin in the same transaction (see `topic_publish_marks`), so a topic
//! holds each committed message once.

use redb::{ReadableDatabase, ReadableTable};
use std::time::{SystemTime, UNIX_EPOCH};

use super::topic_publish_marks::claim_origin;
use super::topics::find_topic_definition;
use super::topics::topic_key;
use super::types::{SystemCatalog, TOPIC_MESSAGES, TOPICS_EP, catalog_err};
use crate::event::topic::types::PublishOrigin;
use crate::event::topic::{TopicDef, TopicMessage, validate_topic_name};
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Append one message at the Raft entry `(epoch, index)` that carries it,
    /// and durably advance the topic's high-water marks in the same
    /// transaction as retention pruning.
    ///
    /// Every replica applies the same entries in log order, so each assigns
    /// the same sequence. An entry at or below the topic's applied position
    /// returns `None`: a re-delivered entry appends nothing twice. So does a
    /// message of an `origin` the topic already holds.
    pub fn append_replicated_topic_message(
        &self,
        scope: (DatabaseId, u64, &str),
        payload: impl Into<String>,
        event_time: u64,
        entry: (u64, u64),
        origin: Option<&PublishOrigin>,
    ) -> crate::Result<Option<TopicMessage>> {
        self.append_topic_message(scope, payload.into(), event_time, entry, origin)
    }

    /// Load messages for one exact `(database, tenant, topic)` identity.
    pub fn load_ep_topic_messages(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        topic: &str,
    ) -> crate::Result<Vec<TopicMessage>> {
        validate_topic_name(topic).map_err(|error| catalog_err("load topic messages", error))?;
        self.load_topic_messages(Some((database_id, tenant_id, topic)))
    }

    /// Load messages for every topic, sorted by scope and sequence.
    pub fn load_all_ep_topic_messages(&self) -> crate::Result<Vec<TopicMessage>> {
        self.load_topic_messages(None)
    }

    fn load_topic_messages(
        &self,
        scope: Option<(DatabaseId, u64, &str)>,
    ) -> crate::Result<Vec<TopicMessage>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read topic messages txn", e))?;
        let table = read_txn
            .open_table(TOPIC_MESSAGES)
            .map_err(|e| catalog_err("open topic_messages", e))?;
        let mut messages = Vec::new();
        for entry in table
            .range(..)
            .map_err(|e| catalog_err("range topic_messages", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read topic message", e))?;
            let (database_id, tenant_id, topic, sequence) = parse_topic_message_key(key.value())?;
            let message: TopicMessage = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode topic message", e))?;
            if (
                message.database_id,
                message.tenant_id,
                message.topic.as_str(),
                message.sequence,
            ) != (database_id, tenant_id, topic.as_str(), sequence)
            {
                return Err(catalog_err(
                    "decode topic message",
                    "message identity does not match key",
                ));
            }
            if scope.is_none_or(|(db, tenant, name)| {
                (db, tenant, name) == (database_id, tenant_id, topic.as_str())
            }) {
                messages.push(message);
            }
        }
        messages.sort_by(|left, right| {
            (
                left.database_id.as_u64(),
                left.tenant_id,
                &left.topic,
                left.sequence,
            )
                .cmp(&(
                    right.database_id.as_u64(),
                    right.tenant_id,
                    &right.topic,
                    right.sequence,
                ))
        });
        Ok(messages)
    }

    fn append_topic_message(
        &self,
        (database_id, tenant_id, topic): (DatabaseId, u64, &str),
        payload: String,
        event_time: u64,
        (epoch, index): (u64, u64),
        origin: Option<&PublishOrigin>,
    ) -> crate::Result<Option<TopicMessage>> {
        validate_topic_name(topic).map_err(|error| catalog_err("append topic", error))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("append topic txn", e))?;
        let message;
        {
            let mut definitions = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            let Some(mut def) = find_topic_definition(&definitions, database_id, tenant_id, topic)?
            else {
                return Err(catalog_err("append topic", "topic not found"));
            };
            if (epoch, index) <= (def.last_epoch, def.last_lsn) {
                return Ok(None);
            }
            let message_lsn = index;
            // A committed message the topic already holds, delivered again.
            if let Some(origin) = origin
                && !claim_origin(&write_txn, (database_id, tenant_id, topic), origin)?
            {
                return Ok(None);
            }
            let sequence = def
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| catalog_err("append topic", "topic sequence overflow"))?;
            message = TopicMessage {
                database_id,
                tenant_id,
                topic: topic.to_owned(),
                sequence,
                event_time,
                lsn: message_lsn,
                epoch,
                payload,
            };
            let bytes = zerompk::to_msgpack_vec(&message)
                .map_err(|e| catalog_err("serialize topic message", e))?;
            {
                let mut messages = write_txn
                    .open_table(TOPIC_MESSAGES)
                    .map_err(|e| catalog_err("open topic_messages", e))?;
                let key = topic_message_key(database_id, tenant_id, topic, sequence)?;
                messages
                    .insert(key.as_slice(), bytes.as_slice())
                    .map_err(|e| catalog_err("insert topic message", e))?;
                prune_topic_messages(&mut messages, &def, database_id, tenant_id, topic)?;
            }
            def.last_sequence = sequence;
            def.last_lsn = message_lsn;
            def.last_epoch = epoch;
            let bytes =
                zerompk::to_msgpack_vec(&def).map_err(|e| catalog_err("serialize topic", e))?;
            definitions
                .insert(
                    topic_key(database_id, tenant_id, topic).as_str(),
                    bytes.as_slice(),
                )
                .map_err(|e| catalog_err("update topic high-water marks", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit topic append", e))?;
        Ok(Some(message))
    }
}

fn prune_topic_messages(
    table: &mut redb::Table<&[u8], &[u8]>,
    def: &TopicDef,
    database_id: DatabaseId,
    tenant_id: u64,
    topic: &str,
) -> crate::Result<()> {
    let cutoff = current_time_ms().saturating_sub(def.retention.max_age_secs.saturating_mul(1_000));
    let mut messages = Vec::new();
    for entry in table
        .range(..)
        .map_err(|e| catalog_err("range topic_messages", e))?
    {
        let (key, value) = entry.map_err(|e| catalog_err("read topic message", e))?;
        let (db, tenant, stored_topic, sequence) = parse_topic_message_key(key.value())?;
        if (db, tenant, stored_topic.as_str()) == (database_id, tenant_id, topic) {
            let message: TopicMessage = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode topic message", e))?;
            if (
                message.database_id,
                message.tenant_id,
                message.topic.as_str(),
                message.sequence,
            ) != (db, tenant, stored_topic.as_str(), sequence)
            {
                return Err(catalog_err(
                    "decode topic message",
                    "message identity does not match key",
                ));
            }
            messages.push((key.value().to_vec(), message));
        }
    }
    messages.sort_by_key(|(_, message)| message.sequence);
    let mut remove: Vec<Vec<u8>> = messages
        .iter()
        .filter(|(_, message)| message.event_time < cutoff)
        .map(|(key, _)| key.clone())
        .collect();
    let retained: Vec<_> = messages
        .into_iter()
        .filter(|(key, _)| !remove.iter().any(|removed| removed == key))
        .collect();
    let overflow = retained
        .len()
        .saturating_sub(def.retention.max_events as usize);
    remove.extend(retained.into_iter().take(overflow).map(|(key, _)| key));
    for key in remove {
        table
            .remove(key.as_slice())
            .map_err(|e| catalog_err("prune topic message", e))?;
    }
    Ok(())
}

pub(super) fn scoped_message_keys(
    table: &redb::Table<&[u8], &[u8]>,
    database_id: DatabaseId,
    tenant_id: u64,
    topic: &str,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for entry in table
        .range(..)
        .map_err(|e| catalog_err("range topic_messages", e))?
    {
        let (key, value) = entry.map_err(|e| catalog_err("read topic message", e))?;
        let (db, tenant, stored_topic, sequence) = parse_topic_message_key(key.value())?;
        if (db, tenant, stored_topic.as_str()) == (database_id, tenant_id, topic) {
            let message: TopicMessage = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("decode topic message", e))?;
            if (
                message.database_id,
                message.tenant_id,
                message.topic.as_str(),
                message.sequence,
            ) != (db, tenant, stored_topic.as_str(), sequence)
            {
                return Err(catalog_err(
                    "decode topic message",
                    "message identity does not match key",
                ));
            }
            keys.push(key.value().to_vec());
        }
    }
    Ok(keys)
}

fn topic_message_key(
    database_id: DatabaseId,
    tenant_id: u64,
    topic: &str,
    sequence: u64,
) -> crate::Result<Vec<u8>> {
    let name_len: u16 = topic
        .len()
        .try_into()
        .map_err(|_| catalog_err("topic message key", "topic name exceeds u16 length"))?;
    let mut key = Vec::with_capacity(26 + topic.len());
    key.extend_from_slice(&database_id.as_u64().to_be_bytes());
    key.extend_from_slice(&tenant_id.to_be_bytes());
    key.extend_from_slice(&name_len.to_be_bytes());
    key.extend_from_slice(topic.as_bytes());
    key.extend_from_slice(&sequence.to_be_bytes());
    Ok(key)
}

pub(super) fn parse_topic_message_key(key: &[u8]) -> crate::Result<(DatabaseId, u64, String, u64)> {
    if key.len() < 26 {
        return Err(catalog_err(
            "topic message key",
            "key is shorter than fixed fields",
        ));
    }
    let database_id = DatabaseId::new(u64::from_be_bytes(
        key[..8]
            .try_into()
            .map_err(|_| catalog_err("topic message key", "invalid database id"))?,
    ));
    let tenant_id = u64::from_be_bytes(
        key[8..16]
            .try_into()
            .map_err(|_| catalog_err("topic message key", "invalid tenant id"))?,
    );
    let name_len = u16::from_be_bytes(
        key[16..18]
            .try_into()
            .map_err(|_| catalog_err("topic message key", "invalid name length"))?,
    ) as usize;
    if key.len() != 26 + name_len {
        return Err(catalog_err(
            "topic message key",
            "key length does not match topic name",
        ));
    }
    let topic = std::str::from_utf8(&key[18..18 + name_len])
        .map_err(|e| catalog_err("topic message key", e))?
        .to_owned();
    let sequence = u64::from_be_bytes(
        key[18 + name_len..]
            .try_into()
            .map_err(|_| catalog_err("topic message key", "invalid sequence"))?,
    );
    Ok((database_id, tenant_id, topic, sequence))
}

pub(super) fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::stream_def::RetentionConfig;

    fn catalog_with_topic() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        catalog
            .put_ep_topic(&TopicDef {
                database_id: DatabaseId::DEFAULT,
                tenant_id: 1,
                name: "events".into(),
                retention: RetentionConfig {
                    max_events: 100,
                    max_age_secs: 3_600,
                },
                owner: "admin".into(),
                created_at: 0,
                last_sequence: 0,
                last_lsn: 0,
                last_epoch: 0,
                modification_hlc: nodedb_types::Hlc::ZERO,
            })
            .expect("put topic");
        (dir, catalog)
    }

    #[test]
    fn a_redelivered_entry_appends_nothing_twice() {
        let (_dir, catalog) = catalog_with_topic();
        let append = |index| {
            catalog
                .append_replicated_topic_message(
                    (DatabaseId::DEFAULT, 1, "events"),
                    "{}",
                    current_time_ms(),
                    (0, index),
                    None,
                )
                .expect("append")
        };
        let first = append(10).expect("first entry appends");
        let second = append(11).expect("second entry appends");
        assert_eq!((first.sequence, first.lsn), (1, 10));
        assert_eq!((second.sequence, second.lsn), (2, 11));
        assert!(append(11).is_none());
        assert!(append(10).is_none());
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
            .expect("load");
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn a_higher_epoch_appends_at_a_lower_index() {
        let (_dir, catalog) = catalog_with_topic();
        let append = |epoch, index| {
            catalog
                .append_replicated_topic_message(
                    (DatabaseId::DEFAULT, 1, "events"),
                    "{}",
                    current_time_ms(),
                    (epoch, index),
                    None,
                )
                .expect("append")
        };
        assert!(append(0, 500).is_some());
        let moved = append(7, 2).expect("new group's entry appends");
        assert_eq!((moved.epoch, moved.lsn, moved.sequence), (7, 2, 2));
    }

    /// A committed message delivered again, by a later lease holder at a
    /// later entry, appends nothing.
    #[test]
    fn a_committed_message_is_appended_once_per_origin() {
        let (_dir, catalog) = catalog_with_topic();
        let origin = PublishOrigin {
            partition: 3,
            position: crate::event::cdc::CdcOffset::data_event(0, 40, 1),
        };
        let replicated = |index| {
            catalog
                .append_replicated_topic_message(
                    (DatabaseId::DEFAULT, 1, "events"),
                    "{}",
                    current_time_ms(),
                    (0, index),
                    Some(&origin),
                )
                .expect("append")
        };
        assert!(replicated(10).is_some());
        assert!(replicated(11).is_none(), "a second entry of one origin");
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
            .expect("load");
        assert_eq!(messages.len(), 1);
    }
}
