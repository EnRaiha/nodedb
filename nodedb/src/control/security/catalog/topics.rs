// SPDX-License-Identifier: BUSL-1.1

//! Durable topic metadata and message operations for the system catalog.

use std::collections::HashMap;

use redb::{ReadableDatabase, ReadableTable};
use std::time::{SystemTime, UNIX_EPOCH};

use super::consumer_groups::decode_consumer_group;
use super::types::{CONSUMER_GROUPS, SystemCatalog, TOPIC_MESSAGES, TOPICS_EP, catalog_err};
use crate::event::topic::{TopicDef, TopicMessage, validate_topic_name};
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Store a topic under an unambiguous database-scoped v2 key. Replacing a
    /// definition cannot move either durable high-water mark backwards.
    pub fn put_ep_topic(&self, def: &TopicDef) -> crate::Result<()> {
        validate_topic_name(&def.name).map_err(|error| catalog_err("put topic", error))?;
        let key = topic_key(def.database_id, def.tenant_id, &def.name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            let (existing, legacy) =
                find_topic_definition(&table, def.database_id, def.tenant_id, &def.name)?;
            let mut stored = def.clone();
            if let Some(existing) = existing {
                stored.last_sequence = stored.last_sequence.max(existing.last_sequence);
                stored.last_lsn = stored.last_lsn.max(existing.last_lsn);
            }
            let bytes =
                zerompk::to_msgpack_vec(&stored).map_err(|e| catalog_err("serialize topic", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert topic", e))?;
            if legacy {
                let legacy_key = legacy_topic_key(def.tenant_id, &def.name);
                table
                    .remove(legacy_key.as_str())
                    .map_err(|e| catalog_err("remove migrated legacy topic", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Insert a topic definition only if the durable identity is absent.
    ///
    /// The check and insert share a write transaction, so concurrent creators
    /// cannot both observe success.
    pub fn create_ep_topic(&self, def: &TopicDef) -> crate::Result<bool> {
        validate_topic_name(&def.name).map_err(|error| catalog_err("create topic", error))?;
        let key = topic_key(def.database_id, def.tenant_id, &def.name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("create topic txn", e))?;
        {
            let mut table = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            if find_topic_definition(&table, def.database_id, def.tenant_id, &def.name)?
                .0
                .is_some()
            {
                return Ok(false);
            }
            let bytes =
                zerompk::to_msgpack_vec(def).map_err(|e| catalog_err("serialize topic", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert topic", e))?;
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit create topic", e))?;
        Ok(true)
    }

    /// Append one exact payload to a topic and durably advance its high-water
    /// marks in the same transaction as retention pruning.
    pub fn append_ep_topic_message(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        topic: &str,
        payload: impl Into<String>,
        event_time: u64,
        lsn: u64,
    ) -> crate::Result<TopicMessage> {
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
            let (Some(mut def), legacy) =
                find_topic_definition(&definitions, database_id, tenant_id, topic)?
            else {
                return Err(catalog_err("append topic", "topic not found"));
            };
            let sequence = def
                .last_sequence
                .checked_add(1)
                .ok_or_else(|| catalog_err("append topic", "topic sequence overflow"))?;
            let message_lsn = lsn.max(def.last_lsn);
            message = TopicMessage {
                database_id,
                tenant_id,
                topic: topic.to_owned(),
                sequence,
                event_time,
                lsn: message_lsn,
                payload: payload.into(),
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
            let bytes =
                zerompk::to_msgpack_vec(&def).map_err(|e| catalog_err("serialize topic", e))?;
            definitions
                .insert(
                    topic_key(database_id, tenant_id, topic).as_str(),
                    bytes.as_slice(),
                )
                .map_err(|e| catalog_err("update topic high-water marks", e))?;
            if legacy {
                definitions
                    .remove(legacy_topic_key(tenant_id, topic).as_str())
                    .map_err(|e| catalog_err("remove migrated legacy topic", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit topic append", e))?;
        Ok(message)
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

    /// Delete a topic and every one of its durable messages atomically.
    /// Historical unscoped definitions are removed only for DEFAULT.
    pub fn delete_ep_topic(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        validate_topic_name(name).map_err(|error| catalog_err("delete topic", error))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let mut existed;
        {
            let mut definitions = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            existed = definitions
                .remove(topic_key(database_id, tenant_id, name).as_str())
                .map_err(|e| catalog_err("delete topic", e))?
                .is_some();
            if database_id == DatabaseId::DEFAULT {
                existed |= definitions
                    .remove(legacy_topic_key(tenant_id, name).as_str())
                    .map_err(|e| catalog_err("delete legacy topic", e))?
                    .is_some();
            }
            let mut messages = write_txn
                .open_table(TOPIC_MESSAGES)
                .map_err(|e| catalog_err("open topic_messages", e))?;
            let keys = scoped_message_keys(&messages, database_id, tenant_id, name)?;
            existed |= !keys.is_empty();
            for key in keys {
                messages
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("delete topic message", e))?;
            }
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Return all canonical and legacy consumer-group names attached to a topic.
    ///
    /// Callers use this before the cross-database offset cleanup so the offset
    /// store can be durably cleared before the catalog transaction commits.
    pub fn topic_consumer_group_names(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Vec<String>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read topic groups txn", e))?;
        let table = read_txn
            .open_table(CONSUMER_GROUPS)
            .map_err(|e| catalog_err("open consumer_groups", e))?;
        let canonical = format!("topic:{name}");
        let mut names = std::collections::BTreeSet::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range consumer_groups", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read consumer_group", e))?;
            let Some(mut group) = decode_consumer_group(value.value()) else {
                continue;
            };
            if !key.value().starts_with("v2:") {
                group.database_id = DatabaseId::DEFAULT;
            }
            if group.database_id == database_id
                && group.tenant_id == tenant_id
                && (group.stream_name == canonical || group.stream_name == name)
            {
                names.insert(group.name);
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Delete a topic, its messages, and every canonical or legacy topic
    /// consumer-group definition in one redb transaction. Offset deletion is
    /// intentionally coordinated by the caller before this transaction: the
    /// offset store is a separate database and this method must never expose a
    /// successful DROP with stale cursors left behind.
    pub fn delete_ep_topic_with_consumer_groups(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        validate_topic_name(name).map_err(|error| catalog_err("delete topic", error))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("delete topic txn", e))?;
        let mut existed = false;
        {
            let mut definitions = write_txn
                .open_table(TOPICS_EP)
                .map_err(|e| catalog_err("open topics_ep", e))?;
            existed |= definitions
                .remove(topic_key(database_id, tenant_id, name).as_str())
                .map_err(|e| catalog_err("delete topic", e))?
                .is_some();
            if database_id == DatabaseId::DEFAULT {
                existed |= definitions
                    .remove(legacy_topic_key(tenant_id, name).as_str())
                    .map_err(|e| catalog_err("delete legacy topic", e))?
                    .is_some();
            }
            let mut messages = write_txn
                .open_table(TOPIC_MESSAGES)
                .map_err(|e| catalog_err("open topic_messages", e))?;
            let keys = scoped_message_keys(&messages, database_id, tenant_id, name)?;
            existed |= !keys.is_empty();
            for key in keys {
                messages
                    .remove(key.as_slice())
                    .map_err(|e| catalog_err("delete topic message", e))?;
            }
            let mut groups = write_txn
                .open_table(CONSUMER_GROUPS)
                .map_err(|e| catalog_err("open consumer_groups", e))?;
            let canonical = format!("topic:{name}");
            let mut keys = Vec::new();
            for entry in groups
                .range::<&str>(..)
                .map_err(|e| catalog_err("range consumer_groups", e))?
            {
                let (key, value) = entry.map_err(|e| catalog_err("read consumer_group", e))?;
                let Some(mut group) = decode_consumer_group(value.value()) else {
                    continue;
                };
                if !key.value().starts_with("v2:") {
                    group.database_id = DatabaseId::DEFAULT;
                }
                if group.database_id == database_id
                    && group.tenant_id == tenant_id
                    && (group.stream_name == canonical || group.stream_name == name)
                {
                    keys.push(key.value().to_owned());
                }
            }
            for key in keys {
                groups
                    .remove(key.as_str())
                    .map_err(|e| catalog_err("delete topic consumer_group", e))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| catalog_err("commit topic deletion", e))?;
        Ok(existed)
    }

    /// Load every durable topic, preferring a v2 definition over an older
    /// DEFAULT-database row with the same logical identity.
    pub fn load_all_ep_topics(&self) -> crate::Result<Vec<TopicDef>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TOPICS_EP)
            .map_err(|e| catalog_err("open topics_ep", e))?;
        let mut topics = HashMap::new();
        for entry in table
            .range::<&str>(..)
            .map_err(|e| catalog_err("range topics_ep", e))?
        {
            let (key, value) = entry.map_err(|e| catalog_err("read topic", e))?;
            let is_v2 = key.value().starts_with("v2/");
            let mut def = decode_topic(value.value())?;
            if !is_v2 {
                def.database_id = DatabaseId::DEFAULT;
            }
            let identity = (def.database_id, def.tenant_id, def.name.clone());
            if is_v2 || !topics.contains_key(&identity) {
                topics.insert(identity, def);
            }
        }
        let mut topics: Vec<_> = topics.into_values().collect();
        topics.sort_by(|left, right| {
            (left.database_id.as_u64(), left.tenant_id, &left.name).cmp(&(
                right.database_id.as_u64(),
                right.tenant_id,
                &right.name,
            ))
        });
        Ok(topics)
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
            .range::<&[u8]>(..)
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
}

fn find_topic_definition(
    table: &redb::Table<&str, &[u8]>,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> crate::Result<(Option<TopicDef>, bool)> {
    let v2 = topic_key(database_id, tenant_id, name);
    if let Some(value) = table
        .get(v2.as_str())
        .map_err(|e| catalog_err("get topic", e))?
    {
        let def = decode_topic(value.value())?;
        validate_topic_identity(&def, database_id, tenant_id, name)?;
        return Ok((Some(def), false));
    }
    if database_id == DatabaseId::DEFAULT {
        let legacy = legacy_topic_key(tenant_id, name);
        if let Some(value) = table
            .get(legacy.as_str())
            .map_err(|e| catalog_err("get legacy topic", e))?
        {
            let mut def = decode_topic(value.value())?;
            def.database_id = DatabaseId::DEFAULT;
            validate_topic_identity(&def, database_id, tenant_id, name)?;
            return Ok((Some(def), true));
        }
    }
    Ok((None, false))
}

fn validate_topic_identity(
    def: &TopicDef,
    database_id: DatabaseId,
    tenant_id: u64,
    name: &str,
) -> crate::Result<()> {
    if (def.database_id, def.tenant_id, def.name.as_str()) != (database_id, tenant_id, name) {
        return Err(catalog_err(
            "decode topic",
            "definition identity does not match key",
        ));
    }
    Ok(())
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
        .range::<&[u8]>(..)
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

fn scoped_message_keys(
    table: &redb::Table<&[u8], &[u8]>,
    database_id: DatabaseId,
    tenant_id: u64,
    topic: &str,
) -> crate::Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    for entry in table
        .range::<&[u8]>(..)
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

fn topic_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> String {
    let mut encoded = String::with_capacity(name.len() * 2);
    for byte in name.as_bytes() {
        use std::fmt::Write;
        let _ = write!(&mut encoded, "{byte:02x}");
    }
    format!(
        "v2/{:016x}/{:016x}/{:08x}/{encoded}",
        database_id.as_u64(),
        tenant_id,
        name.len()
    )
}

fn legacy_topic_key(tenant_id: u64, name: &str) -> String {
    format!("{tenant_id}:{name}")
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

fn parse_topic_message_key(key: &[u8]) -> crate::Result<(DatabaseId, u64, String, u64)> {
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

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Positional wire shape written before topics adopted map encoding.
#[derive(zerompk::FromMessagePack, zerompk::ToMessagePack)]
#[msgpack(array)]
struct LegacyTopicDef {
    tenant_id: u64,
    name: String,
    retention: crate::event::cdc::stream_def::RetentionConfig,
    owner: String,
    created_at: u64,
}

impl From<LegacyTopicDef> for TopicDef {
    fn from(legacy: LegacyTopicDef) -> Self {
        Self {
            tenant_id: legacy.tenant_id,
            name: legacy.name,
            retention: legacy.retention,
            owner: legacy.owner,
            created_at: legacy.created_at,
            database_id: DatabaseId::DEFAULT,
            last_sequence: 0,
            last_lsn: 0,
        }
    }
}

fn decode_topic(bytes: &[u8]) -> crate::Result<TopicDef> {
    zerompk::from_msgpack(bytes)
        .or_else(|_| zerompk::from_msgpack::<LegacyTopicDef>(bytes).map(TopicDef::from))
        .map_err(|e| catalog_err("decode topic", e))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::event::cdc::consumer_group::ConsumerGroupDef;
    use crate::event::cdc::stream_def::RetentionConfig;

    fn catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().expect("tempdir");
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).expect("catalog");
        (dir, catalog)
    }

    fn topic(database_id: DatabaseId, tenant_id: u64, name: &str, max_events: u64) -> TopicDef {
        TopicDef {
            database_id,
            tenant_id,
            name: name.into(),
            retention: RetentionConfig {
                max_events,
                max_age_secs: 3_600,
            },
            owner: "admin".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        }
    }

    #[test]
    fn concurrent_appends_are_contiguous_and_survive_reopen() {
        let (dir, catalog) = catalog();
        catalog
            .put_ep_topic(&topic(DatabaseId::new(7), 1, "events", 100))
            .expect("topic");
        let catalog = Arc::new(catalog);
        let mut workers = Vec::new();
        for number in 0..16 {
            let catalog = Arc::clone(&catalog);
            workers.push(std::thread::spawn(move || {
                catalog.append_ep_topic_message(
                    DatabaseId::new(7),
                    1,
                    "events",
                    number.to_string(),
                    current_time_ms(),
                    number,
                )
            }));
        }
        for worker in workers {
            worker.join().expect("worker").expect("append");
        }
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::new(7), 1, "events")
            .expect("load");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            (1..=16).collect::<Vec<_>>()
        );
        drop(catalog);
        let reopened = SystemCatalog::open(&dir.path().join("system.redb")).expect("reopen");
        assert_eq!(
            reopened
                .load_ep_topic_messages(DatabaseId::new(7), 1, "events")
                .expect("reload")
                .len(),
            16
        );
    }

    #[test]
    fn concurrent_creates_have_one_durable_winner_per_scope() {
        let (_dir, catalog) = catalog();
        let catalog = Arc::new(catalog);
        let mut workers = Vec::new();
        for _ in 0..16 {
            let catalog = Arc::clone(&catalog);
            workers.push(std::thread::spawn(move || {
                catalog.create_ep_topic(&topic(DatabaseId::new(7), 1, "events", 100))
            }));
        }
        let successes = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker").expect("create"))
            .filter(|created| *created)
            .count();
        assert_eq!(successes, 1);
        assert_eq!(catalog.load_all_ep_topics().expect("topics").len(), 1);
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::new(8), 1, "events", 100))
                .expect("other database")
        );
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::new(7), 2, "events", 100))
                .expect("other tenant")
        );
    }

    #[test]
    fn catalog_rejects_invalid_or_oversized_topic_names() {
        let (_dir, catalog) = catalog();
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::DEFAULT, 1, "1invalid", 1))
                .is_err()
        );
        assert!(
            catalog
                .create_ep_topic(&topic(DatabaseId::DEFAULT, 1, &"a".repeat(257), 1))
                .is_err()
        );
    }

    #[test]
    fn retention_prunes_messages_without_moving_high_water_marks_backwards() {
        let (_dir, catalog) = catalog();
        catalog
            .put_ep_topic(&topic(DatabaseId::DEFAULT, 1, "events", 2))
            .expect("topic");
        let now = current_time_ms();
        for sequence in 1..=3 {
            catalog
                .append_ep_topic_message(DatabaseId::DEFAULT, 1, "events", "{}", now, sequence)
                .expect("append");
        }
        let messages = catalog
            .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
            .expect("load");
        assert_eq!(
            messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (3, 3));
        let replacement = topic(DatabaseId::DEFAULT, 1, "events", 2);
        catalog.put_ep_topic(&replacement).expect("replace");
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (3, 3));
    }

    #[test]
    fn max_age_prunes_expired_messages() {
        let (_dir, catalog) = catalog();
        let mut definition = topic(DatabaseId::DEFAULT, 1, "events", 10);
        definition.retention.max_age_secs = 1;
        catalog.put_ep_topic(&definition).expect("topic");
        catalog
            .append_ep_topic_message(DatabaseId::DEFAULT, 1, "events", "old", 0, 1)
            .expect("append");
        assert!(
            catalog
                .load_ep_topic_messages(DatabaseId::DEFAULT, 1, "events")
                .expect("load")
                .is_empty()
        );
        let definition = catalog
            .load_all_ep_topics()
            .expect("definitions")
            .pop()
            .expect("definition");
        assert_eq!((definition.last_sequence, definition.last_lsn), (1, 1));
    }

    #[test]
    fn topic_drop_transaction_removes_canonical_and_legacy_groups() {
        let (_dir, catalog) = catalog();
        let database_id = DatabaseId::new(7);
        catalog
            .create_ep_topic(&topic(database_id, 1, "events", 10))
            .expect("create");
        for stream_name in ["topic:events", "events"] {
            catalog
                .put_consumer_group(&ConsumerGroupDef {
                    database_id,
                    tenant_id: 1,
                    name: format!("group_{}", stream_name.replace(':', "_")),
                    stream_name: stream_name.into(),
                    owner: "admin".into(),
                    created_at: 0,
                })
                .expect("group");
        }
        catalog
            .append_ep_topic_message(database_id, 1, "events", "before", current_time_ms(), 1)
            .expect("message");
        assert_eq!(
            catalog
                .topic_consumer_group_names(database_id, 1, "events")
                .expect("names")
                .len(),
            2
        );
        assert!(
            catalog
                .delete_ep_topic_with_consumer_groups(database_id, 1, "events")
                .expect("drop")
        );
        assert!(
            catalog
                .load_ep_topic_messages(database_id, 1, "events")
                .expect("messages")
                .is_empty()
        );
        assert!(
            catalog
                .topic_consumer_group_names(database_id, 1, "events")
                .expect("names")
                .is_empty()
        );
    }

    #[test]
    fn drop_then_recreate_has_a_fresh_durable_lifecycle() {
        let (_dir, catalog) = catalog();
        let database_id = DatabaseId::new(7);
        catalog
            .create_ep_topic(&topic(database_id, 1, "events", 10))
            .expect("create");
        catalog
            .append_ep_topic_message(database_id, 1, "events", "before", current_time_ms(), 1)
            .expect("append");
        assert!(
            catalog
                .delete_ep_topic(database_id, 1, "events")
                .expect("drop")
        );
        assert!(
            catalog
                .create_ep_topic(&topic(database_id, 1, "events", 10))
                .expect("recreate")
        );
        let message = catalog
            .append_ep_topic_message(database_id, 1, "events", "after", current_time_ms(), 1)
            .expect("append recreated");
        assert_eq!(message.sequence, 1);
        assert_eq!(
            catalog
                .load_ep_topic_messages(database_id, 1, "events")
                .expect("messages")
                .len(),
            1
        );
    }

    #[test]
    fn message_scopes_are_isolated_and_delete_is_atomic_for_one_scope() {
        let (_dir, catalog) = catalog();
        let first = (DatabaseId::new(1), 1, "events");
        let second = (DatabaseId::new(2), 1, "events");
        catalog
            .put_ep_topic(&topic(first.0, first.1, first.2, 10))
            .expect("first topic");
        catalog
            .put_ep_topic(&topic(second.0, second.1, second.2, 10))
            .expect("second topic");
        let now = current_time_ms();
        catalog
            .append_ep_topic_message(first.0, first.1, first.2, "one", now, 1)
            .expect("first append");
        catalog
            .append_ep_topic_message(second.0, second.1, second.2, "two", now, 1)
            .expect("second append");
        assert!(
            catalog
                .delete_ep_topic(first.0, first.1, first.2)
                .expect("delete")
        );
        assert!(
            catalog
                .load_ep_topic_messages(first.0, first.1, first.2)
                .expect("first load")
                .is_empty()
        );
        assert_eq!(
            catalog
                .load_ep_topic_messages(second.0, second.1, second.2)
                .expect("second load")
                .len(),
            1
        );
    }

    #[test]
    fn appending_a_legacy_default_topic_migrates_its_high_water_marks() {
        let (_dir, catalog) = catalog();
        let legacy = LegacyTopicDef {
            tenant_id: 1,
            name: "events".into(),
            retention: RetentionConfig::default(),
            owner: "admin".into(),
            created_at: 0,
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).expect("legacy bytes");
        let txn = catalog.db.begin_write().expect("txn");
        {
            let mut table = txn.open_table(TOPICS_EP).expect("topics");
            table
                .insert(legacy_topic_key(1, "events").as_str(), bytes.as_slice())
                .expect("legacy insert");
        }
        txn.commit().expect("commit");
        let message = catalog
            .append_ep_topic_message(
                DatabaseId::DEFAULT,
                1,
                "events",
                "raw",
                current_time_ms(),
                4,
            )
            .expect("append");
        assert_eq!((message.sequence, message.lsn), (1, 4));
        let loaded = catalog.load_all_ep_topics().expect("definitions");
        assert_eq!((loaded[0].last_sequence, loaded[0].last_lsn), (1, 4));
    }
}
