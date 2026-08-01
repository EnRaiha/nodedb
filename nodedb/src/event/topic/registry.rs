// SPDX-License-Identifier: BUSL-1.1

//! In-memory registry of durable topics.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::sync::{Mutex, broadcast};

use super::types::{TopicDef, TopicMessage};
use crate::types::DatabaseId;

const LIVE_TOPIC_CHANNEL_CAPACITY: usize = 1_024;

type TopicKey = (DatabaseId, u64, String);
type ScopeKey = (DatabaseId, u64);

struct TopicEntry {
    definition: TopicDef,
    live_sender: broadcast::Sender<Arc<TopicMessage>>,
}

impl TopicEntry {
    fn new(definition: TopicDef) -> Self {
        let (live_sender, _) = broadcast::channel(LIVE_TOPIC_CHANNEL_CAPACITY);
        Self {
            definition,
            live_sender,
        }
    }
}

/// In-memory topic registry.
pub struct EpTopicRegistry {
    by_name: RwLock<HashMap<TopicKey, TopicEntry>>,
    /// Lifecycle locks deliberately outlive runtime registrations. This keeps
    /// DROP/CREATE/PUBLISH and cleanup serialized across a remove/recreate.
    lifecycle_locks: RwLock<HashMap<TopicKey, Arc<Mutex<()>>>>,
    /// Bounded buses for pattern subscriptions, isolated by database and tenant.
    scoped_senders: RwLock<HashMap<ScopeKey, broadcast::Sender<Arc<TopicMessage>>>>,
}

impl EpTopicRegistry {
    pub fn new() -> Self {
        Self {
            by_name: RwLock::new(HashMap::new()),
            lifecycle_locks: RwLock::new(HashMap::new()),
            scoped_senders: RwLock::new(HashMap::new()),
        }
    }

    fn topic_key(database_id: DatabaseId, tenant_id: u64, name: &str) -> TopicKey {
        (database_id, tenant_id, name.to_owned())
    }

    fn scoped_sender(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> broadcast::Sender<Arc<TopicMessage>> {
        let key = (database_id, tenant_id);
        let mut senders = self
            .scoped_senders
            .write()
            .unwrap_or_else(|p| p.into_inner());
        senders
            .entry(key)
            .or_insert_with(|| broadcast::channel(LIVE_TOPIC_CHANNEL_CAPACITY).0)
            .clone()
    }

    pub fn register(&self, def: TopicDef) {
        let key = Self::topic_key(def.database_id, def.tenant_id, &def.name);
        // Allocate before registration so the lifecycle lock is stable even if
        // a concurrent DROP removes the entry immediately afterwards.
        self.lifecycle_lock(def.database_id, def.tenant_id, &def.name);
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        match map.get_mut(&key) {
            Some(entry) => entry.definition = def,
            None => {
                map.insert(key, TopicEntry::new(def));
            }
        }
    }

    pub fn unregister(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> bool {
        let key = Self::topic_key(database_id, tenant_id, name);
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        map.remove(&key).is_some()
    }

    pub fn get(&self, database_id: DatabaseId, tenant_id: u64, name: &str) -> Option<TopicDef> {
        let key = Self::topic_key(database_id, tenant_id, name);
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).map(|entry| entry.definition.clone())
    }

    /// Return the stable lock for one topic lifecycle, creating it if needed.
    pub fn lifecycle_lock(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> Arc<Mutex<()>> {
        let key = Self::topic_key(database_id, tenant_id, name);
        let mut locks = self
            .lifecycle_locks
            .write()
            .unwrap_or_else(|p| p.into_inner());
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Return the bounded live-message sender for one registered topic.
    pub fn sender(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> Option<broadcast::Sender<Arc<TopicMessage>>> {
        let key = Self::topic_key(database_id, tenant_id, name);
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.get(&key).map(|entry| entry.live_sender.clone())
    }

    /// Subscribe to messages for exactly one registered topic.
    pub fn subscribe(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> Option<broadcast::Receiver<Arc<TopicMessage>>> {
        self.sender(database_id, tenant_id, name)
            .map(|sender| sender.subscribe())
    }

    /// Subscribe to messages in one database and tenant for pattern matching.
    pub fn subscribe_scope(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> broadcast::Receiver<Arc<TopicMessage>> {
        self.scoped_sender(database_id, tenant_id).subscribe()
    }

    /// Broadcast one committed message to pattern subscribers in its scope.
    pub fn broadcast_committed(&self, message: Arc<TopicMessage>) {
        let _ = self
            .scoped_sender(message.database_id, message.tenant_id)
            .send(message);
    }

    /// Return the number of scoped pattern receivers.
    pub fn receiver_count(&self) -> usize {
        self.scoped_senders
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .map(broadcast::Sender::receiver_count)
            .sum()
    }

    pub fn topic_receiver_count(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> usize {
        self.sender(database_id, tenant_id, name)
            .map_or(0, |sender| sender.receiver_count())
    }

    pub fn list_for_database_tenant(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
    ) -> Vec<TopicDef> {
        let map = self.by_name.read().unwrap_or_else(|p| p.into_inner());
        map.values()
            .filter(|entry| {
                entry.definition.database_id == database_id
                    && entry.definition.tenant_id == tenant_id
            })
            .map(|entry| entry.definition.clone())
            .collect()
    }

    /// Load catalog definitions atomically into the registry.
    pub fn load_from_catalog(
        &self,
        catalog: &crate::control::security::catalog::types::SystemCatalog,
    ) -> crate::Result<()> {
        let topics = catalog.load_all_ep_topics()?;
        // Establish locks before taking the registry map lock; all paths then
        // acquire lifecycle state before runtime state.
        for topic in &topics {
            self.lifecycle_lock(topic.database_id, topic.tenant_id, &topic.name);
        }
        let mut map = self.by_name.write().unwrap_or_else(|p| p.into_inner());
        let mut loaded = HashMap::with_capacity(topics.len());
        for topic in topics {
            let key = Self::topic_key(topic.database_id, topic.tenant_id, &topic.name);
            let entry = match map.remove(&key) {
                Some(mut entry) => {
                    entry.definition = topic;
                    entry
                }
                None => TopicEntry::new(topic),
            };
            loaded.insert(key, entry);
        }
        *map = loaded;
        tracing::info!(count = map.len(), "loaded topics from catalog");
        Ok(())
    }
}

impl Default for EpTopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cdc::stream_def::RetentionConfig;

    fn definition(database_id: DatabaseId, tenant_id: u64, name: &str) -> TopicDef {
        TopicDef {
            database_id,
            tenant_id,
            name: name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        }
    }

    #[tokio::test]
    async fn live_buses_are_isolated_by_topic_and_scope() {
        let registry = EpTopicRegistry::new();
        let database_id = DatabaseId::new(7);
        registry.register(definition(database_id, 1, "events"));
        registry.register(definition(database_id, 1, "other"));
        registry.register(definition(database_id, 2, "events"));
        let mut exact = registry
            .subscribe(database_id, 1, "events")
            .expect("receiver");
        let mut scope = registry.subscribe_scope(database_id, 1);
        let message = Arc::new(TopicMessage {
            database_id,
            tenant_id: 1,
            topic: "other".into(),
            sequence: 1,
            event_time: 1,
            lsn: 1,
            payload: "{}".into(),
        });
        registry.broadcast_committed(Arc::clone(&message));
        assert!(matches!(
            exact.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert!(Arc::ptr_eq(
            &scope.recv().await.expect("scoped message"),
            &message
        ));
        assert_eq!(registry.topic_receiver_count(database_id, 2, "events"), 0);
    }

    #[tokio::test]
    async fn lifecycle_lock_survives_unregister() {
        let registry = EpTopicRegistry::new();
        let database_id = DatabaseId::new(7);
        registry.register(definition(database_id, 1, "events"));
        let lock = registry.lifecycle_lock(database_id, 1, "events");
        assert!(registry.unregister(database_id, 1, "events"));
        assert!(Arc::ptr_eq(
            &lock,
            &registry.lifecycle_lock(database_id, 1, "events")
        ));
    }
}
