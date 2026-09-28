// SPDX-License-Identifier: BUSL-1.1

//! Point read of one durable topic definition.

use redb::{ReadableDatabase, ReadableTable};

use super::topics::{decode_topic, topic_key, validate_topic_identity};
use super::types::{SystemCatalog, TOPICS_EP, catalog_err};
use crate::event::topic::TopicDef;
use crate::types::DatabaseId;

impl SystemCatalog {
    /// Committed-only read of one topic definition.
    pub fn get_committed_ep_topic(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<TopicDef>> {
        let key = topic_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(TOPICS_EP)
            .map_err(|e| catalog_err("open topics_ep", e))?;
        let Some(value) = table
            .get(key.as_str())
            .map_err(|e| catalog_err("get topic", e))?
        else {
            return Ok(None);
        };
        let def = decode_topic(value.value())?;
        validate_topic_identity(&def, database_id, tenant_id, name)?;
        Ok(Some(def))
    }
}
