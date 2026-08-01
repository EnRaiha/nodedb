// SPDX-License-Identifier: BUSL-1.1

//! Startup hydration of durable topic messages into CDC buffers.

use crate::control::state::SharedState;

/// Load every durably retained topic message into its canonical CDC buffer.
///
/// The catalog is read and every message is validated against an already-loaded
/// runtime topic definition before any buffer is changed. Hydration deliberately
/// does not use the live topic sender: replaying retained messages on startup
/// must not look like a new publication to live subscribers.
pub fn hydrate_topic_buffers(state: &SharedState) -> crate::Result<usize> {
    let mut messages = state.credentials.catalog().load_all_ep_topic_messages()?;
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

    // Validate the complete durable set first. Returning an error prevents the
    // state constructor from exposing a partially hydrated runtime.
    for message in &messages {
        if state
            .ep_topic_registry
            .get(message.database_id, message.tenant_id, &message.topic)
            .is_none()
        {
            return Err(crate::Error::Storage {
                engine: "topic".into(),
                detail: format!(
                    "durable message for undefined topic database={} tenant={} topic={}",
                    message.database_id.as_u64(),
                    message.tenant_id,
                    message.topic,
                ),
            });
        }
    }

    for message in &messages {
        let definition = state
            .ep_topic_registry
            .get(message.database_id, message.tenant_id, &message.topic)
            .ok_or_else(|| crate::Error::Storage {
                engine: "topic".into(),
                detail: format!(
                    "durable message for undefined topic database={} tenant={} topic={}",
                    message.database_id.as_u64(),
                    message.tenant_id,
                    message.topic,
                ),
            })?;
        state
            .cdc_router
            .ensure_buffer(
                message.database_id,
                message.tenant_id,
                &format!("topic:{}", message.topic),
                &definition.retention,
            )
            .push(std::sync::Arc::new(message.to_cdc_event()));
    }

    Ok(messages.len())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::hydrate_topic_buffers;
    use crate::control::security::credential::CredentialStore;
    use crate::control::state::SharedState;
    use crate::event::cdc::CdcOffset;
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::{TopicDef, publish_to_topic};
    use crate::types::DatabaseId;
    use crate::wal::WalManager;

    #[tokio::test(flavor = "current_thread")]
    async fn restart_hydrates_retained_messages_and_resumes_topic_hwm() {
        let directory = tempfile::tempdir().expect("tempdir");
        let catalog_path = directory.path().join("catalog.redb");
        let wal_dir = directory.path().join("wal");
        std::fs::create_dir_all(&wal_dir).expect("wal directory");
        let wal = Arc::new(WalManager::open_for_testing(&wal_dir).expect("wal"));
        let database_id = DatabaseId::new(7);
        let tenant_id = 11;
        let topic_name = "events";
        let definition = TopicDef {
            database_id,
            tenant_id,
            name: topic_name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
        };

        {
            let credentials = Arc::new(CredentialStore::open(&catalog_path).expect("catalog"));
            credentials
                .catalog()
                .put_ep_topic(&definition)
                .expect("persist topic");
            let (dispatcher, _) = crate::bridge::dispatch::Dispatcher::new(1, 16);
            let state =
                SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials)
                    .expect("state");
            assert_eq!(
                publish_to_topic(
                    &state,
                    database_id,
                    tenant_id,
                    topic_name,
                    r#"{"message":"one"}"#
                )
                .await
                .expect("publish one"),
                1
            );
            assert_eq!(
                publish_to_topic(
                    &state,
                    database_id,
                    tenant_id,
                    topic_name,
                    r#"{"message":"two"}"#
                )
                .await
                .expect("publish two"),
                2
            );
        }

        let credentials = Arc::new(CredentialStore::open(&catalog_path).expect("reopen catalog"));
        let (dispatcher, _) = crate::bridge::dispatch::Dispatcher::new(1, 16);
        let state = SharedState::new_with_credentials(dispatcher, Arc::clone(&wal), credentials)
            .expect("restarted state");
        let buffer = state
            .cdc_router
            .get_buffer(database_id, tenant_id, "topic:events")
            .expect("hydrated topic buffer");
        let messages = buffer.read_from(CdcOffset::ZERO, 16);
        assert_eq!(
            messages
                .iter()
                .map(|message| message.sequence)
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(
            messages
                .iter()
                .map(|message| message.new_value.as_ref().expect("payload")["message"].as_str())
                .collect::<Vec<_>>(),
            [Some("one"), Some("two")]
        );
        assert_eq!(
            buffer
                .partition_tails()
                .get(&0)
                .copied()
                .expect("topic hwm")
                .sequence,
            2
        );
        assert_eq!(
            publish_to_topic(
                &state,
                database_id,
                tenant_id,
                topic_name,
                r#"{"message":"three"}"#
            )
            .await
            .expect("publish after restart"),
            3
        );
        assert_eq!(
            buffer
                .partition_tails()
                .get(&0)
                .copied()
                .expect("new topic hwm")
                .sequence,
            3
        );

        // Explicit rehydration is idempotent and does not broadcast retained data.
        let mut live = state
            .ep_topic_registry
            .subscribe(database_id, tenant_id, topic_name)
            .expect("live receiver");
        assert_eq!(hydrate_topic_buffers(&state).expect("repeat hydration"), 3);
        assert!(matches!(
            live.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }
}
