// SPDX-License-Identifier: BUSL-1.1

//! Kafka producer: publishes CDC events to external Kafka topics.
//!
//! One background Tokio task per Kafka-delivery stream. Consumes from the
//! stream's `StreamBuffer` via an internal consumer group, serializes events,
//! and publishes to the configured Kafka topic using rdkafka's `FutureProducer`.
//!
//! **Exactly-once:** When `transactional = true`, uses Kafka's idempotent
//! producer (`enable.idempotence = true`) with a `transactional.id` per stream.
//! Each batch is wrapped in `begin_transaction` / `commit_transaction`.
//!
//! In a cluster only the node that holds the leader lease of the stream's
//! owning group publishes. It reads every partition, from its own buffer and
//! from the members of groups it does not replicate. It checks the lease
//! right before each send and right before each offset commit. Each record's
//! key is the event's `<partition>:<epoch>:<index>:<sequence>`, the same for
//! every owner and retry. Its `fencing-token` header is the owner's lease
//! term, which rises with each new owner (see
//! [`crate::event::cdc::sink_owner`]).

use sonic_rs;

use std::sync::Arc;
use std::time::Duration;

use rdkafka::producer::Producer;
use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use super::config::KafkaDeliveryConfig;
use crate::control::state::SharedState;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::consume::{ConsumeParams, consume_for_sink};
use crate::event::cdc::sink_owner::{SinkFence, kafka_group, register_sink_groups, sink_lease};

/// Spawn a background Kafka producer task for a change stream.
///
/// Consumes events from the stream's buffer using an internal consumer group
/// (`_kafka_{stream_name}`) and publishes to the configured Kafka topic.
///
/// Returns the `JoinHandle` for lifecycle management (abort on DROP CHANGE STREAM).
pub fn spawn_kafka_task(
    database_id: crate::types::DatabaseId,
    stream_name: String,
    tenant_id: u64,
    config: KafkaDeliveryConfig,
    shared_state: Arc<SharedState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    // The replicated offset commit names a registered group. Installing the
    // stream registered it on every node; this covers a stream registered
    // before its sink task started.
    if let Some(def) = shared_state
        .stream_registry
        .get(database_id, tenant_id, &stream_name)
    {
        register_sink_groups(&shared_state.group_registry, &def);
    }
    tokio::spawn(async move {
        info!(
            stream = %stream_name,
            topic = %config.topic,
            brokers = %config.brokers,
            transactional = config.transactional,
            "Kafka producer task started"
        );

        let producer = match create_producer(&config, &stream_name) {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "failed to create Kafka producer — task exiting"
                );
                return;
            }
        };

        // Initialize transactional producer if configured.
        if config.transactional
            && let Err(e) = producer.init_transactions(Duration::from_secs(10))
        {
            warn!(
                stream = %stream_name,
                error = %e,
                "failed to init Kafka transactions — falling back to at-least-once"
            );
        }

        let group_name = kafka_group(&stream_name);
        let poll_interval = Duration::from_millis(100);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(poll_interval) => {
                    // Only the lease holder of the stream's owning group
                    // publishes.
                    let Some(lease) = sink_lease(&shared_state, database_id, &stream_name) else {
                        continue;
                    };
                    let fence = SinkFence {
                        state: &shared_state,
                        database_id,
                        stream_name: &stream_name,
                        lease,
                    };
                    let fencing_token = lease.term.to_string();
                    let consume_params = ConsumeParams {
                        database_id,
                        tenant_id,
                        stream_name: &stream_name,
                        group_name: &group_name,
                        partition: None,
                        limit: 100,
                    };

                    let result = consume_for_sink(&shared_state, &consume_params).await;
                    let events = match result {
                        Ok(r) if !r.events.is_empty() => r,
                        _ => continue,
                    };

                    let batch_size = events.events.len();

                    // The topic is subscribed on behalf of the principal that
                    // created the stream, whose roles the subscription record
                    // carries. Without that record there is no scope to
                    // evaluate a column redaction policy against, so nothing
                    // is published.
                    let Some(mut subscriber) = CdcSubscriberScope::for_stream(
                        &shared_state,
                        database_id,
                        tenant_id,
                        &stream_name,
                    ) else {
                        warn!(
                            stream = %stream_name,
                            "Kafka publish: change stream is not registered — \
                             refusing to publish events with no subscriber scope"
                        );
                        continue;
                    };

                    // Begin transaction if configured.
                    if config.transactional
                        && let Err(e) = producer.begin_transaction()
                    {
                        warn!(error = %e, "Kafka begin_transaction failed");
                        continue;
                    }

                    let mut published = 0u32;
                    // Events the subscriber's rules cover but whose payload
                    // cannot be rewritten are withheld rather than published in
                    // the clear. They are still finished with, so the offset
                    // commit advances past them instead of redelivering them
                    // forever — hence a count separate from `published`, which
                    // drives the Kafka transaction.
                    let mut finished = 0u32;
                    for event in &events.events {
                        let Some(event) = subscriber.apply(&shared_state.redaction, event) else {
                            finished += 1;
                            continue;
                        };
                        let payload = match serialize_event(&event, config.format) {
                            Ok(p) => p,
                            Err(e) => {
                                warn!(error = %e, "failed to serialize event for Kafka");
                                break;
                            }
                        };

                        // The lease is checked right before each send: a
                        // node whose lease lapsed or moved on stops here.
                        if !fence.holds() {
                            break;
                        }
                        let key = format!("{}:{}", event.partition, event.offset_token());
                        let headers = rdkafka::message::OwnedHeaders::new().insert(
                            rdkafka::message::Header {
                                key: "fencing-token",
                                value: Some(fencing_token.as_str()),
                            },
                        );
                        let record = rdkafka::producer::FutureRecord::to(&config.topic)
                            .key(&key)
                            .payload(&payload)
                            .headers(headers);

                        match producer.send(record, Duration::from_secs(5)).await {
                            Ok(_) => {
                                published += 1;
                                finished += 1;
                            }
                            Err((e, _)) => {
                                warn!(
                                    error = %e,
                                    topic = %config.topic,
                                    "Kafka publish failed"
                                );
                                // Stop batch — next cycle retries from last committed offset.
                                break;
                            }
                        }
                    }

                    // Commit Kafka transaction.
                    if config.transactional && published > 0
                        && let Err(e) = producer
                            .commit_transaction(Duration::from_secs(10))
                    {
                        warn!(error = %e, "Kafka commit_transaction failed");
                        continue;
                    }

                    // Commit consumer offsets over the events this cycle
                    // finished with, taken from the batch as consumed, only
                    // while this node still holds the batch's lease.
                    if finished > 0 && fence.holds() {
                        // One replicated commit per tick carries every
                        // partition, so a producer on any node resumes here.
                        let finished_events =
                            &events.events[..(finished as usize).min(events.events.len())];
                        let offsets = crate::event::cdc::consume::batch_tails(finished_events)
                            .into_iter()
                            .map(|(partition_id, offset)| {
                                crate::event::cdc::consumer_group::PartitionOffset::new(
                                    partition_id,
                                    offset,
                                )
                            })
                            .collect();
                        if let Err(e) = crate::control::server::shared::ddl::neutral::consumer_group::commit::commit_group_offsets(
                            &shared_state,
                            database_id,
                            tenant_id,
                            &stream_name,
                            &group_name,
                            offsets,
                        )
                        .await
                        {
                            warn!(
                                stream = %stream_name,
                                error = %e.message,
                                "failed to commit Kafka offset"
                            );
                        }
                        trace!(
                            stream = %stream_name,
                            published,
                            batch_size,
                            "Kafka batch published"
                        );
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!(stream = %stream_name, "Kafka producer task shutting down");
                        return;
                    }
                }
            }
        }
    })
}

/// Create an rdkafka FutureProducer with the given configuration.
fn create_producer(
    config: &KafkaDeliveryConfig,
    stream_name: &str,
) -> crate::Result<rdkafka::producer::FutureProducer> {
    use rdkafka::ClientConfig;

    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", &config.brokers);
    client_config.set("message.timeout.ms", "30000");

    if config.transactional {
        client_config.set("enable.idempotence", "true");
        client_config.set("transactional.id", format!("nodedb-kafka-{stream_name}"));
    }

    client_config.create().map_err(|e| crate::Error::Config {
        detail: format!("create Kafka producer: {e}"),
    })
}

/// Serialize a CdcEvent for Kafka publishing.
fn serialize_event(
    event: &crate::event::cdc::event::CdcEvent,
    format: super::config::KafkaFormat,
) -> crate::Result<Vec<u8>> {
    match format {
        super::config::KafkaFormat::Json => {
            sonic_rs::to_vec(event).map_err(|e| crate::Error::Serialization {
                format: "json".to_string(),
                detail: format!("JSON serialize: {e}"),
            })
        }
        super::config::KafkaFormat::Avro => {
            // Avro serialization uses the same JSON representation for now.
            // Full Avro schema registry integration is a future enhancement —
            // the config field and wire path are ready for it.
            sonic_rs::to_vec(event).map_err(|e| crate::Error::Serialization {
                format: "avro".to_string(),
                detail: format!("Avro (JSON fallback) serialize: {e}"),
            })
        }
    }
}
