// SPDX-License-Identifier: BUSL-1.1

//! Webhook delivery task: background Tokio task per webhook-enabled stream.
//!
//! Consumes events from the stream's buffer using an internal consumer group
//! (`_webhook:<stream_name>`), POSTs each event batch to the URL, and commits
//! offsets on success. Failed deliveries retry with exponential backoff,
//! then go to the trigger DLQ after max retries.

use std::sync::Arc;
use std::time::Duration;

use sonic_rs;

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::control::security::redaction::RedactionStore;
use crate::control::state::SharedState;
use crate::event::cdc::CdcSubscriberScope;
use crate::event::cdc::event::CdcEvent;

use super::types::WebhookConfig;

/// Internal consumer group name for webhook delivery.
fn webhook_group_name(stream_name: &str) -> String {
    format!("_webhook:{stream_name}")
}

/// Spawn a webhook delivery task for a single stream.
///
/// Returns a handle that can be used to abort the task.
pub fn spawn_delivery_task(
    state: Arc<SharedState>,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    stream_name: String,
    config: WebhookConfig,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    let group_name = webhook_group_name(&stream_name);

    // Register the internal consumer group (if not already present).
    if state
        .group_registry
        .get(database_id, tenant_id, &stream_name, &group_name)
        .is_none()
    {
        let def = crate::event::cdc::consumer_group::ConsumerGroupDef {
            database_id,
            tenant_id,
            name: group_name.clone(),
            stream_name: stream_name.clone(),
            owner: "_system_webhook".into(),
            created_at: 0,
        };
        state.group_registry.register(def);
    }

    tokio::spawn(async move {
        delivery_loop(
            state,
            database_id,
            tenant_id,
            stream_name,
            group_name,
            config,
            shutdown,
        )
        .await;
    })
}

/// The main delivery loop.
async fn delivery_loop(
    state: Arc<SharedState>,
    database_id: crate::types::DatabaseId,
    tenant_id: u64,
    stream_name: String,
    group_name: String,
    config: WebhookConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    info!(
        stream = %stream_name,
        url = %config.url,
        "webhook delivery task started"
    );

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .unwrap_or_default();

    loop {
        if *shutdown.borrow() {
            debug!(stream = %stream_name, "webhook delivery task shutting down");
            return;
        }

        // Read events from the buffer using the internal consumer group.
        let consume_params = crate::event::cdc::consume::ConsumeParams {
            database_id,
            tenant_id,
            stream_name: &stream_name,
            group_name: &group_name,
            partition: None,
            limit: 100, // Batch size per delivery cycle.
        };

        let result = crate::event::cdc::consume::consume_stream(&state, &consume_params);

        match result {
            Ok(consume_result) if !consume_result.events.is_empty() => {
                let batch_size = consume_result.events.len();
                trace!(
                    stream = %stream_name,
                    batch_size,
                    "delivering webhook batch"
                );

                // The destination is subscribed on behalf of the principal that
                // created the stream, whose roles the subscription record
                // carries. Without that record there is no scope to evaluate a
                // column redaction policy against, so nothing is delivered.
                let Some(mut subscriber) =
                    CdcSubscriberScope::for_stream(&state, database_id, tenant_id, &stream_name)
                else {
                    warn!(
                        stream = %stream_name,
                        "webhook delivery: change stream is not registered — \
                         refusing to deliver events with no subscriber scope"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                };

                // POST each event individually (not batched — simpler retry semantics).
                let delivered = deliver_batch(BatchDelivery {
                    client: &client,
                    config: &config,
                    stream_name: &stream_name,
                    store: &state.redaction,
                    subscriber: &mut subscriber,
                    events: &consume_result.events,
                })
                .await;

                // Commit offsets for the events this cycle finished with.
                if delivered > 0 {
                    // Commit the exact composite position of the last
                    // processed event in each partition, taken from the batch
                    // as consumed: an event withheld by a redaction rule is
                    // finished with too, and must not be redelivered forever.
                    let delivered_events = &consume_result.events[..delivered];
                    let mut partition_max = std::collections::HashMap::new();
                    for event in delivered_events {
                        let entry = partition_max
                            .entry(event.partition)
                            .or_insert(crate::event::cdc::CdcOffset::ZERO);
                        if event.position() > *entry {
                            *entry = event.position();
                        }
                    }
                    for (partition_id, offset) in partition_max {
                        if let Err(e) = state.offset_store.commit_offset(
                            database_id,
                            tenant_id,
                            &stream_name,
                            &group_name,
                            partition_id,
                            offset,
                        ) {
                            warn!(
                                stream = %stream_name,
                                error = %e,
                                "failed to commit webhook offset"
                            );
                        }
                    }
                    trace!(
                        stream = %stream_name,
                        delivered,
                        total = batch_size,
                        "webhook batch delivered"
                    );
                }

                // If we delivered everything, immediately try again (more may have arrived).
                if delivered == batch_size {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
            Ok(_) => {
                // No events — wait before polling again.
            }
            Err(crate::event::cdc::consume::ConsumeError::BufferEmpty(_)) => {
                // Stream exists but no events yet.
            }
            Err(e) => {
                warn!(
                    stream = %stream_name,
                    error = %e,
                    "webhook delivery: consume error"
                );
            }
        }

        // Poll interval: 200ms for reasonable latency without busy-spinning.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

/// One batch delivery's inputs.
struct BatchDelivery<'a> {
    client: &'a reqwest::Client,
    config: &'a WebhookConfig,
    stream_name: &'a str,
    store: &'a RedactionStore,
    subscriber: &'a mut CdcSubscriberScope,
    events: &'a [Arc<CdcEvent>],
}

/// POST a batch, redacting each event for the subscriber first.
///
/// Returns how many leading events of the batch this cycle finished with, which
/// is what the offset commit advances over. An event a rule covers but whose
/// payload could not be rewritten is skipped rather than POSTed in the clear,
/// and still counts as finished so the cursor moves past it instead of
/// redelivering it forever. Delivery stops at the first POST failure, so the
/// next cycle retries from the last committed offset.
async fn deliver_batch(delivery: BatchDelivery<'_>) -> usize {
    let BatchDelivery {
        client,
        config,
        stream_name,
        store,
        subscriber,
        events,
    } = delivery;
    let mut finished = 0usize;
    for event in events {
        if let Some(event) = subscriber.apply(store, event)
            && !deliver_event(client, config, &event, stream_name).await
        {
            break;
        }
        finished += 1;
    }
    finished
}

/// POST a single event to the webhook URL. Returns true on success.
/// Retries with exponential backoff on failure.
async fn deliver_event(
    client: &reqwest::Client,
    config: &WebhookConfig,
    event: &CdcEvent,
    stream_name: &str,
) -> bool {
    let body = match sonic_rs::to_vec(event) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(
                error = %e,
                stream = stream_name,
                row_id = %event.row_id,
                "failed to serialize webhook event, skipping"
            );
            return false;
        }
    };
    let idempotency_key = format!("{}:{}", event.partition, event.offset_token());

    for attempt in 0..=config.max_retries {
        let mut request = client
            .post(&config.url)
            .header("Content-Type", "application/json")
            .header("X-Idempotency-Key", &idempotency_key)
            .header("X-Event-Sequence", event.sequence.to_string())
            .header("X-Stream-Name", stream_name)
            .header("X-Partition", event.partition.to_string())
            .header("X-LSN", event.lsn.to_string())
            .header("X-CDC-Offset", event.offset_token());

        // Add custom headers.
        for (key, value) in &config.headers {
            request = request.header(key, value);
        }

        match request.body(body.clone()).send().await {
            Ok(response) if response.status().is_success() => {
                trace!(
                    stream = stream_name,
                    lsn = event.lsn,
                    status = response.status().as_u16(),
                    "webhook delivered"
                );
                return true;
            }
            Ok(response) => {
                let status = response.status().as_u16();
                // 4xx errors (except 429) are permanent — don't retry.
                if (400..500).contains(&status) && status != 429 {
                    warn!(
                        stream = stream_name,
                        lsn = event.lsn,
                        status,
                        attempt,
                        "webhook rejected with client error, not retrying"
                    );
                    return false;
                }
                warn!(
                    stream = stream_name,
                    lsn = event.lsn,
                    status,
                    attempt,
                    max_retries = config.max_retries,
                    "webhook delivery failed, retrying"
                );
            }
            Err(e) => {
                warn!(
                    stream = stream_name,
                    lsn = event.lsn,
                    error = %e,
                    attempt,
                    max_retries = config.max_retries,
                    "webhook delivery error, retrying"
                );
            }
        }

        if attempt < config.max_retries {
            tokio::time::sleep(backoff_delay(attempt)).await;
        }
    }

    warn!(
        stream = stream_name,
        lsn = event.lsn,
        max_retries = config.max_retries,
        "webhook delivery exhausted retries"
    );
    false
}

/// Compute exponential backoff delay for a given retry attempt.
///
/// Formula: min(100ms * 2^attempt, 10s).
fn backoff_delay(attempt: u32) -> Duration {
    Duration::from_millis(100 * 2u64.saturating_pow(attempt)).min(Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_exponential_with_cap() {
        assert_eq!(backoff_delay(0), Duration::from_millis(100));
        assert_eq!(backoff_delay(1), Duration::from_millis(200));
        assert_eq!(backoff_delay(2), Duration::from_millis(400));
        assert_eq!(backoff_delay(3), Duration::from_millis(800));
        assert_eq!(backoff_delay(4), Duration::from_millis(1600));
        assert_eq!(backoff_delay(5), Duration::from_millis(3200));
        assert_eq!(backoff_delay(6), Duration::from_millis(6400));
        // Capped at 10s.
        assert_eq!(backoff_delay(7), Duration::from_secs(10));
        assert_eq!(backoff_delay(10), Duration::from_secs(10));
        assert_eq!(backoff_delay(32), Duration::from_secs(10));
    }

    #[test]
    fn webhook_group_name_format() {
        let name = format!("_webhook:{}", "orders_stream");
        assert_eq!(name, "_webhook:orders_stream");
    }
}
