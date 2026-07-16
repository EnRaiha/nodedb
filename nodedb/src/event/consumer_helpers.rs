// SPDX-License-Identifier: BUSL-1.1

//! Utility helpers for the Event Plane consumer loop.
//!
//! Extracted from `consumer.rs` to keep the main consumer module focused
//! on the state machine and dispatch orchestration.

use std::sync::Arc;

use tracing::{trace, warn};

use super::bus::EventConsumerRx;
use super::metrics::CoreMetrics;
use super::trigger::retry::TriggerRetryQueue;
use super::types::WriteEvent;
use super::watermark::WatermarkStore;
use crate::control::state::SharedState;
use crate::types::Lsn;

/// How often to persist the watermark to redb (avoid fsync on every event).
pub const WATERMARK_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Maximum events to process per ring buffer drain before yielding.
pub const DRAIN_BATCH_LIMIT: u32 = 1024;

/// Detect sequence gaps (events dropped by the producer due to buffer overflow).
pub fn detect_sequence_gap(
    core_id: usize,
    event: &WriteEvent,
    last_sequence: u64,
    metrics: &CoreMetrics,
) {
    if last_sequence > 0 && event.sequence > last_sequence + 1 {
        let gap = event.sequence - last_sequence - 1;
        metrics.record_drop(gap);
        warn!(
            core_id,
            gap,
            last_seq = last_sequence,
            new_seq = event.sequence,
            "event sequence gap — {gap} events dropped (WAL replay needed)"
        );
    }
}

/// Process a single event. Dispatch point for trigger matching, CDC, etc.
pub fn record_event(core_id: usize, event: &WriteEvent, metrics: &CoreMetrics) {
    metrics.record_process_for_tenant(event.lsn.as_u64(), event.sequence, event.tenant_id.as_u64());

    trace!(
        core_id,
        seq = event.sequence,
        collection = %event.collection,
        op = %event.op,
        source = %event.source,
        lsn = event.lsn.as_u64(),
        "event consumed"
    );
}

/// Flush watermark to redb if the flush interval has elapsed.
pub fn maybe_flush_watermark(
    store: &WatermarkStore,
    core_id: usize,
    lsn: Lsn,
    dirty: &mut bool,
    last_flush: &mut tokio::time::Instant,
) {
    if *dirty && last_flush.elapsed() >= WATERMARK_FLUSH_INTERVAL {
        flush_watermark(store, core_id, lsn);
        *dirty = false;
        *last_flush = tokio::time::Instant::now();
    }
}

/// Persist watermark to redb (best-effort — log on failure).
pub fn flush_watermark(store: &WatermarkStore, core_id: usize, lsn: Lsn) {
    if lsn == Lsn::ZERO {
        return;
    }
    if let Err(e) = store.save(core_id, lsn) {
        warn!(core_id, lsn = lsn.as_u64(), error = %e, "failed to persist watermark");
    } else {
        trace!(core_id, lsn = lsn.as_u64(), "watermark flushed");
    }
}

/// Drain all available events from the ring buffer (up to DRAIN_BATCH_LIMIT).
/// Returns the drained events for async processing (trigger dispatch).
pub fn drain_ring_buffer(
    rx: &mut EventConsumerRx,
    metrics: &CoreMetrics,
    core_id: usize,
    last_sequence: &mut u64,
    last_lsn: &mut Lsn,
) -> Vec<WriteEvent> {
    let mut events = Vec::new();
    while let Some(event) = rx.try_recv() {
        detect_sequence_gap(core_id, &event, *last_sequence, metrics);
        record_event(core_id, &event, metrics);

        *last_sequence = event.sequence;
        if event.lsn.is_ahead_of(*last_lsn) {
            *last_lsn = event.lsn;
        }

        events.push(event);
        if (events.len() as u32).is_multiple_of(DRAIN_BATCH_LIMIT) {
            break;
        }
    }
    events
}

/// Drain ring-buffer events already covered by WAL replay (`lsn <= last_lsn`),
/// returning the first event whose LSN is beyond the replay point so the
/// caller can dispatch it.
///
/// Reconciliation is by **LSN** — the durable, monotonic key — not by
/// `sequence`: the per-core `sequence` counter resets to 0 on process
/// restart, so at boot it collides between WAL-replayed events and freshly
/// produced live events and cannot distinguish them. The ring is SPSC (a
/// `try_recv`'d event cannot be pushed back), so the first non-stale event is
/// handed back to the caller rather than dropped; every event after it in the
/// ring is also fresh (the producer emits in LSN order) and is left for the
/// Normal-mode drain.
pub fn drain_and_skip_stale(rx: &mut EventConsumerRx, last_lsn: Lsn) -> Option<WriteEvent> {
    let mut skipped = 0u32;
    let mut fresh = None;
    while let Some(event) = rx.try_recv() {
        if event.lsn.is_ahead_of(last_lsn) {
            fresh = Some(event);
            break;
        }
        skipped += 1;
    }
    if skipped > 0 {
        trace!(
            skipped,
            "drained stale events from ring buffer after WAL catchup"
        );
    }
    fresh
}

/// Dispatch a single write event: triggers, CDC, permission cache, streaming MVs, CRDT sync.
///
/// Called for both Normal-mode ring-buffer events and WAL-catchup-replayed events.
pub async fn dispatch_event(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    retry_queue: &mut TriggerRetryQueue,
    cdc_router: &Arc<super::cdc::CdcRouter>,
) {
    shared_state
        .watermark_tracker
        .advance_lsn_only(event.vshard_id.as_u32(), event.lsn.as_u64());

    super::trigger::dispatcher::dispatch_triggers(event, shared_state, retry_queue).await;
    cdc_router.route_event(event, &shared_state.watermark_tracker);
}

/// Apply the non-trigger side effects of a data write event
/// (`op.is_data_event() == true`): advances the wall-time watermark, routes
/// CDC, updates the permission cache, feeds streaming MVs and CRDT sync.
///
/// Row/statement trigger dispatch is NOT done here — it is owned exclusively
/// by [`dispatch_triggers`] (called once per event by both the Normal-mode
/// and WAL-catchup paths) so a per-row event fires its AFTER-ROW trigger
/// exactly once regardless of which path consumed it.
pub fn accumulate_data_event(
    event: &WriteEvent,
    shared_state: &Arc<SharedState>,
    cdc_router: &Arc<super::cdc::CdcRouter>,
) {
    let event_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    shared_state.watermark_tracker.advance(
        event.vshard_id.as_u32(),
        event.lsn.as_u64(),
        event_time_ms,
    );

    cdc_router.route_event(event, &shared_state.watermark_tracker);
    crate::control::security::permission_tree::event_handler::handle_permission_event(
        event,
        &shared_state.permission_cache,
    );
    let matching_streams = shared_state
        .stream_registry
        .find_matching(event.tenant_id.as_u64(), &event.collection);
    for stream_def in &matching_streams {
        super::streaming_mv::processor::process_write_event_for_mvs(
            event,
            &shared_state.mv_registry,
            &stream_def.name,
        );
    }
    shared_state
        .delta_packager
        .package_and_enqueue(event, &shared_state.crdt_sync_delivery);
}
