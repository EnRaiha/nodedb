// SPDX-License-Identifier: BUSL-1.1

//! `RegistryShuffleReceiver` — bridges the cluster `ShufflePush` read-loop to
//! the in-process [`ShuffleReceiverRegistry`] (E1).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the receiver
//! registry lives here and is exposed to the transport via the
//! [`nodedb_cluster::ShuffleReceiver`] hook. The `RaftLoop` is built
//! `with_shuffle_receiver(Arc::new(RegistryShuffleReceiver { .. }))`.

use std::sync::Arc;

use nodedb_cluster::TypedClusterError;

use super::inbox::ShuffleReceiverRegistry;

/// Default per-inbox bounded buffer capacity (number of chunk payloads).
///
/// Caps how many chunks one part may buffer before the transport read-loop
/// blocks the producer (bounded back-pressure → QUIC flow control). The E3
/// Data Plane drain keeps this buffer flowing.
pub const DEFAULT_SHUFFLE_INBOX_CAPACITY: usize = 1024;

/// `nodedb`-side implementation of [`nodedb_cluster::ShuffleReceiver`].
///
/// Delegates every callback to the shared [`ShuffleReceiverRegistry`] held by
/// `SharedState`.
pub struct RegistryShuffleReceiver {
    pub registry: Arc<ShuffleReceiverRegistry>,
    /// Per-inbox bounded buffer capacity used when lazily creating an inbox.
    pub capacity: usize,
}

impl RegistryShuffleReceiver {
    /// Build a receiver over `registry` using [`DEFAULT_SHUFFLE_INBOX_CAPACITY`].
    pub fn new(registry: Arc<ShuffleReceiverRegistry>) -> Self {
        Self {
            registry,
            capacity: DEFAULT_SHUFFLE_INBOX_CAPACITY,
        }
    }
}

impl nodedb_cluster::ShuffleReceiver for RegistryShuffleReceiver {
    fn on_shuffle_request(&self, shuffle_id: u64, part: u32, side: u8, producer_count: u32) {
        // Lazily create the inbox on the opening frame; subsequent producers
        // for the same part reuse it.
        self.registry.get_or_create(
            shuffle_id,
            part,
            side,
            producer_count as usize,
            self.capacity,
        );
    }

    fn on_shuffle_chunk(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        payload: Vec<u8>,
    ) -> nodedb_cluster::Result<()> {
        // The opening frame created the inbox; if a chunk arrives without one
        // (producer skipped the request frame), create with a single expected
        // producer so the chunk is not dropped.
        let inbox = self
            .registry
            .get((shuffle_id, part, side))
            .unwrap_or_else(|| {
                self.registry
                    .get_or_create(shuffle_id, part, side, 1, self.capacity)
            });
        // Bounded push — blocks the transport read-loop while the inbox is full,
        // back-pressuring the producer via QUIC flow control.
        inbox.push(payload);
        Ok(())
    }

    fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        let inbox = self
            .registry
            .get((shuffle_id, part, side))
            .unwrap_or_else(|| {
                self.registry
                    .get_or_create(shuffle_id, part, side, 1, self.capacity)
            });
        if let Some(e) = error {
            inbox.set_error(e);
        }
        inbox.record_end();
    }
}
