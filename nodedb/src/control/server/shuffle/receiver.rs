// SPDX-License-Identifier: BUSL-1.1

//! `RegistryShuffleReceiver` — bridges the cluster `ShufflePush` read-loop to
//! the in-process [`ShuffleReceiverRegistry`] (E3b).
//!
//! `nodedb-cluster` cannot depend on `nodedb` (circular), so the receiver
//! registry lives here and is exposed to the transport via the
//! [`nodedb_cluster::ShuffleReceiver`] hook. The `RaftLoop` is built
//! `with_shuffle_receiver(Arc::new(RegistryShuffleReceiver { .. }))`.
//!
//! The hook is async: each arriving chunk is staged to a Control-Plane scratch
//! file on the Tokio transport reactor ([`ShuffleInbox::append_chunk`]), and the
//! file is flushed + synced when the per-part build barrier completes
//! ([`ShuffleInbox::finalize`]). The awaited file write back-pressures the
//! producer via QUIC flow control.

use std::sync::Arc;

use nodedb_cluster::TypedClusterError;

use super::inbox::ShuffleReceiverRegistry;

/// `nodedb`-side implementation of [`nodedb_cluster::ShuffleReceiver`].
///
/// Delegates every callback to the shared [`ShuffleReceiverRegistry`] held by
/// `SharedState`, which owns the on-disk staging layout.
pub struct RegistryShuffleReceiver {
    pub registry: Arc<ShuffleReceiverRegistry>,
}

impl RegistryShuffleReceiver {
    /// Build a receiver over `registry`.
    pub fn new(registry: Arc<ShuffleReceiverRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait::async_trait]
impl nodedb_cluster::ShuffleReceiver for RegistryShuffleReceiver {
    async fn on_shuffle_request(&self, shuffle_id: u64, part: u32, side: u8, producer_count: u32) {
        // Lazily create the inbox on the opening frame; subsequent producers
        // for the same part reuse it.
        self.registry
            .get_or_create(shuffle_id, part, side, producer_count as usize);
    }

    async fn on_shuffle_chunk(
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
            .unwrap_or_else(|| self.registry.get_or_create(shuffle_id, part, side, 1));
        // Stage the chunk's rows to the scratch file. A malformed array or I/O
        // failure surfaces as a typed transport error (and is also captured in
        // the inbox's error slot so the consumer sees it after the barrier),
        // never a silent drop.
        match inbox.append_chunk(&payload).await {
            Ok(()) => Ok(()),
            Err(e) => {
                let detail = format!("shuffle stage append ({shuffle_id},{part},{side}): {e}");
                inbox.set_error(TypedClusterError::Internal {
                    code: 0,
                    message: detail.clone(),
                });
                Err(nodedb_cluster::ClusterError::Storage { detail })
            }
        }
    }

    async fn on_shuffle_end(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        error: Option<TypedClusterError>,
    ) {
        let inbox = self
            .registry
            .get((shuffle_id, part, side))
            .unwrap_or_else(|| self.registry.get_or_create(shuffle_id, part, side, 1));
        if let Some(e) = error {
            inbox.set_error(e);
        }
        // On barrier completion, flush + sync the staged file so the Data Plane
        // reader sees a complete, durable file. A finalize I/O error is captured
        // in the inbox so the consumer surfaces it rather than reading a
        // half-written file as if it were complete.
        if inbox.record_end()
            && let Err(e) = inbox.finalize().await
        {
            inbox.set_error(TypedClusterError::Internal {
                code: 0,
                message: format!("shuffle stage finalize ({shuffle_id},{part},{side}): {e}"),
            });
        }
    }
}
