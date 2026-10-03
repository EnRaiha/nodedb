// SPDX-License-Identifier: BUSL-1.1

//! Asking each Event Plane consumer for an image of its action retry store.
//!
//! The store's redb handle lives inside the consumer task that owns the retry
//! queue, so only that task can read a consistent image. A snapshot parks one
//! reply slot per core here, each consumer answers on its next loop pass, and
//! the snapshot awaits every answer.

use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::oneshot;

/// One consumer's answer: the store image, or `None` when the core has never
/// kept an action and so has no store file.
pub type RetryStoreImage = crate::Result<Option<Vec<u8>>>;

/// Requests in flight per core. A snapshot sends one per core, so a deeper
/// backlog means snapshots are started faster than consumers answer.
const MAX_PENDING_PER_CORE: usize = 4;

/// Per-core reply slots for retry store capture requests.
pub struct RetryStoreCaptures {
    per_core: Vec<Mutex<Vec<oneshot::Sender<RetryStoreImage>>>>,
}

impl RetryStoreCaptures {
    pub fn for_cores(num_cores: usize) -> Self {
        Self {
            per_core: (0..num_cores).map(|_| Mutex::new(Vec::new())).collect(),
        }
    }

    /// Ask every consumer for its store image and wait up to `timeout` for
    /// all of them. Returns `(core_id, image)` in core order.
    pub async fn capture_all(
        &self,
        timeout: Duration,
    ) -> crate::Result<Vec<(usize, Option<Vec<u8>>)>> {
        let mut replies = Vec::with_capacity(self.per_core.len());
        for (core_id, slot) in self.per_core.iter().enumerate() {
            let (tx, rx) = oneshot::channel();
            let mut pending = slot.lock().unwrap_or_else(|p| p.into_inner());
            if pending.len() >= MAX_PENDING_PER_CORE {
                return Err(crate::Error::Dispatch {
                    detail: format!(
                        "event consumer {core_id} has {MAX_PENDING_PER_CORE} retry store \
                         captures awaiting it; retry once they complete"
                    ),
                });
            }
            pending.push(tx);
            replies.push((core_id, rx));
        }

        let mut images = Vec::with_capacity(replies.len());
        for (core_id, rx) in replies {
            let answer = tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| crate::Error::Dispatch {
                    detail: format!(
                        "event consumer {core_id} did not answer a retry store capture \
                         within {timeout:?}"
                    ),
                })?
                .map_err(|_| crate::Error::Dispatch {
                    detail: format!(
                        "event consumer {core_id} stopped before answering a retry store capture"
                    ),
                })?;
            images.push((core_id, answer?));
        }
        Ok(images)
    }

    /// Take every reply slot parked for `core_id`. The consumer answers each.
    pub fn take_for_core(&self, core_id: usize) -> Vec<oneshot::Sender<RetryStoreImage>> {
        match self.per_core.get(core_id) {
            Some(slot) => std::mem::take(&mut *slot.lock().unwrap_or_else(|p| p.into_inner())),
            None => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_core_answers_in_core_order() {
        let captures = std::sync::Arc::new(RetryStoreCaptures::for_cores(2));
        let serving = std::sync::Arc::clone(&captures);
        let consumers = tokio::spawn(async move {
            loop {
                for core_id in 0..2 {
                    for reply in serving.take_for_core(core_id) {
                        let image = (core_id == 1).then(|| vec![core_id as u8]);
                        let _ = reply.send(Ok(image));
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        let images = captures.capture_all(Duration::from_secs(5)).await.unwrap();
        consumers.abort();
        assert_eq!(images, vec![(0, None), (1, Some(vec![1]))]);
    }

    #[tokio::test]
    async fn a_silent_consumer_times_out() {
        let captures = RetryStoreCaptures::for_cores(1);
        assert!(
            captures
                .capture_all(Duration::from_millis(20))
                .await
                .is_err()
        );
    }
}
