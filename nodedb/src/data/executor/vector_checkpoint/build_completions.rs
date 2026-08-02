// SPDX-License-Identifier: BUSL-1.1

//! Draining completed background HNSW builds into the live collections.
//!
//! Lives beside the checkpoint because it shares the checkpoint's key
//! encoding: a `BuildComplete.key` is the same `"{db}:{tid}:{coll}"` string a
//! checkpoint filename carries, so both parse it through
//! [`parse_build_key`](super::paths::parse_build_key).

use super::paths::parse_build_key;
use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Drain completed HNSW builds from the background builder thread and
    /// promote the corresponding building segments to sealed segments.
    ///
    /// Called at the top of `tick()` before draining new requests.
    ///
    /// `BuildComplete.key` is the `"{db}:{tid}:{coll}"` string produced by
    /// `VectorCollection::seal` (fed the `vector_checkpoint_filename` of the
    /// index key). Parse it back to the tuple key to look up the map.
    pub fn poll_build_completions(&mut self) {
        let Some(rx) = &self.build_rx else { return };
        while let Ok(complete) = rx.try_recv() {
            // Parse the string key `"{db}:{tid}:{coll_key}"` back into the tuple.
            let Some(tuple_key) = parse_build_key(&complete.key) else {
                tracing::warn!(
                    core = self.core_id,
                    key = %complete.key,
                    "HNSW build completion has unparseable key; dropping"
                );
                continue;
            };
            if let Some(coll) = self.vector_collections.get_mut(&tuple_key) {
                coll.complete_build(complete.segment_id, complete.index);
                tracing::info!(
                    core = self.core_id,
                    key = %complete.key,
                    segment_id = complete.segment_id,
                    "HNSW build completed, segment promoted to sealed"
                );
            }
        }
    }
}
