// SPDX-License-Identifier: BUSL-1.1

//! Garbage collection of snapshot chunks that no kept manifest lists.
//!
//! Bases share chunks, so deleting a base deletes only its manifest. A chunk
//! goes once no pin names it: neither a kept base nor a base in progress.

use std::sync::Arc;

use object_store::ObjectStore;
use tracing::info;

use super::pins::ColdPins;
use crate::storage::snapshot_writer::{delete_chunk, list_chunk_ids};

/// Delete every chunk in `store` that `pins` does not pin. Returns the number
/// deleted.
///
/// Each delete holds the read lock across its pin check and the delete. A
/// base pins its ids under the write lock before it checks the store, so a
/// chunk it relies on is either pinned first or deleted before it looks.
pub async fn collect_unreferenced_chunks(
    store: &Arc<dyn ObjectStore>,
    pins: &tokio::sync::RwLock<ColdPins>,
) -> crate::Result<u64> {
    let mut collected = 0;
    for id in list_chunk_ids(store).await? {
        let guard = pins.read().await;
        if guard.is_pinned(&id) {
            continue;
        }
        delete_chunk(store, &id).await?;
        drop(guard);
        collected += 1;
    }
    if collected > 0 {
        info!(collected, "snapshot chunks no kept base lists collected");
    }
    Ok(collected)
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;
    use crate::control::pitr::pins::pending_pin_name;
    use crate::storage::snapshot_writer::chunk_path;

    fn id(digit: char) -> String {
        digit.to_string().repeat(64)
    }

    async fn store_with(ids: &[String]) -> Arc<dyn ObjectStore> {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for id in ids {
            store
                .put(&chunk_path(id), PutPayload::from_static(b"chunk"))
                .await
                .unwrap();
        }
        store
    }

    async fn stored(store: &Arc<dyn ObjectStore>) -> Vec<String> {
        let mut ids = list_chunk_ids(store).await.unwrap();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn a_chunk_goes_only_when_no_kept_base_lists_it() {
        let (shared, old, new) = (id('a'), id('b'), id('c'));
        let store = store_with(&[shared.clone(), old.clone(), new.clone()]).await;
        let kept = vec![shared.clone(), new.clone()];
        let pins =
            tokio::sync::RwLock::new(ColdPins::from_bases([("snap-2", kept.as_slice())], true));
        assert_eq!(collect_unreferenced_chunks(&store, &pins).await.unwrap(), 1);
        assert_eq!(stored(&store).await, [shared, new]);
    }

    #[tokio::test]
    async fn a_chunk_of_a_base_in_progress_survives() {
        let pending_id = id('d');
        let store = store_with(std::slice::from_ref(&pending_id)).await;
        let pins = tokio::sync::RwLock::new(ColdPins::default());
        let pending = pending_pin_name();
        pins.write().await.pin(&pending, vec![pending_id.clone()]);
        // Retention rebuilds the kept pins while the base is still written.
        pins.write().await.rebuild([], true);

        assert_eq!(collect_unreferenced_chunks(&store, &pins).await.unwrap(), 0);
        assert_eq!(stored(&store).await, [pending_id]);

        pins.write().await.unpin(&pending);
        assert_eq!(collect_unreferenced_chunks(&store, &pins).await.unwrap(), 1);
        assert!(stored(&store).await.is_empty());
    }

    #[tokio::test]
    async fn incomplete_pins_collect_nothing() {
        let store = store_with(&[id('e')]).await;
        let pins = tokio::sync::RwLock::new(ColdPins::from_bases([], false));
        assert_eq!(collect_unreferenced_chunks(&store, &pins).await.unwrap(), 0);
        assert_eq!(stored(&store).await.len(), 1);
    }
}
