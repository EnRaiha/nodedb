// SPDX-License-Identifier: BUSL-1.1

//! Owned drain holds.
//!
//! Each hold carries an id, and a release removes exactly that id. A release
//! therefore never frees another holder's drain.
//!
//! A `_system.pending_reclaim` row owns at most one hold, keyed by the row's
//! own key, [`ReclaimOwner`]. Only a path that removed that row releases it.
//! A second hold handed to the same row is released at once. Row-owned holds
//! live in memory only, so after a restart a row owns none until its retry
//! takes one.

use std::sync::{Arc, MutexGuard};

use super::refcount::{CollectionQuiesce, Inner};

/// Key of one collection's quiesce state: `(database, tenant, name)`.
type Key = (u64, u64, String);

/// The owner of a row-owned hold: the `_system.pending_reclaim` row, keyed
/// `(database, tenant, name)` like the row.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReclaimOwner {
    pub database_id: u64,
    pub tenant_id: u64,
    pub name: String,
}

impl ReclaimOwner {
    pub fn new(database_id: u64, tenant_id: u64, name: &str) -> Self {
        Self {
            database_id,
            tenant_id,
            name: name.to_string(),
        }
    }

    fn key(&self) -> Key {
        (self.database_id, self.tenant_id, self.name.clone())
    }
}

/// One drain hold. New scans and same-name CREATE stay blocked while it
/// lives. Dropping it releases the hold.
#[must_use = "the drain is released when the hold drops"]
pub struct DrainHold {
    registry: Arc<CollectionQuiesce>,
    key: Key,
    id: u64,
    active: bool,
}

impl DrainHold {
    /// Release the hold now. Equivalent to dropping it.
    pub fn release(self) {
        drop(self);
    }

    /// Pass the hold to the `_system.pending_reclaim` row of the same
    /// collection. A second hold handed to that row is released: the row
    /// already holds the name.
    pub fn hand_to_reclaim(mut self) {
        self.active = false;
        let (database_id, tenant_id, name) = self.key.clone();
        self.registry.park_reclaim_hold(
            ReclaimOwner {
                database_id,
                tenant_id,
                name,
            },
            self.id,
        );
    }
}

impl Drop for DrainHold {
    fn drop(&mut self) {
        if self.active {
            self.registry.release_hold(&self.key, self.id);
        }
    }
}

impl std::fmt::Debug for DrainHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DrainHold")
            .field("key", &self.key)
            .field("id", &self.id)
            .field("active", &self.active)
            .finish()
    }
}

impl CollectionQuiesce {
    fn locked(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record a new hold on `key` and return its id. The caller holds the
    /// registry lock.
    fn add_hold(inner: &mut Inner, key: &Key) -> u64 {
        let id = inner.next_hold_id;
        inner.next_hold_id = inner.next_hold_id.wrapping_add(1);
        inner
            .states
            .entry(key.clone())
            .or_default()
            .drain_holders
            .insert(id);
        id
    }

    fn wrap(self: &Arc<Self>, key: Key, id: u64) -> DrainHold {
        DrainHold {
            registry: Arc::clone(self),
            key,
            id,
            active: true,
        }
    }

    /// Stop new scans on the collection. Wait for open scans with
    /// [`Self::wait_until_drained`]. The drain lasts until every hold on the
    /// collection is released.
    pub fn begin_drain(
        self: &Arc<Self>,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> DrainHold {
        let key = (database_id, tenant_id, collection.to_string());
        let id = Self::add_hold(&mut self.locked(), &key);
        self.wrap(key, id)
    }

    /// Give `owner` a hold on its collection, unless it already owns one.
    pub fn ensure_reclaim_hold(&self, owner: &ReclaimOwner) {
        let mut inner = self.locked();
        if inner.reclaim_holds.contains_key(owner) {
            return;
        }
        let id = Self::add_hold(&mut inner, &owner.key());
        inner.reclaim_holds.insert(owner.clone(), id);
    }

    /// Whether `owner` holds its collection.
    pub fn has_reclaim_hold(&self, owner: &ReclaimOwner) -> bool {
        self.locked().reclaim_holds.contains_key(owner)
    }

    /// Release the hold `owner` owns, if any. Call it only after the row is
    /// removed. No other holder's drain is touched.
    pub fn release_reclaim_hold(&self, owner: &ReclaimOwner) {
        let owned = self.locked().reclaim_holds.remove(owner);
        if let Some(id) = owned {
            self.release_hold(&owner.key(), id);
        }
    }

    fn park_reclaim_hold(&self, owner: ReclaimOwner, id: u64) {
        let duplicate = {
            let mut inner = self.locked();
            if inner.reclaim_holds.contains_key(&owner) {
                true
            } else {
                inner.reclaim_holds.insert(owner.clone(), id);
                false
            }
        };
        if duplicate {
            self.release_hold(&owner.key(), id);
        }
    }

    /// Remove hold `id` from `key` and wake CREATE and drain waiters.
    fn release_hold(&self, key: &Key, id: u64) {
        {
            let mut inner = self.locked();
            let remove = match inner.states.get_mut(key) {
                Some(state) => {
                    state.drain_holders.remove(&id);
                    state.drain_holders.is_empty() && state.open_scans == 0
                }
                None => false,
            };
            if remove {
                inner.states.remove(key);
            }
        }
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u64 = 0;

    /// Releasing the reclaim path's hold never frees another holder, and a
    /// release with no owned hold, as after a restart, touches nothing.
    #[test]
    fn a_reclaim_release_frees_only_its_own_hold() {
        let q = CollectionQuiesce::new();
        let row = ReclaimOwner::new(DB, 1, "c");
        let other = q.begin_drain(DB, 1, "c");

        q.release_reclaim_hold(&row);
        assert!(q.is_draining(DB, 1, "c"), "no owned hold, nothing released");

        q.ensure_reclaim_hold(&row);
        q.ensure_reclaim_hold(&row);
        q.release_reclaim_hold(&row);
        assert!(
            q.is_draining(DB, 1, "c"),
            "the other holder's drain survives the reclaim release"
        );

        drop(other);
        assert!(!q.is_draining(DB, 1, "c"));
    }

    /// A second hold handed to the reclaim path is released at once, so
    /// one release frees the name.
    #[test]
    fn a_second_handed_hold_is_released() {
        let q = CollectionQuiesce::new();
        q.begin_drain(DB, 1, "c").hand_to_reclaim();
        q.begin_drain(DB, 1, "c").hand_to_reclaim();
        assert!(q.is_draining(DB, 1, "c"));

        q.release_reclaim_hold(&ReclaimOwner::new(DB, 1, "c"));
        assert!(!q.is_draining(DB, 1, "c"));
    }

    /// Rows of two collections own separate holds: releasing one row's hold
    /// leaves the other's drain in place.
    #[test]
    fn row_owned_holds_are_keyed_by_row() {
        let q = CollectionQuiesce::new();
        let first = ReclaimOwner::new(DB, 1, "a");
        let second = ReclaimOwner::new(DB, 1, "b");
        q.ensure_reclaim_hold(&first);
        q.ensure_reclaim_hold(&second);

        q.release_reclaim_hold(&first);
        assert!(!q.is_draining(DB, 1, "a"));
        assert!(q.is_draining(DB, 1, "b"));
        assert!(q.has_reclaim_hold(&second));
    }
}
