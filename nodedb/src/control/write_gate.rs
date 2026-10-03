// SPDX-License-Identifier: BUSL-1.1

//! One read-write gate per replicated write target.
//!
//! A replica holds a write's gate shared from its routing decision until the
//! write reaches its core's queue. A rekey, drop or purge holds the gate
//! exclusive while it changes the cores and the catalog. A routing decision
//! is therefore never stale by the time its write applies, and a change to
//! one target never stalls writes to another.
//!
//! - An array cell write keys on its array's incarnation: a MOVE TENANT
//!   rekeys the array in place, and the write follows the incarnation.
//! - A collection write keys on the collection's storage key: a purge or a
//!   MOVE TENANT reclaim clears that key.
//!
//! A gate exists only while someone holds or waits on it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use nodedb_types::Hlc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// What a gate guards.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum GateKey {
    /// An array incarnation.
    Array(Hlc),
    /// A collection's storage key.
    Collection {
        database_id: u64,
        tenant_id: u64,
        name: String,
    },
}

type GateMap = HashMap<GateKey, Arc<RwLock<()>>>;

static GATES: Mutex<Option<GateMap>> = Mutex::new(None);

fn gates() -> MutexGuard<'static, Option<GateMap>> {
    GATES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn gate_of(key: &GateKey) -> Arc<RwLock<()>> {
    let mut gates = gates();
    Arc::clone(
        gates
            .get_or_insert_with(HashMap::new)
            .entry(key.clone())
            .or_default(),
    )
}

/// A held gate. Dropping it releases the gate and forgets it when no one
/// else holds or waits on it.
pub(crate) struct HeldGate<G> {
    guard: Option<G>,
    key: GateKey,
    lock: Arc<RwLock<()>>,
}

impl<G> Drop for HeldGate<G> {
    fn drop(&mut self) {
        self.guard.take();
        let mut gates = gates();
        // The map holds one reference and this gate the other: no one else
        // holds or waits on it. Every clone is taken under the map lock.
        if Arc::strong_count(&self.lock) == 2
            && let Some(map) = gates.as_mut()
        {
            map.remove(&self.key);
        }
    }
}

/// A shared hold, taken by a write from its routing until its core's queue
/// holds it.
pub(crate) type SharedGate = HeldGate<OwnedRwLockReadGuard<()>>;

/// An exclusive hold, taken by a rekey, drop or purge.
pub(crate) type ExclusiveGate = HeldGate<OwnedRwLockWriteGuard<()>>;

/// Hold `key`'s gate shared.
pub(crate) async fn shared(key: GateKey) -> SharedGate {
    let lock = gate_of(&key);
    let guard = Arc::clone(&lock).read_owned().await;
    HeldGate {
        guard: Some(guard),
        key,
        lock,
    }
}

/// Hold `key`'s gate shared if no one holds it exclusive, without waiting.
pub(crate) fn try_shared(key: GateKey) -> Option<SharedGate> {
    let lock = gate_of(&key);
    let guard = Arc::clone(&lock).try_read_owned().ok()?;
    Some(HeldGate {
        guard: Some(guard),
        key,
        lock,
    })
}

/// Hold every gate of `keys` shared, in key order, so two writes never wait
/// on each other.
pub(crate) async fn shared_all(mut keys: Vec<GateKey>) -> Vec<SharedGate> {
    keys.sort();
    keys.dedup();
    let mut held = Vec::with_capacity(keys.len());
    for key in keys {
        held.push(shared(key).await);
    }
    held
}

/// Hold `key`'s gate exclusive.
pub(crate) async fn exclusive(key: GateKey) -> ExclusiveGate {
    let lock = gate_of(&key);
    let guard = Arc::clone(&lock).write_owned().await;
    HeldGate {
        guard: Some(guard),
        key,
        lock,
    }
}

#[cfg(test)]
fn held(key: &GateKey) -> bool {
    gates().as_ref().is_some_and(|map| map.contains_key(key))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// An exclusive hold on one incarnation never blocks another
    /// incarnation's writes, and it blocks its own.
    #[tokio::test]
    async fn a_gate_blocks_only_its_own_incarnation() {
        let rekeyed = GateKey::Array(Hlc::new(9_001, 0));
        let other = GateKey::Array(Hlc::new(9_002, 0));
        let rekey = exclusive(rekeyed.clone()).await;

        let unrelated = tokio::time::timeout(Duration::from_millis(200), shared(other)).await;
        assert!(unrelated.is_ok(), "another incarnation's write proceeds");

        let blocked =
            tokio::time::timeout(Duration::from_millis(50), shared(rekeyed.clone())).await;
        assert!(blocked.is_err(), "the rekeyed incarnation's write waits");

        drop(rekey);
        let after = tokio::time::timeout(Duration::from_millis(200), shared(rekeyed)).await;
        assert!(after.is_ok(), "the write proceeds after the rekey");
    }

    #[tokio::test]
    async fn a_released_gate_is_forgotten() {
        let key = GateKey::Collection {
            database_id: 0,
            tenant_id: 1,
            name: "orders".into(),
        };
        let first = shared(key.clone()).await;
        let second = shared(key.clone()).await;
        drop(first);
        assert!(held(&key), "still held by the second write");
        drop(second);
        assert!(!held(&key));
    }
}
