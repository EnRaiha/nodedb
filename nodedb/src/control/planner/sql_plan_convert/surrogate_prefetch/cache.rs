// SPDX-License-Identifier: BUSL-1.1

//! The surrogate answers one plan batch's bind step resolved, and the keys a
//! conversion pass asked for without an answer.
//!
//! Conversion never draws a surrogate or asks a key's home. It reads the
//! answers held here. A key with no answer is recorded as a miss, and the
//! pass goes on with a placeholder. The bind step then resolves the misses,
//! awaiting each draw and home request, and converts the plan again.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use nodedb_types::{CollectionKey, DatabaseId, Surrogate};

use super::super::array_convert::ArrayCellKeys;

/// One collection: its database and bare catalog name.
type CollectionSlot = (DatabaseId, String);

/// A fresh identity drawn ahead: its surrogate and its identity string.
type FreshIdentity = (Surrogate, String);

/// One `INSERT INTO ARRAY` row batch: its array, and the address and length
/// of the plan's rows. The bind step borrows the plans for its whole run, so
/// the address names one batch until [`PrefetchedSurrogates::forget_array_keys`]
/// runs at its end.
type ArrayRowsSlot = (CollectionSlot, usize, usize);

/// One collection's bind answers: primary key to its bound surrogate, or
/// `None` when the key's home binds none.
type KeyAnswers = HashMap<Vec<u8>, Option<Surrogate>>;

/// The bind step's answer for each `(collection, primary key)`: the bound
/// surrogate, or `None` when the key's home binds none. Also the fresh
/// identities drawn ahead for rows that name no key, and the misses of the
/// running conversion pass. All hold for the one plan batch they belong to.
#[derive(Debug, Default)]
pub struct PrefetchedSurrogates {
    /// Keyed database, then collection, then key, so a lookup borrows its
    /// key and allocates nothing.
    answers: HashMap<DatabaseId, HashMap<String, KeyAnswers>>,
    /// The coerced coordinates and identity keys of each `INSERT INTO ARRAY`
    /// row batch the bind step read, so conversion encodes neither again.
    array_keys: HashMap<ArrayRowsSlot, ArrayCellKeys>,
    /// Conversion reads through a shared reference, so the fresh pool and
    /// the misses sit behind locks.
    fresh: Mutex<FreshPool>,
    misses: Mutex<SurrogateMisses>,
}

/// Fresh identities per collection, each already bound under its identity,
/// handed out in draw order.
#[derive(Debug, Default)]
struct FreshPool {
    drawn: HashMap<CollectionSlot, Vec<FreshIdentity>>,
    /// How many of each collection's identities conversion has taken.
    taken: HashMap<CollectionSlot, usize>,
}

/// How far conversion has taken each collection's fresh identities. A pass
/// that is converted again rewinds to the mark taken before it.
#[derive(Debug, Clone, Default)]
pub struct FreshMark(HashMap<CollectionSlot, usize>);

/// The keys one conversion pass asked for without an answer, per collection.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SurrogateMisses {
    collections: HashMap<CollectionSlot, CollectionMisses>,
}

/// The misses of one collection.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct CollectionMisses {
    /// Keys a write binds when the key has no surrogate.
    pub binds: HashSet<Vec<u8>>,
    /// Keys a read or a key-preserving write only looks up at their home.
    pub lookups: HashSet<Vec<u8>>,
    /// Fresh identities asked for beyond those drawn.
    pub fresh: usize,
}

impl SurrogateMisses {
    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }

    /// How many answers the misses ask for: each key, and each fresh
    /// identity.
    pub fn len(&self) -> usize {
        self.collections
            .values()
            .map(|m| m.binds.len() + m.lookups.len() + m.fresh)
            .sum()
    }

    /// Each collection's key and misses.
    pub fn iter(&self) -> impl Iterator<Item = (CollectionKey<'_>, &CollectionMisses)> {
        self.collections
            .iter()
            .map(|((database_id, name), misses)| {
                (CollectionKey::from_bare(*database_id, name), misses)
            })
    }

    /// The names of the collections with misses, sorted, for an error
    /// message.
    pub fn collection_names(&self) -> String {
        let mut names: Vec<&str> = self
            .collections
            .keys()
            .map(|(_, name)| name.as_str())
            .collect();
        names.sort_unstable();
        names.join(", ")
    }

    fn collection(&mut self, key: CollectionKey<'_>) -> &mut CollectionMisses {
        self.collections.entry(slot_of(key)).or_default()
    }
}

impl PrefetchedSurrogates {
    /// Keep the answer for `pk` in `key`.
    pub fn insert(&mut self, key: CollectionKey<'_>, pk: &[u8], answer: Option<Surrogate>) {
        let by_name = self.answers.entry(key.database_id()).or_default();
        // The collection's map exists after its first key, so later keys
        // allocate no name.
        if let Some(keys) = by_name.get_mut(key.name()) {
            match keys.get_mut(pk) {
                Some(slot) => *slot = answer,
                None => {
                    keys.insert(pk.to_vec(), answer);
                }
            }
            return;
        }
        let mut keys = HashMap::new();
        keys.insert(pk.to_vec(), answer);
        by_name.insert(key.name().to_string(), keys);
    }

    /// The answer for `pk` in `key`. `None` when the key has no answer.
    pub fn get(&self, key: CollectionKey<'_>, pk: &[u8]) -> Option<Option<Surrogate>> {
        self.answers
            .get(&key.database_id())?
            .get(key.name())?
            .get(pk)
            .copied()
    }

    /// Keep the coerced coordinates and identity keys the bind step read off
    /// the `INSERT INTO ARRAY` rows at `rows` in `key`.
    pub(in crate::control::planner::sql_plan_convert) fn keep_array_keys<T>(
        &mut self,
        key: CollectionKey<'_>,
        rows: &[T],
        keys: ArrayCellKeys,
    ) {
        self.array_keys.insert(array_rows_slot(key, rows), keys);
    }

    /// The coordinates and identity keys kept for the rows at `rows` in
    /// `key`. `None` when the bind step kept none, or kept them for a batch
    /// of another length.
    pub(in crate::control::planner::sql_plan_convert) fn array_keys<T>(
        &self,
        key: CollectionKey<'_>,
        rows: &[T],
    ) -> Option<&ArrayCellKeys> {
        if self.array_keys.is_empty() {
            return None;
        }
        self.array_keys
            .get(&array_rows_slot(key, rows))
            .filter(|keys| keys.pks.len() == rows.len())
    }

    /// Drop every kept array row batch. Runs when the bind step ends, before
    /// the plans whose addresses key them are released.
    pub(in crate::control::planner::sql_plan_convert) fn forget_array_keys(&mut self) {
        self.array_keys.clear();
    }

    /// The bound surrogate of `pk` in `key`, when the answer binds one.
    pub fn bound(&self, key: CollectionKey<'_>, pk: &[u8]) -> Option<Surrogate> {
        self.get(key, pk).flatten()
    }

    /// Keep a fresh identity drawn for a row of `key` that names no key.
    pub fn push_fresh(&mut self, key: CollectionKey<'_>, fresh: FreshIdentity) {
        self.fresh
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .drawn
            .entry(slot_of(key))
            .or_default()
            .push(fresh);
    }

    /// Take the next fresh identity drawn for `key`. `None` once the drawn
    /// identities run out.
    pub fn take_fresh(&self, key: CollectionKey<'_>) -> Option<FreshIdentity> {
        let mut pool = self.fresh.lock().unwrap_or_else(|p| p.into_inner());
        if pool.drawn.is_empty() {
            return None;
        }
        let slot = slot_of(key);
        let taken = pool.taken.get(&slot).copied().unwrap_or(0);
        let fresh = pool.drawn.get(&slot)?.get(taken)?.clone();
        pool.taken.insert(slot, taken + 1);
        Some(fresh)
    }

    /// Where conversion has taken each collection's fresh identities up to.
    pub fn fresh_mark(&self) -> FreshMark {
        let pool = self.fresh.lock().unwrap_or_else(|p| p.into_inner());
        FreshMark(pool.taken.clone())
    }

    /// Hand the fresh identities taken since `mark` out again, to a pass
    /// that converts the same plan again.
    pub fn rewind_fresh(&self, mark: &FreshMark) {
        let mut pool = self.fresh.lock().unwrap_or_else(|p| p.into_inner());
        pool.taken = mark.0.clone();
    }

    /// Record that a write asked to bind `pk` in `key` and found no answer.
    pub fn record_bind_miss(&self, key: CollectionKey<'_>, pk: &[u8]) {
        self.lock_misses().collection(key).binds.insert(pk.to_vec());
    }

    /// Record that a read asked for the binding of `pk` in `key` and found
    /// no answer.
    pub fn record_lookup_miss(&self, key: CollectionKey<'_>, pk: &[u8]) {
        self.lock_misses()
            .collection(key)
            .lookups
            .insert(pk.to_vec());
    }

    /// Record that a row of `key` asked for a fresh identity beyond those
    /// drawn.
    pub fn record_fresh_miss(&self, key: CollectionKey<'_>) {
        self.lock_misses().collection(key).fresh += 1;
    }

    /// Take the misses recorded since the last take.
    pub fn take_misses(&self) -> SurrogateMisses {
        std::mem::take(&mut *self.lock_misses())
    }

    fn lock_misses(&self) -> std::sync::MutexGuard<'_, SurrogateMisses> {
        self.misses.lock().unwrap_or_else(|p| p.into_inner())
    }
}

fn slot_of(key: CollectionKey<'_>) -> CollectionSlot {
    (key.database_id(), key.name().to_string())
}

fn array_rows_slot<T>(key: CollectionKey<'_>, rows: &[T]) -> ArrayRowsSlot {
    (slot_of(key), rows.as_ptr() as usize, rows.len())
}

#[cfg(test)]
mod tests {
    use nodedb_types::{CollectionKey, DatabaseId, Surrogate};

    use super::PrefetchedSurrogates;

    #[test]
    fn answers_are_kept_per_collection_and_key() {
        let users = CollectionKey::from_bare(DatabaseId::DEFAULT, "users");
        let orders = CollectionKey::from_bare(DatabaseId::DEFAULT, "orders");
        let mut prefetched = PrefetchedSurrogates::default();
        prefetched.insert(users, b"alice", Some(Surrogate::new(7)));
        prefetched.insert(users, b"bob", None);

        assert_eq!(
            prefetched.get(users, b"alice"),
            Some(Some(Surrogate::new(7)))
        );
        assert_eq!(prefetched.bound(users, b"alice"), Some(Surrogate::new(7)));
        assert_eq!(prefetched.get(users, b"bob"), Some(None));
        assert_eq!(prefetched.bound(users, b"bob"), None);
        assert_eq!(prefetched.get(users, b"carol"), None);
        assert_eq!(prefetched.get(orders, b"alice"), None);
    }

    #[test]
    fn fresh_identities_are_taken_in_draw_order_per_collection() {
        let users = CollectionKey::from_bare(DatabaseId::DEFAULT, "users");
        let orders = CollectionKey::from_bare(DatabaseId::DEFAULT, "orders");
        let mut prefetched = PrefetchedSurrogates::default();
        prefetched.push_fresh(users, (Surrogate::new(3), "3".to_string()));
        prefetched.push_fresh(users, (Surrogate::new(4), "4".to_string()));

        assert_eq!(prefetched.take_fresh(orders), None);
        assert_eq!(
            prefetched.take_fresh(users),
            Some((Surrogate::new(3), "3".to_string()))
        );
        assert_eq!(
            prefetched.take_fresh(users),
            Some((Surrogate::new(4), "4".to_string()))
        );
        assert_eq!(prefetched.take_fresh(users), None);
    }

    #[test]
    fn a_rewound_pass_takes_the_same_fresh_identities_again() {
        let users = CollectionKey::from_bare(DatabaseId::DEFAULT, "users");
        let mut prefetched = PrefetchedSurrogates::default();
        prefetched.push_fresh(users, (Surrogate::new(3), "3".to_string()));
        prefetched.push_fresh(users, (Surrogate::new(4), "4".to_string()));
        assert_eq!(
            prefetched.take_fresh(users).map(|f| f.0),
            Some(Surrogate::new(3))
        );

        let mark = prefetched.fresh_mark();
        assert_eq!(
            prefetched.take_fresh(users).map(|f| f.0),
            Some(Surrogate::new(4))
        );
        prefetched.rewind_fresh(&mark);
        assert_eq!(
            prefetched.take_fresh(users).map(|f| f.0),
            Some(Surrogate::new(4))
        );
    }

    #[test]
    fn array_keys_are_kept_per_row_batch_until_forgotten() {
        use nodedb_array::types::coord::value::CoordValue;

        use super::ArrayCellKeys;

        let scene = CollectionKey::from_bare(DatabaseId::DEFAULT, "scene");
        let rows = vec![1u8, 2];
        let other_rows = vec![1u8, 2];
        let mut prefetched = PrefetchedSurrogates::default();
        prefetched.keep_array_keys(
            scene,
            &rows,
            ArrayCellKeys {
                coords: vec![vec![CoordValue::Int64(1)], vec![CoordValue::Int64(2)]],
                pks: vec![b"a".to_vec(), b"b".to_vec()],
            },
        );

        let kept = prefetched.array_keys(scene, &rows).expect("kept");
        assert_eq!(kept.pks[1], b"b".to_vec());
        assert!(prefetched.array_keys(scene, &other_rows).is_none());
        assert!(prefetched.array_keys(scene, &rows[..1]).is_none());

        prefetched.forget_array_keys();
        assert!(prefetched.array_keys(scene, &rows).is_none());
    }

    #[test]
    fn misses_are_kept_per_collection_until_taken() {
        let users = CollectionKey::from_bare(DatabaseId::DEFAULT, "users");
        let orders = CollectionKey::from_bare(DatabaseId::DEFAULT, "orders");
        let prefetched = PrefetchedSurrogates::default();
        prefetched.record_bind_miss(users, b"a");
        prefetched.record_bind_miss(users, b"a");
        prefetched.record_lookup_miss(orders, b"o");
        prefetched.record_fresh_miss(users);
        prefetched.record_fresh_miss(users);

        let misses = prefetched.take_misses();
        assert_eq!(misses.len(), 4);
        assert_eq!(misses.collection_names(), "orders, users");
        for (key, recorded) in misses.iter() {
            if key.name() == "users" {
                assert_eq!(recorded.binds.len(), 1);
                assert_eq!(recorded.fresh, 2);
            } else {
                assert!(recorded.lookups.contains(b"o".as_slice()));
            }
        }
        assert!(prefetched.take_misses().is_empty());
    }
}
