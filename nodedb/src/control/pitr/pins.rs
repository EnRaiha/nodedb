// SPDX-License-Identifier: BUSL-1.1

//! Object keys pinned by kept base snapshots.
//!
//! One pin set holds the cold-store keys the bases reference, another the
//! chunk ids they list. A deleter checks the pins first and skips a pinned
//! key. It holds the read lock of the set across the check and the delete,
//! and a base pins its keys under the write lock before it relies on them,
//! so no key is deleted between the two.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicU64, Ordering};

/// Prefix of the name a base still being written pins its keys under. A real
/// base prefix starts with `snap-`.
pub const PENDING_BASE: &str = "pending";

/// A pin name no other base in progress holds.
pub fn pending_pin_name() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{PENDING_BASE}-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

#[derive(Debug)]
pub struct ColdPins {
    /// Keys each base references, by base prefix.
    by_base: HashMap<String, Vec<String>>,
    /// How many bases reference each key.
    counts: HashMap<String, usize>,
    /// `false` while a snapshot prefix's manifest does not load. Its keys are
    /// unknown, so every key counts as pinned.
    complete: bool,
}

impl Default for ColdPins {
    fn default() -> Self {
        Self {
            by_base: HashMap::new(),
            counts: HashMap::new(),
            complete: true,
        }
    }
}

impl ColdPins {
    /// Pins built from every base, as `(prefix, cold keys)`.
    pub fn from_bases<'a>(
        bases: impl IntoIterator<Item = (&'a str, &'a [String])>,
        complete: bool,
    ) -> Self {
        let mut pins = Self {
            complete,
            ..Self::default()
        };
        for (base, keys) in bases {
            pins.pin(base, keys.to_vec());
        }
        pins
    }

    /// Replace every kept base's pins with `bases`. Pins of bases still being
    /// written stay: a rebuild from the store cannot see them yet.
    pub fn rebuild<'a>(
        &mut self,
        bases: impl IntoIterator<Item = (&'a str, &'a [String])>,
        complete: bool,
    ) {
        let mut rebuilt = Self::from_bases(bases, complete);
        for (base, keys) in self.by_base.drain() {
            if base.starts_with(PENDING_BASE) {
                rebuilt.pin(&base, keys);
            }
        }
        *self = rebuilt;
    }

    /// Pin `keys` for `base`, replacing what `base` pinned before.
    pub fn pin(&mut self, base: &str, keys: Vec<String>) {
        self.unpin(base);
        for key in &keys {
            *self.counts.entry(key.clone()).or_default() += 1;
        }
        self.by_base.insert(base.to_owned(), keys);
    }

    /// Release every key `base` pinned.
    pub fn unpin(&mut self, base: &str) {
        let Some(keys) = self.by_base.remove(base) else {
            return;
        };
        for key in keys {
            if let Entry::Occupied(mut count) = self.counts.entry(key) {
                *count.get_mut() -= 1;
                if *count.get() == 0 {
                    count.remove();
                }
            }
        }
    }

    /// Move the pins of `from` to `to`.
    pub fn rename(&mut self, from: &str, to: &str) {
        if let Some(keys) = self.by_base.remove(from) {
            self.by_base.insert(to.to_owned(), keys);
        }
    }

    pub fn is_pinned(&self, key: &str) -> bool {
        !self.complete || self.counts.contains_key(key)
    }

    /// Keys pinned by any base.
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn a_key_stays_pinned_until_its_last_base_unpins() {
        let mut pins = ColdPins::default();
        pins.pin("snap-1", keys(&["a", "b"]));
        pins.pin("snap-2", keys(&["b"]));
        pins.unpin("snap-1");
        assert!(!pins.is_pinned("a"));
        assert!(pins.is_pinned("b"));
        pins.unpin("snap-2");
        assert!(pins.is_empty());
    }

    #[test]
    fn a_pending_base_keeps_its_keys_through_the_rename() {
        let mut pins = ColdPins::default();
        pins.pin(PENDING_BASE, keys(&["a"]));
        pins.rename(PENDING_BASE, "snap-9");
        assert!(pins.is_pinned("a"));
        pins.unpin(PENDING_BASE);
        assert!(pins.is_pinned("a"));
        pins.unpin("snap-9");
        assert!(!pins.is_pinned("a"));
    }

    #[test]
    fn a_rebuild_keeps_the_pins_of_a_base_in_progress() {
        let mut pins = ColdPins::default();
        let pending = pending_pin_name();
        assert_ne!(pending, pending_pin_name());
        pins.pin(&pending, keys(&["new"]));
        pins.pin("snap-1", keys(&["old"]));
        let kept = keys(&["kept"]);
        pins.rebuild([("snap-2", kept.as_slice())], true);
        assert!(pins.is_pinned("new"));
        assert!(pins.is_pinned("kept"));
        assert!(!pins.is_pinned("old"));
        pins.unpin(&pending);
        assert!(!pins.is_pinned("new"));
    }

    #[test]
    fn incomplete_pins_pin_every_key() {
        let pins = ColdPins::from_bases([], false);
        assert!(pins.is_pinned("anything"));
    }
}
