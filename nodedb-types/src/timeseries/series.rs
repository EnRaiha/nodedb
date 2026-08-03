// SPDX-License-Identifier: Apache-2.0

//! Series identity and catalog.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};

use serde::{Deserialize, Serialize};

/// Unique identifier for a timeseries (hash of metric name + sorted tag set).
pub type SeriesId = u64;

/// The canonical key for a series — used for collision detection in the
/// series catalog. Two `SeriesKey`s that hash to the same `SeriesId` are
/// a collision; the catalog rehashes with a salt.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

impl SeriesKey {
    pub fn new(metric: impl Into<String>, mut tags: Vec<(String, String)>) -> Self {
        tags.sort();
        Self {
            metric: metric.into(),
            tags,
        }
    }

    /// Compute the SeriesId for this key at a given rehash attempt.
    ///
    /// `rehash_attempt` is a plain collision-resolution counter — attempt 0 is
    /// the natural hash, and `SeriesCatalog::resolve` increments it until it
    /// lands on a free slot. It is NOT a cryptographic salt: the hash is
    /// `DefaultHasher`, the value is stored in the clear beside the key, and it
    /// carries no secrecy or uniqueness guarantee.
    pub fn to_series_id(&self, rehash_attempt: u64) -> SeriesId {
        let mut hasher = DefaultHasher::new();
        rehash_attempt.hash(&mut hasher);
        self.metric.hash(&mut hasher);
        for (k, v) in &self.tags {
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Persistent catalog that maps SeriesId → SeriesKey with collision detection.
///
/// On insert, if the SeriesId already maps to a *different* SeriesKey, the
/// catalog rehashes with an incrementing attempt counter until it finds a free
/// slot. This is one lookup per new series (not per row).
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SeriesCatalog {
    /// SeriesId → (SeriesKey, rehash attempt that produced this ID).
    entries: HashMap<SeriesId, (SeriesKey, u64)>,
}

impl SeriesCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve a SeriesKey to its SeriesId, registering it if new.
    ///
    /// Returns the SeriesId (potentially rehashed if the natural hash collided).
    pub fn resolve(&mut self, key: &SeriesKey) -> SeriesId {
        let mut rehash_attempt = 0u64;
        loop {
            let id = key.to_series_id(rehash_attempt);
            match self.entries.get(&id) {
                None => {
                    self.entries.insert(id, (key.clone(), rehash_attempt));
                    return id;
                }
                Some((existing_key, _)) if existing_key == key => {
                    return id;
                }
                Some(_) => {
                    rehash_attempt += 1;
                }
            }
        }
    }

    /// Look up a SeriesId to get its canonical key.
    pub fn get(&self, id: SeriesId) -> Option<&SeriesKey> {
        self.entries.get(&id).map(|(k, _)| k)
    }

    /// Number of registered series.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Persistent identity of a NodeDB-Lite database instance.
///
/// Generated as a CUID2 on first `open()`, stored in redb metadata.
/// Scope = one database file. Not a device ID, user ID, or app ID.
pub type LiteId = String;

/// Battery state reported by the host application for battery-aware flushing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum BatteryState {
    /// Battery level is sufficient (>50%) or device is on AC power.
    Normal,
    /// Battery is low (<20%) and not charging. Defer non-critical I/O.
    Low,
    /// Device is currently charging. Safe to flush.
    Charging,
    /// Battery state unknown (desktop, non-mobile). Treat as Normal.
    #[default]
    Unknown,
}

impl BatteryState {
    /// Whether flushing should be deferred in battery-aware mode.
    pub fn should_defer_flush(&self) -> bool {
        matches!(self, Self::Low)
    }
}
