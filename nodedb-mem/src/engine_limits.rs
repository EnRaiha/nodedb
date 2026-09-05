// SPDX-License-Identifier: Apache-2.0

//! Total per-engine byte limit map.
//!
//! `EngineLimits` holds exactly one byte limit for every `EngineId` —
//! absence is unrepresentable. A limit of 0 is a valid config: the engine
//! rejects every non-zero reservation, it is never a missing-entry error.

use crate::engine::EngineId;

/// A byte limit for every `EngineId`, indexed by `EngineId::index()`.
#[derive(Debug, Clone)]
pub struct EngineLimits([usize; EngineId::COUNT]);

impl EngineLimits {
    /// Every engine limited to zero bytes.
    pub const fn zeroed() -> Self {
        Self([0; EngineId::COUNT])
    }

    /// Every engine limited to the same number of bytes.
    pub const fn uniform(bytes: usize) -> Self {
        Self([bytes; EngineId::COUNT])
    }

    /// Consuming builder: set `engine`'s limit to `bytes`.
    pub fn with(mut self, engine: EngineId, bytes: usize) -> Self {
        self.set(engine, bytes);
        self
    }

    /// Set `engine`'s limit to `bytes`.
    pub fn set(&mut self, engine: EngineId, bytes: usize) {
        self.0[engine.index()] = bytes;
    }

    /// The byte limit configured for `engine`.
    pub fn get(&self, engine: EngineId) -> usize {
        self.0[engine.index()]
    }

    /// Sum of every engine's limit.
    pub fn total(&self) -> usize {
        self.0.iter().sum()
    }

    /// Iterate `(engine, limit)` pairs in `EngineId::ALL` order.
    pub fn iter(&self) -> impl Iterator<Item = (EngineId, usize)> + '_ {
        EngineId::ALL
            .iter()
            .map(|&engine| (engine, self.get(engine)))
    }

    /// Borrow the raw per-engine array, indexed by `EngineId::index()`.
    pub(crate) fn as_array(&self) -> &[usize; EngineId::COUNT] {
        &self.0
    }
}

impl FromIterator<(EngineId, usize)> for EngineLimits {
    /// Builds a total map from a partial iterator. An engine not yielded
    /// keeps its zero default rather than being absent.
    fn from_iter<T: IntoIterator<Item = (EngineId, usize)>>(iter: T) -> Self {
        let mut limits = Self::zeroed();
        for (engine, bytes) in iter {
            limits.set(engine, bytes);
        }
        limits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zeroed_has_no_limit() {
        let limits = EngineLimits::zeroed();
        for &engine in EngineId::ALL {
            assert_eq!(limits.get(engine), 0);
        }
        assert_eq!(limits.total(), 0);
    }

    #[test]
    fn uniform_applies_to_every_engine() {
        let limits = EngineLimits::uniform(1024);
        for &engine in EngineId::ALL {
            assert_eq!(limits.get(engine), 1024);
        }
        assert_eq!(limits.total(), 1024 * EngineId::COUNT);
    }

    #[test]
    fn with_sets_a_single_engine() {
        let limits = EngineLimits::zeroed()
            .with(EngineId::Vector, 100)
            .with(EngineId::Query, 200);
        assert_eq!(limits.get(EngineId::Vector), 100);
        assert_eq!(limits.get(EngineId::Query), 200);
        assert_eq!(limits.get(EngineId::Kv), 0);
        assert_eq!(limits.total(), 300);
    }

    #[test]
    fn set_mutates_in_place() {
        let mut limits = EngineLimits::zeroed();
        limits.set(EngineId::Crdt, 42);
        assert_eq!(limits.get(EngineId::Crdt), 42);
    }

    #[test]
    fn iter_covers_every_engine_in_all_order() {
        let limits = EngineLimits::zeroed().with(EngineId::Fts, 7);
        let collected: Vec<_> = limits.iter().collect();
        assert_eq!(collected.len(), EngineId::COUNT);
        assert_eq!(
            collected.iter().map(|(e, _)| *e).collect::<Vec<_>>(),
            EngineId::ALL.to_vec()
        );
        assert_eq!(
            collected
                .iter()
                .find(|(e, _)| *e == EngineId::Fts)
                .unwrap()
                .1,
            7
        );
    }

    #[test]
    fn from_iter_leaves_unlisted_engines_zero() {
        let limits: EngineLimits = [(EngineId::Vector, 10), (EngineId::Kv, 20)]
            .into_iter()
            .collect();
        assert_eq!(limits.get(EngineId::Vector), 10);
        assert_eq!(limits.get(EngineId::Kv), 20);
        assert_eq!(limits.get(EngineId::Graph), 0);
    }
}
