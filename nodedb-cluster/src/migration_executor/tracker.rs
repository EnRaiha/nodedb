// SPDX-License-Identifier: BUSL-1.1

use std::sync::Mutex;
use std::time::Duration;

use crate::metadata_group::migration_state::MigrationId;
use crate::migration::MigrationState;

/// The migrations this node's executor ran, by migration id, with the state
/// each reached last.
pub struct MigrationTracker {
    active: Mutex<Vec<(MigrationId, MigrationState)>>,
}

impl MigrationTracker {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(Vec::new()),
        }
    }

    /// Record the current state of migration `id`, replacing the state
    /// recorded for it before.
    pub fn record(&self, id: MigrationId, state: &MigrationState) {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        match active.iter_mut().find(|(recorded, _)| *recorded == id) {
            Some((_, recorded)) => *recorded = state.clone(),
            None => active.push((id, state.clone())),
        }
    }

    pub fn active_count(&self) -> usize {
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active.iter().filter(|(_, s)| s.is_active()).count()
    }

    pub fn snapshot(&self) -> Vec<MigrationSnapshot> {
        let active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active
            .iter()
            .map(|(_, s)| MigrationSnapshot {
                vshard_id: s.vshard_id(),
                phase: format!("{:?}", s.phase()),
                elapsed_ms: s.elapsed().map(|d| d.as_millis() as u64).unwrap_or(0),
                is_active: s.is_active(),
            })
            .collect()
    }

    pub fn gc(&self, max_age: Duration) {
        let mut active = self.active.lock().unwrap_or_else(|p| p.into_inner());
        active.retain(|(_, s)| s.is_active() || s.elapsed().map(|d| d < max_age).unwrap_or(true));
    }
}

impl Default for MigrationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Observability snapshot of a migration.
#[derive(Debug, Clone)]
pub struct MigrationSnapshot {
    pub vshard_id: u32,
    pub phase: String,
    pub elapsed_ms: u64,
    pub is_active: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migration::MigrationState;

    #[test]
    fn migration_tracker_lifecycle() {
        let tracker = MigrationTracker::new();
        assert_eq!(tracker.active_count(), 0);

        let id = uuid::Uuid::new_v4();
        let mut state = MigrationState::new(0, 0, 1, 1, 2, 500_000);
        state.start_base_copy(100);
        tracker.record(id, &state);

        assert_eq!(tracker.active_count(), 1);
        assert_eq!(tracker.snapshot().len(), 1);
        assert!(tracker.snapshot()[0].is_active);

        // A later record of the same migration replaces its state.
        state.complete(10);
        tracker.record(id, &state);
        assert_eq!(tracker.snapshot().len(), 1);
        assert_eq!(tracker.active_count(), 0);
    }
}
