// SPDX-License-Identifier: BUSL-1.1

//! Shutdown phase enum. Mirrors [`crate::control::startup::StartupPhase`]
//! in reverse — drain in the opposite order subsystems were initialised.
//!
//! The compiler enforces exhaustiveness on every `match` over this type:
//! adding a new variant without updating `next()` and every match site
//! is a compile error.

use std::fmt;

/// Ordered shutdown phases. Each phase has a 500 ms drain budget.
/// Subsystems that do not call [`super::DrainGuard::report_drained`]
/// within the budget are aborted and logged as offenders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum ShutdownPhase {
    #[default]
    /// Normal operation — no shutdown in progress.
    Running,
    /// Listeners stop accepting new connections; in-flight handshakes
    /// complete. Corresponds to reversing `ListenersAccepting`.
    DrainingListeners,
    /// Raft leader step-down; lease release committed. Every loop that
    /// dispatches to the Data Plane is joined here, before the next phase
    /// closes the enqueue gate. Corresponds to reversing `GatewayEnable`.
    DrainingControlPlane,
    /// TPC Data Plane cores drain their request queues; WAL switches to
    /// accelerated group-commit (10 ms cadence). The response poller runs
    /// here, routing in-flight responses to their sessions. Corresponds to
    /// reversing `CatalogHydrated`.
    DrainingDataPlane,
    /// Trigger retry loops, CDC consumers, streaming MV persist — the Event
    /// Plane tasks that touch no Data Plane dispatch. Corresponds to
    /// reversing `RaftReady`.
    DrainingEventPlane,
    /// LSN watermarks are flushed to redb. Corresponds to reversing
    /// `StorageReady`.
    PersistingWatermarks,
    /// Final WAL fsync + redb checkpoint. After this the process exits.
    WalFsync,
    /// Shutdown complete — process is about to exit.
    Closed,
}

impl ShutdownPhase {
    /// Next phase in the shutdown sequence. Returns `None` only for
    /// `Closed` (terminal state). No `_ =>` — exhaustive by design.
    pub fn next(self) -> Option<Self> {
        match self {
            Self::Running => Some(Self::DrainingListeners),
            Self::DrainingListeners => Some(Self::DrainingControlPlane),
            Self::DrainingControlPlane => Some(Self::DrainingDataPlane),
            Self::DrainingDataPlane => Some(Self::DrainingEventPlane),
            Self::DrainingEventPlane => Some(Self::PersistingWatermarks),
            Self::PersistingWatermarks => Some(Self::WalFsync),
            Self::WalFsync => Some(Self::Closed),
            Self::Closed => None,
        }
    }

    /// Human-readable label for logging and metrics.
    pub fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::DrainingListeners => "draining_listeners",
            Self::DrainingControlPlane => "draining_control_plane",
            Self::DrainingDataPlane => "draining_data_plane",
            Self::DrainingEventPlane => "draining_event_plane",
            Self::PersistingWatermarks => "persisting_watermarks",
            Self::WalFsync => "wal_fsync",
            Self::Closed => "closed",
        }
    }
}

impl fmt::Display for ShutdownPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_is_exhaustive_and_terminates() {
        // Walk the entire chain — must reach Closed without looping.
        let mut phase = ShutdownPhase::Running;
        let mut count = 0usize;
        loop {
            count += 1;
            assert!(count < 20, "phase chain did not terminate");
            match phase.next() {
                Some(next) => phase = next,
                None => {
                    assert_eq!(phase, ShutdownPhase::Closed);
                    break;
                }
            }
        }
        // Exactly 8 phases (Running … Closed).
        assert_eq!(count, 8);
    }

    #[test]
    fn closed_has_no_next() {
        assert_eq!(ShutdownPhase::Closed.next(), None);
    }

    #[test]
    fn running_is_less_than_closed() {
        assert!(ShutdownPhase::Running < ShutdownPhase::Closed);
        assert!(ShutdownPhase::DrainingListeners < ShutdownPhase::WalFsync);
    }

    #[test]
    fn labels_are_unique() {
        use std::collections::HashSet;
        let phases = [
            ShutdownPhase::Running,
            ShutdownPhase::DrainingListeners,
            ShutdownPhase::DrainingControlPlane,
            ShutdownPhase::DrainingDataPlane,
            ShutdownPhase::DrainingEventPlane,
            ShutdownPhase::PersistingWatermarks,
            ShutdownPhase::WalFsync,
            ShutdownPhase::Closed,
        ];
        let labels: HashSet<_> = phases.iter().map(|p| p.label()).collect();
        assert_eq!(labels.len(), phases.len());
    }
}
