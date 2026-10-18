// SPDX-License-Identifier: BUSL-1.1

//! Leader/follower group commit for a WAL whose appends and syncs are separate steps.
//!
//! A caller appends its records first and notes the highest LSN it appended.
//! It then calls [`GroupCommit::sync_through`] with that LSN.
//! With no sync in flight, the caller becomes the leader and syncs everything appended so far.
//! With a sync in flight, the caller waits until a sync covers its LSN.
//! So concurrent callers share one sync.
//!
//! A failed sync covers a range of LSNs. Every caller whose LSN is in that range gets an error.
//! None of them reports success, even when a later sync succeeds.

use std::sync::{Condvar, Mutex, MutexGuard};

/// One failed sync: the LSNs it covered and what failed.
struct FailedSync {
    from: u64,
    through: u64,
    detail: String,
}

struct GroupState {
    /// Every LSN at or below this is durable.
    synced: u64,
    /// Whether a leader is running a sync now.
    syncing: bool,
    /// Every failed sync. A failure is rare, so the list stays short.
    failed: Vec<FailedSync>,
}

/// Shared group-commit state for one WAL.
pub struct GroupCommit {
    state: Mutex<GroupState>,
    changed: Condvar,
}

/// Clears the leader flag on every exit, a panic included, and wakes the followers.
struct LeaderTurn<'a> {
    commit: &'a GroupCommit,
}

impl Drop for LeaderTurn<'_> {
    fn drop(&mut self) {
        self.commit.lock().syncing = false;
        self.commit.changed.notify_all();
    }
}

impl GroupCommit {
    /// Start with every LSN at or below `synced` durable.
    pub fn new(synced: u64) -> Self {
        Self {
            state: Mutex::new(GroupState {
                synced,
                syncing: false,
                failed: Vec::new(),
            }),
            changed: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, GroupState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Wait until `lsn` is durable.
    ///
    /// `sync` makes every appended record durable.
    /// It returns the highest LSN appended before it synced, and the sync result.
    /// The caller must hold no lock that an appender or `sync` needs.
    pub fn sync_through(
        &self,
        lsn: u64,
        sync: impl Fn() -> (u64, crate::Result<()>),
    ) -> crate::Result<()> {
        let mut state = self.lock();
        loop {
            if let Some(failure) = state
                .failed
                .iter()
                .find(|failure| (failure.from..=failure.through).contains(&lsn))
            {
                return Err(crate::Error::Storage {
                    engine: "audit_wal".into(),
                    detail: format!(
                        "audit WAL sync failed for LSNs {}..={}: {}",
                        failure.from, failure.through, failure.detail
                    ),
                });
            }
            if state.synced >= lsn {
                return Ok(());
            }
            if state.syncing {
                state = match self.changed.wait(state) {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                continue;
            }

            // This caller leads: it syncs everything appended so far, with no group lock held.
            state.syncing = true;
            let from = state.synced.saturating_add(1);
            drop(state);
            let turn = LeaderTurn { commit: self };
            let (through, result) = sync();
            {
                let mut state = self.lock();
                match result {
                    Ok(()) => state.synced = state.synced.max(through),
                    Err(e) => {
                        tracing::error!(error = %e, from, through, "audit WAL sync failed");
                        state.failed.push(FailedSync {
                            from,
                            through: through.max(from),
                            detail: e.to_string(),
                        });
                    }
                }
            }
            drop(turn);
            state = self.lock();
        }
    }

    /// The highest durable LSN.
    pub fn synced(&self) -> u64 {
        self.lock().synced
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::*;

    #[test]
    fn concurrent_callers_share_syncs() {
        let commit = Arc::new(GroupCommit::new(0));
        let appended = Arc::new(AtomicU64::new(0));
        let syncs = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(32));

        let threads: Vec<_> = (0..32)
            .map(|_| {
                let (commit, appended, syncs, start) = (
                    Arc::clone(&commit),
                    Arc::clone(&appended),
                    Arc::clone(&syncs),
                    Arc::clone(&start),
                );
                std::thread::spawn(move || {
                    start.wait();
                    let lsn = appended.fetch_add(1, Ordering::SeqCst) + 1;
                    commit.sync_through(lsn, || {
                        let through = appended.load(Ordering::SeqCst);
                        // A slow disk: every other caller appends while this sync runs.
                        std::thread::sleep(Duration::from_millis(30));
                        syncs.fetch_add(1, Ordering::SeqCst);
                        (through, Ok(()))
                    })
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("caller thread").expect("durable");
        }
        let syncs = syncs.load(Ordering::SeqCst);
        assert!(syncs <= 3, "32 callers must share syncs, saw {syncs}");
        assert_eq!(commit.synced(), 32);
    }

    #[test]
    fn a_failed_sync_reaches_every_covered_waiter() {
        let commit = Arc::new(GroupCommit::new(0));
        let appended = Arc::new(AtomicU64::new(0));
        let start = Arc::new(Barrier::new(8));

        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (commit, appended, start) = (
                    Arc::clone(&commit),
                    Arc::clone(&appended),
                    Arc::clone(&start),
                );
                std::thread::spawn(move || {
                    start.wait();
                    let lsn = appended.fetch_add(1, Ordering::SeqCst) + 1;
                    let outcome = commit.sync_through(lsn, || {
                        let through = appended.load(Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(30));
                        (
                            through,
                            Err(crate::Error::Internal {
                                detail: "injected fsync error".into(),
                            }),
                        )
                    });
                    (lsn, outcome)
                })
            })
            .collect();
        for thread in threads {
            let (lsn, outcome) = thread.join().expect("caller thread");
            assert!(outcome.is_err(), "LSN {lsn} must see the sync error");
        }
        assert_eq!(commit.synced(), 0, "a failed sync makes nothing durable");
    }

    #[test]
    fn a_later_success_does_not_clear_an_earlier_failure() {
        let commit = GroupCommit::new(0);
        let failed = commit.sync_through(1, || {
            (
                1,
                Err(crate::Error::Internal {
                    detail: "injected fsync error".into(),
                }),
            )
        });
        assert!(failed.is_err());
        assert!(commit.sync_through(2, || (2, Ok(()))).is_ok());
        assert!(
            commit.sync_through(1, || (2, Ok(()))).is_err(),
            "LSN 1 was covered by the failed sync"
        );
    }

    #[test]
    fn a_leader_panic_releases_the_followers() {
        let commit = GroupCommit::new(0);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            commit.sync_through(1, || panic!("injected sync panic"))
        }));
        assert!(outcome.is_err());
        assert!(
            commit.sync_through(1, || (1, Ok(()))).is_ok(),
            "the next caller must become leader"
        );
    }
}
