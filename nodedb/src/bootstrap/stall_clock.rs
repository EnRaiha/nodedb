// SPDX-License-Identifier: BUSL-1.1

//! Progress clock for the boot readiness waits.
//!
//! A boot wait fails when it goes `bound` without progress, never on total
//! elapsed time. A node replaying a large backlog keeps making progress and
//! finishes, however long the replay takes. A wait that makes no progress fails
//! after `bound`.
//!
//! Each wait reports the items it still waits on. Each item carries `N`
//! progress measures, and each measure only counts upward. The clock resets
//! when a measure passes the highest value seen for its item, or when an item
//! stops waiting. A measure that drops and climbs back, such as a leader that
//! steps down and is elected again, does not reset the clock.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Tracks the last time a boot wait made progress.
#[derive(Debug)]
pub(crate) struct StallClock<const N: usize> {
    bound: Duration,
    last_progress: Instant,
    /// Highest value seen of each measure, per item still waiting.
    best: BTreeMap<u64, [u64; N]>,
}

impl<const N: usize> StallClock<N> {
    /// A clock that starts at `now` with no item seen.
    pub(crate) fn new(bound: Duration, now: Instant) -> Self {
        Self {
            bound,
            last_progress: now,
            best: BTreeMap::new(),
        }
    }

    /// The bound this clock fails at.
    pub(crate) fn bound(&self) -> Duration {
        self.bound
    }

    /// Fold one sample of the items still waiting, taken at `now`.
    ///
    /// An item seen for the first time sets its baseline and is not progress.
    pub(crate) fn observe(
        &mut self,
        now: Instant,
        waiting: impl IntoIterator<Item = (u64, [u64; N])>,
    ) {
        let mut progressed = false;
        let mut still_waiting = BTreeMap::new();
        for (id, measures) in waiting {
            let best = match self.best.remove(&id) {
                Some(mut best) => {
                    for (seen, current) in best.iter_mut().zip(measures) {
                        if current > *seen {
                            *seen = current;
                            progressed = true;
                        }
                    }
                    best
                }
                None => measures,
            };
            still_waiting.insert(id, best);
        }
        // Items left in `best` were waiting before and are done now.
        if !self.best.is_empty() {
            progressed = true;
        }
        self.best = still_waiting;
        if progressed {
            self.last_progress = now;
        }
    }

    /// Whether the wait has gone `bound` without progress at `now`.
    pub(crate) fn stalled(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_progress) >= self.bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUND: Duration = Duration::from_secs(10);

    fn at(start: Instant, secs: u64) -> Instant {
        start + Duration::from_secs(secs)
    }

    #[test]
    fn no_progress_stalls_at_the_bound() {
        let start = Instant::now();
        let mut clock = StallClock::<1>::new(BOUND, start);
        clock.observe(at(start, 0), [(1, [5])]);
        clock.observe(at(start, 9), [(1, [5])]);
        assert!(!clock.stalled(at(start, 9)));
        clock.observe(at(start, 10), [(1, [5])]);
        assert!(clock.stalled(at(start, 10)));
    }

    #[test]
    fn a_rising_measure_resets_the_clock() {
        let start = Instant::now();
        let mut clock = StallClock::<2>::new(BOUND, start);
        clock.observe(at(start, 0), [(1, [0, 3])]);
        clock.observe(at(start, 8), [(1, [0, 4])]);
        clock.observe(at(start, 16), [(1, [0, 4])]);
        assert!(!clock.stalled(at(start, 16)));
        assert!(clock.stalled(at(start, 18)));
    }

    #[test]
    fn a_measure_that_drops_and_climbs_back_is_not_progress() {
        let start = Instant::now();
        let mut clock = StallClock::<1>::new(BOUND, start);
        clock.observe(at(start, 0), [(1, [1])]);
        clock.observe(at(start, 4), [(1, [0])]);
        clock.observe(at(start, 8), [(1, [1])]);
        assert!(clock.stalled(at(start, 10)));
    }

    #[test]
    fn an_item_that_stops_waiting_is_progress() {
        let start = Instant::now();
        let mut clock = StallClock::<1>::new(BOUND, start);
        clock.observe(at(start, 0), [(1, [0]), (2, [0])]);
        clock.observe(at(start, 8), [(2, [0])]);
        assert!(!clock.stalled(at(start, 17)));
        assert!(clock.stalled(at(start, 18)));
    }

    #[test]
    fn an_item_seen_for_the_first_time_is_not_progress() {
        let start = Instant::now();
        let mut clock = StallClock::<1>::new(BOUND, start);
        clock.observe(at(start, 0), [(1, [0])]);
        clock.observe(at(start, 8), [(1, [0]), (2, [9])]);
        assert!(clock.stalled(at(start, 10)));
    }
}
