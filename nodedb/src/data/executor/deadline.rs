// SPDX-License-Identifier: BUSL-1.1

//! Cooperative deadline enforcement inside Data-Plane execution.
//!
//! The Control -> Data request envelope carries one absolute deadline
//! ([`Request::deadline`](crate::bridge::envelope::Request)). The core loop
//! refuses a task that is already past it before execution starts; this type
//! carries the same deadline INTO execution so a statement that goes over
//! while it runs stops at its next safe point instead of running to
//! completion.
//!
//! The clock is [`Instant`], matching the envelope field. `Instant` is
//! monotonic, so an NTP step or a leap second cannot stretch or shrink a
//! running statement's budget. A wall clock can move backwards and would.
//!
//! Cost of a check: `Instant::now()` reads the kernel clock page through the
//! vDSO on Linux — tens of nanoseconds, no syscall. That is the same order as
//! decoding one small document, so a clock read on every row is measurable.
//! [`DeadlineCheck::expired`] reads the clock once per [`STRIDE`] calls and
//! costs a decrement plus a predictable branch on every other call. At the
//! stride below, a scan overshoots its deadline by at most the time 1024 rows
//! take, which stays far under the millisecond granularity a
//! `statement_timeout` is expressed in.

use std::cell::Cell;
use std::time::{Duration, Instant};

use super::task::ExecutionTask;

/// Calls between two clock reads on the strided path.
const STRIDE: u32 = 1024;

/// Deadline stamped on a synthetic task built on a Data-Plane core: startup WAL
/// replay, and the internal sub-plans replay drives.
///
/// A literal is correct here, and the configured default is not reachable. A
/// Data-Plane core holds no `SharedState` handle — taking one would put
/// Control-Plane state inside the Data Plane — and there is no client session
/// to read a `statement_timeout` from. A statement budget would also be the
/// wrong shape: abandoning replay partway leaves engine state short of the WAL,
/// which is data loss rather than a cancelled query. The field exists on the
/// envelope, so replay fills it; no safe point on the replay path consults it.
pub(in crate::data::executor) const REPLAY_DEADLINE: Duration = Duration::from_secs(60);

/// A task's deadline, checkable from a row loop.
///
/// `!Send` by construction (it holds [`Cell`]s), which is what the Data Plane
/// requires. Interior mutability lets an `Fn` predicate closure consult it.
pub(in crate::data::executor) struct DeadlineCheck {
    deadline: Instant,
    countdown: Cell<u32>,
    tripped: Cell<bool>,
}

impl DeadlineCheck {
    /// Take the deadline off the request envelope this task arrived on.
    pub(in crate::data::executor) fn for_task(task: &ExecutionTask) -> Self {
        Self {
            deadline: task.request.deadline,
            // The first call reads the clock: a task that entered execution
            // barely inside its deadline must stop on its first row, not after
            // a full stride.
            countdown: Cell::new(1),
            tripped: Cell::new(false),
        }
    }

    /// Strided check for a row loop. Reads the clock once per [`STRIDE`] calls.
    pub(in crate::data::executor) fn expired(&self) -> bool {
        if self.tripped.get() {
            return true;
        }
        let remaining = self.countdown.get() - 1;
        if remaining > 0 {
            self.countdown.set(remaining);
            return false;
        }
        self.countdown.set(STRIDE);
        self.read_clock()
    }

    /// Unstrided check for a stage boundary — a completed fetch, a completed
    /// sort, one emitted chunk. These run once per stage, so the clock read is
    /// free relative to the stage itself.
    pub(in crate::data::executor) fn expired_now(&self) -> bool {
        if self.tripped.get() {
            return true;
        }
        self.read_clock()
    }

    /// Whether an earlier check already found the deadline passed. Reads no
    /// clock, so a caller can ask after a scan returned without paying for it.
    pub(in crate::data::executor) fn tripped(&self) -> bool {
        self.tripped.get()
    }

    fn read_clock(&self) -> bool {
        let over = Instant::now() > self.deadline;
        if over {
            self.tripped.set(true);
        }
        over
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn check_at(deadline: Instant) -> DeadlineCheck {
        DeadlineCheck {
            deadline,
            countdown: Cell::new(1),
            tripped: Cell::new(false),
        }
    }

    #[test]
    fn first_strided_call_reads_the_clock() {
        let check = check_at(Instant::now() - Duration::from_millis(1));
        assert!(check.expired(), "an already-passed deadline trips on row 1");
    }

    #[test]
    fn live_deadline_never_trips() {
        let check = check_at(Instant::now() + Duration::from_secs(3600));
        for _ in 0..(STRIDE * 3) {
            assert!(!check.expired());
        }
        assert!(!check.tripped());
    }

    #[test]
    fn trip_latches() {
        let check = check_at(Instant::now() - Duration::from_millis(1));
        assert!(check.expired_now());
        assert!(check.tripped(), "the latch survives without a clock read");
        assert!(check.expired());
    }

    #[test]
    fn stride_bounds_clock_reads() {
        // A live deadline leaves the countdown mid-stride after one call, which
        // is what makes the per-row cost a decrement rather than a clock read.
        let check = check_at(Instant::now() + Duration::from_secs(3600));
        assert!(!check.expired());
        assert_eq!(check.countdown.get(), STRIDE);
        assert!(!check.expired());
        assert_eq!(check.countdown.get(), STRIDE - 1);
    }
}
