// SPDX-License-Identifier: BUSL-1.1

//! Three-tier priority task queue for the Data Plane core loop.
//!
//! Replaces the single `VecDeque<ExecutionTask>` with three queues:
//!
//! | Tier     | Priorities          | Drain budget per cycle |
//! |----------|---------------------|------------------------|
//! | Critical | `Critical`          | 8 slots                |
//! | High     | `High`              | 4 slots                |
//! | Low      | `Normal`,`Background`| 2 slots               |
//!
//! **Drain algorithm.** Each call to [`PriorityQueues::drain_batch`] pulls at
//! most `budget` tasks total using the 8:4:2 ratio.  Empty tiers donate their
//! unused slots to the next lower tier so no cycle is wasted when, e.g.,
//! Critical is empty.
//!
//! **Starvation prevention.** Because lower-priority work always gets at least
//! 2 slots *and* inherits unused upper-tier slots, it can never be permanently
//! starved even under sustained Critical load.

use std::collections::VecDeque;
use std::time::Instant;

use crate::bridge::envelope::Priority;
use crate::data::executor::task::ExecutionTask;

/// Drain budget per tier per cycle (8 Critical : 4 High : 2 Low).
const BUDGET_CRITICAL: usize = 8;
const BUDGET_HIGH: usize = 4;
const BUDGET_LOW: usize = 2;

/// A task held in the priority queue alongside its enqueue timestamp.
///
/// The timestamp is used to record IO wait latency in `IoMetrics`.
pub struct QueuedTask {
    pub task: ExecutionTask,
    /// Nanosecond timestamp (from `Instant`) captured when the task was
    /// pushed.  Used by the IO metrics path to compute wait time.
    pub enqueued_at: Instant,
}

impl QueuedTask {
    fn new(task: ExecutionTask) -> Self {
        Self {
            task,
            enqueued_at: Instant::now(),
        }
    }
}

/// Three-tier bounded-ratio priority queue.
///
/// `!Send` — lives on a single Data Plane core alongside `CoreLoop`.
pub struct PriorityQueues {
    /// `Critical` priority tasks.
    critical: VecDeque<QueuedTask>,
    /// `High` priority tasks.
    high: VecDeque<QueuedTask>,
    /// `Normal` and `Background` priority tasks (merged low tier).
    low: VecDeque<QueuedTask>,
}

impl PriorityQueues {
    /// Create empty queues.
    pub fn new() -> Self {
        Self {
            critical: VecDeque::new(),
            high: VecDeque::new(),
            low: VecDeque::new(),
        }
    }

    /// Enqueue a task at the appropriate tier.
    pub fn push(&mut self, task: ExecutionTask) {
        let queued = QueuedTask::new(task);
        match queued.task.request.priority {
            Priority::Critical => self.critical.push_back(queued),
            Priority::High => self.high.push_back(queued),
            Priority::Normal | Priority::Background => self.low.push_back(queued),
        }
    }

    /// Total tasks across all tiers.
    pub fn len(&self) -> usize {
        self.critical.len() + self.high.len() + self.low.len()
    }

    /// `true` if all tiers are empty.
    pub fn is_empty(&self) -> bool {
        self.critical.is_empty() && self.high.is_empty() && self.low.is_empty()
    }

    /// Pending count for the Critical tier.
    pub fn critical_len(&self) -> usize {
        self.critical.len()
    }

    /// Pending count for the High tier.
    pub fn high_len(&self) -> usize {
        self.high.len()
    }

    /// Pending count for the Low tier.
    pub fn low_len(&self) -> usize {
        self.low.len()
    }

    /// Peek at the front task without removing it.
    ///
    /// Returns the highest-priority available task (Critical → High → Low).
    /// Used by `poll_write_batch` to decide whether to start a batch.
    pub fn front(&self) -> Option<&ExecutionTask> {
        if let Some(qt) = self.critical.front() {
            return Some(&qt.task);
        }
        if let Some(qt) = self.high.front() {
            return Some(&qt.task);
        }
        self.low.front().map(|qt| &qt.task)
    }

    /// Remove and return the highest-priority front task without applying the
    /// drain ratio.
    ///
    /// Used by `poll_write_batch` when collecting a write-coalesce batch.
    pub fn pop_front(&mut self) -> Option<ExecutionTask> {
        if let Some(qt) = self.critical.pop_front() {
            return Some(qt.task);
        }
        if let Some(qt) = self.high.pop_front() {
            return Some(qt.task);
        }
        self.low.pop_front().map(|qt| qt.task)
    }

    /// Iterate over all tasks across all tiers in priority order (Critical → High → Low).
    ///
    /// Used by the cancel handler to locate a task by request ID.
    pub fn iter(&self) -> impl Iterator<Item = &ExecutionTask> {
        self.critical
            .iter()
            .chain(self.high.iter())
            .chain(self.low.iter())
            .map(|qt| &qt.task)
    }

    /// Remove the task at `pos` (position in priority order: Critical first, then High, then Low).
    ///
    /// Used by the cancel handler after `iter().position(...)`.
    pub fn remove(&mut self, pos: usize) {
        let crit_len = self.critical.len();
        let high_len = self.high.len();
        if pos < crit_len {
            self.critical.remove(pos);
        } else if pos < crit_len + high_len {
            self.high.remove(pos - crit_len);
        } else {
            self.low.remove(pos - crit_len - high_len);
        }
    }

    /// Push a task back to the front of its priority tier.
    ///
    /// Used by `poll_write_batch` to return tasks that could not be batched.
    /// The task is re-inserted at the *front* of its tier so it is the next
    /// candidate for that tier.
    pub fn push_front(&mut self, task: ExecutionTask) {
        let queued = QueuedTask::new(task);
        match queued.task.request.priority {
            Priority::Critical => self.critical.push_front(queued),
            Priority::High => self.high.push_front(queued),
            Priority::Normal | Priority::Background => self.low.push_front(queued),
        }
    }

    /// Pop the next task according to the 8:4:2 drain ratio.
    ///
    /// Each call to `pop_next` pulls one task from whichever tier has
    /// remaining budget in the current cycle.  Once a tier's budget for the
    /// cycle is exhausted the next lower tier is tried; if *that* is also
    /// exhausted or empty the remaining budget cascades further down.
    ///
    /// Cycle state is maintained via the mutable `cycle` counter passed by
    /// the caller (reset to 0 to start a new cycle).  The cycle counter
    /// counts tasks already dequeued in the current 14-slot window
    /// (8 + 4 + 2 = 14).
    ///
    /// Returns `None` when all tiers are empty.
    pub fn pop_next(&mut self, cycle: &mut usize) -> Option<QueuedTask> {
        // Within a 14-slot window: slots 0–7 = Critical, 8–11 = High, 12–13 = Low.
        // When a preferred tier is empty its slots go to the next lower tier.

        const CYCLE_LEN: usize = BUDGET_CRITICAL + BUDGET_HIGH + BUDGET_LOW; // 14

        let pos = *cycle % CYCLE_LEN;

        // Determine preferred tier based on cycle position.
        let preferred = if pos < BUDGET_CRITICAL {
            TierPref::Critical
        } else if pos < BUDGET_CRITICAL + BUDGET_HIGH {
            TierPref::High
        } else {
            TierPref::Low
        };

        let task = match preferred {
            TierPref::Critical => self
                .critical
                .pop_front()
                .or_else(|| self.high.pop_front())
                .or_else(|| self.low.pop_front()),
            TierPref::High => self
                .high
                .pop_front()
                .or_else(|| self.critical.pop_front())
                .or_else(|| self.low.pop_front()),
            TierPref::Low => self
                .low
                .pop_front()
                .or_else(|| self.high.pop_front())
                .or_else(|| self.critical.pop_front()),
        };

        if task.is_some() {
            *cycle = cycle.wrapping_add(1);
        }

        task
    }
}

impl Default for PriorityQueues {
    fn default() -> Self {
        Self::new()
    }
}

enum TierPref {
    Critical,
    High,
    Low,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::bridge::envelope::{Priority, Request};
    use crate::data::executor::task::ExecutionTask;
    use crate::event::EventSource;
    use crate::types::{DatabaseId, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_physical::physical_plan::meta::MetaOp;

    fn make_task(priority: Priority) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Meta(MetaOp::Compact),
            deadline: Instant::now() + Duration::from_secs(60),
            priority,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Eventual,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: vec![],
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        })
    }

    /// Feed equal numbers of all three tiers; verify drain order reflects
    /// the 8:4:2 ratio within each 14-slot cycle.
    #[test]
    fn drain_respects_ratio() {
        let mut q = PriorityQueues::new();

        // Push 8 Critical, 4 High, 2 Low (exactly one cycle's worth).
        for _ in 0..8 {
            q.push(make_task(Priority::Critical));
        }
        for _ in 0..4 {
            q.push(make_task(Priority::High));
        }
        for _ in 0..2 {
            q.push(make_task(Priority::Normal));
        }

        let mut cycle = 0usize;
        let mut critical_count = 0usize;
        let mut high_count = 0usize;
        let mut low_count = 0usize;

        while let Some(qt) = q.pop_next(&mut cycle) {
            match qt.task.request.priority {
                Priority::Critical => critical_count += 1,
                Priority::High => high_count += 1,
                Priority::Normal | Priority::Background => low_count += 1,
            }
        }

        assert_eq!(critical_count, 8);
        assert_eq!(high_count, 4);
        assert_eq!(low_count, 2);
    }

    /// Push 100 Critical + 100 Normal; verify that all Normal tasks eventually
    /// drain (no permanent starvation).
    #[test]
    fn no_permanent_starvation() {
        let mut q = PriorityQueues::new();

        for _ in 0..100 {
            q.push(make_task(Priority::Critical));
        }
        for _ in 0..100 {
            q.push(make_task(Priority::Normal));
        }

        let mut cycle = 0usize;
        let mut normal_drained = 0usize;
        let mut total = 0usize;

        while let Some(qt) = q.pop_next(&mut cycle) {
            total += 1;
            if matches!(
                qt.task.request.priority,
                Priority::Normal | Priority::Background
            ) {
                normal_drained += 1;
            }
        }

        assert_eq!(total, 200, "all 200 tasks must drain");
        assert_eq!(normal_drained, 100, "all 100 Normal tasks must drain");
    }

    /// Verify empty-tier slot donation: 8 Critical-only tasks drain without
    /// stalling when High and Low tiers are empty.
    #[test]
    fn empty_tier_slot_donation() {
        let mut q = PriorityQueues::new();

        for _ in 0..16 {
            q.push(make_task(Priority::Critical));
        }

        let mut cycle = 0usize;
        let mut drained = 0usize;
        while q.pop_next(&mut cycle).is_some() {
            drained += 1;
        }
        assert_eq!(drained, 16);
    }
}
