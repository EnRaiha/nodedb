// SPDX-License-Identifier: Apache-2.0

//! Dead-letter queue for rejected CRDT deltas.
//!
//! When a delta fails constraint validation at commit time, it's not silently
//! dropped — it's routed to a dead-letter queue with actionable compensation
//! hints that tell the application exactly how to recover.
//!
//! ## Developer Experience
//!
//! The DX of handling constraint rejections must be pristine. If developers
//! have to write massive amounts of complex undo/compensation logic, they
//! will abandon the database. The DLQ provides:
//!
//! 1. The original delta that was rejected.
//! 2. Which constraint was violated and why.
//! 3. A machine-readable `CompensationHint` suggesting how to fix it.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::constraint::Constraint;
use crate::error::{CrdtError, Result};

/// Suggested action the application should take to resolve a constraint violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompensationHint {
    /// Retry with a different value for the conflicting field.
    /// Example: UNIQUE violation — suggest appending a suffix.
    RetryWithDifferentValue {
        field: String,
        conflicting_value: String,
        suggestion: String,
    },

    /// Delete the conflicting row first, then retry.
    /// Example: UNIQUE violation where the application wants to "upsert".
    DeleteThenRetry {
        collection: String,
        conflicting_key: String,
    },

    /// The referenced row doesn't exist — create it first.
    /// Example: FK violation — the parent row is missing.
    CreateReferencedRow {
        ref_collection: String,
        ref_key: String,
        missing_value: String,
    },

    /// The field must not be empty — provide a value.
    ProvideRequiredField { field: String },

    /// No automatic compensation available — manual intervention required.
    ManualIntervention { reason: String },
}

/// A rejected delta with metadata for debugging and recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter {
    /// Unique ID for this dead letter entry.
    pub id: u64,

    /// The peer that produced this delta.
    pub peer_id: u64,

    /// The authenticated user_id that submitted this delta (0 = unauthenticated/legacy).
    #[serde(default)]
    pub user_id: u64,

    /// The tenant this delta belongs to (0 = system).
    #[serde(default)]
    pub tenant_id: u64,

    /// The raw delta bytes that were rejected.
    pub delta: Vec<u8>,

    /// The constraint that was violated.
    pub violated_constraint: String,

    /// The collection where the violation occurred.
    pub collection: String,

    /// Human-readable explanation of why the delta was rejected.
    pub reason: String,

    /// Machine-readable hint for how to fix the violation.
    pub hint: CompensationHint,

    /// Timestamp when the rejection occurred (unix millis).
    pub rejected_at: u64,

    /// Number of times this delta has been retried.
    pub retry_count: u32,
}

/// Bounded dead-letter queue.
///
/// Stores rejected deltas for later inspection and retry. The queue is bounded
/// to prevent unbounded memory growth from a flood of invalid deltas.
pub struct DeadLetterQueue {
    entries: VecDeque<DeadLetter>,
    capacity: usize,
    next_id: u64,
}

impl DeadLetterQueue {
    /// Create a new DLQ with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity.min(1024)),
            capacity,
            next_id: 1,
        }
    }

    /// Enqueue a rejected delta with full auth context.
    #[allow(clippy::too_many_arguments)]
    pub fn enqueue(
        &mut self,
        peer_id: u64,
        user_id: u64,
        tenant_id: u64,
        delta: Vec<u8>,
        constraint: &Constraint,
        reason: String,
        hint: CompensationHint,
    ) -> Result<u64> {
        if self.entries.len() >= self.capacity {
            return Err(CrdtError::DlqFull {
                capacity: self.capacity,
                pending: self.entries.len(),
            });
        }

        let id = self.next_id;
        self.next_id += 1;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.entries.push_back(DeadLetter {
            id,
            peer_id,
            user_id,
            tenant_id,
            delta,
            violated_constraint: constraint.name.clone(),
            collection: constraint.collection.clone(),
            reason,
            hint,
            rejected_at: now,
            retry_count: 0,
        });

        Ok(id)
    }

    /// Peek at the oldest dead letter without removing it.
    pub fn peek(&self) -> Option<&DeadLetter> {
        self.entries.front()
    }

    /// Dequeue the oldest dead letter for retry or inspection.
    pub fn dequeue(&mut self) -> Option<DeadLetter> {
        self.entries.pop_front()
    }

    /// Get a dead letter by ID.
    pub fn get(&self, id: u64) -> Option<&DeadLetter> {
        self.entries.iter().find(|dl| dl.id == id)
    }

    /// Remove a dead letter by ID (e.g., after successful manual resolution).
    pub fn remove(&mut self, id: u64) -> Option<DeadLetter> {
        if let Some(pos) = self.entries.iter().position(|dl| dl.id == id) {
            self.entries.remove(pos)
        } else {
            None
        }
    }

    /// Drop every entry for `(tenant_id, collection)`. Returns the
    /// number of entries removed. Called during collection hard-delete
    /// so stale rejected deltas don't resurface if a collection of the
    /// same name is recreated inside the retention window's shadow.
    pub fn purge_collection(&mut self, tenant_id: u64, collection: &str) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|e| !(e.tenant_id == tenant_id && e.collection == collection));
        before - self.entries.len()
    }

    /// Drain all entries for a specific peer.
    pub fn drain_peer(&mut self, peer_id: u64) -> Vec<DeadLetter> {
        let mut drained = Vec::new();
        self.entries.retain(|dl| {
            if dl.peer_id == peer_id {
                drained.push(dl.clone());
                false
            } else {
                true
            }
        });
        drained
    }

    /// Number of pending dead letters.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constraint::{Constraint, ConstraintKind};

    fn test_constraint() -> Constraint {
        Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: ConstraintKind::Unique,
        }
    }

    #[test]
    fn enqueue_and_dequeue() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();

        let id = dlq
            .enqueue(
                42,
                0,
                0,
                b"delta-bytes".to_vec(),
                &c,
                "email already exists".into(),
                CompensationHint::RetryWithDifferentValue {
                    field: "email".into(),
                    conflicting_value: "alice@example.com".into(),
                    suggestion: "alice+1@example.com".into(),
                },
            )
            .unwrap();

        assert_eq!(dlq.len(), 1);
        assert_eq!(id, 1);

        let dl = dlq.dequeue().unwrap();
        assert_eq!(dl.peer_id, 42);
        assert_eq!(dl.violated_constraint, "users_email_unique");
        assert!(dlq.is_empty());
    }

    #[test]
    fn capacity_enforced() {
        let mut dlq = DeadLetterQueue::new(2);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        dlq.enqueue(1, 0, 0, vec![], &c, "r1".into(), hint.clone())
            .unwrap();
        dlq.enqueue(2, 0, 0, vec![], &c, "r2".into(), hint.clone())
            .unwrap();

        let err = dlq.enqueue(3, 0, 0, vec![], &c, "r3".into(), hint);
        assert!(matches!(err, Err(CrdtError::DlqFull { .. })));
    }

    #[test]
    fn purge_collection_drops_only_matching_entries() {
        let mut dlq = DeadLetterQueue::new(10);
        let users_c = Constraint {
            name: "users_email_unique".into(),
            collection: "users".into(),
            field: "email".into(),
            kind: ConstraintKind::Unique,
        };
        let orders_c = Constraint {
            name: "orders_sku_unique".into(),
            collection: "orders".into(),
            field: "sku".into(),
            kind: ConstraintKind::Unique,
        };
        let hint = CompensationHint::ManualIntervention { reason: "t".into() };

        // Two entries for tenant 1 / users, one for tenant 1 / orders,
        // one for tenant 2 / users — only the first two should be
        // dropped by `purge_collection(1, "users")`.
        dlq.enqueue(10, 0, 1, vec![], &users_c, "a".into(), hint.clone())
            .unwrap();
        dlq.enqueue(11, 0, 1, vec![], &users_c, "b".into(), hint.clone())
            .unwrap();
        dlq.enqueue(12, 0, 1, vec![], &orders_c, "c".into(), hint.clone())
            .unwrap();
        dlq.enqueue(13, 0, 2, vec![], &users_c, "d".into(), hint.clone())
            .unwrap();

        let removed = dlq.purge_collection(1, "users");
        assert_eq!(removed, 2);
        assert_eq!(dlq.len(), 2);

        // Idempotent — repeated call is a no-op.
        assert_eq!(dlq.purge_collection(1, "users"), 0);

        // Remaining entries are the orders row (t1) and users row (t2).
        let remaining: Vec<_> = (0..dlq.len()).map(|_| dlq.dequeue().unwrap()).collect();
        assert!(
            remaining
                .iter()
                .any(|d| d.collection == "orders" && d.tenant_id == 1)
        );
        assert!(
            remaining
                .iter()
                .any(|d| d.collection == "users" && d.tenant_id == 2)
        );
    }

    #[test]
    fn drain_by_peer() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        dlq.enqueue(1, 0, 0, vec![], &c, "a".into(), hint.clone())
            .unwrap();
        dlq.enqueue(2, 0, 0, vec![], &c, "b".into(), hint.clone())
            .unwrap();
        dlq.enqueue(1, 0, 0, vec![], &c, "c".into(), hint).unwrap();

        let peer1 = dlq.drain_peer(1);
        assert_eq!(peer1.len(), 2);
        assert_eq!(dlq.len(), 1);
    }

    #[test]
    fn remove_by_id() {
        let mut dlq = DeadLetterQueue::new(10);
        let c = test_constraint();
        let hint = CompensationHint::ManualIntervention {
            reason: "test".into(),
        };

        let id1 = dlq
            .enqueue(1, 0, 0, vec![], &c, "a".into(), hint.clone())
            .unwrap();
        let _id2 = dlq.enqueue(1, 0, 0, vec![], &c, "b".into(), hint).unwrap();

        let removed = dlq.remove(id1).unwrap();
        assert_eq!(removed.reason, "a");
        assert_eq!(dlq.len(), 1);
    }
}
