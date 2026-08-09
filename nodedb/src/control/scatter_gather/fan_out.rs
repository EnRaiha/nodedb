// SPDX-License-Identifier: BUSL-1.1

//! Adaptive fan-out policy: how wide a single hop is allowed to scatter.
//!
//! Separate from the envelope that carries the destinations and from the hop
//! that dispatches them, because this is the one place a traversal is allowed
//! to be narrowed or refused. It answers a policy question — soft warn, hard
//! stop, or partial — from `GraphTraversalOptions` alone, touching no routing
//! table, no gateway, and no network, so the decision is testable in isolation
//! and cannot drift into the dispatch loop.

use crate::engine::graph::traversal_options::{GraphResponseMeta, GraphTraversalOptions};

use super::envelope::{ScatterBatch, ScatterEnvelope};

/// Result of applying adaptive fan-out limits to a scatter envelope.
#[derive(Debug)]
pub enum FanOutDecision {
    /// All batches can proceed. No limits hit.
    Proceed {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Soft limit exceeded but continuing. Response annotated with warning.
    ProceedWithWarning {
        batches: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
    /// Hard limit exceeded. If fan_out_partial, return partial results.
    /// Otherwise, return FAN_OUT_EXCEEDED error.
    Exceeded {
        /// Batches that were dispatched before limit was hit (for partial mode).
        dispatched: Vec<ScatterBatch>,
        /// Batches that were skipped.
        skipped: Vec<ScatterBatch>,
        meta: GraphResponseMeta,
    },
}

/// Apply adaptive fan-out limits to a scatter envelope.
/// - Soft limit (default 12): query continues, response annotated with warning
/// - Hard limit (default 16): query terminates with FAN_OUT_EXCEEDED unless
///   fan_out_partial is true, in which case partial results are returned
pub fn apply_fan_out_limits(
    envelope: ScatterEnvelope,
    options: &GraphTraversalOptions,
) -> FanOutDecision {
    let shard_count = envelope.shard_count() as u16;

    if shard_count <= options.fan_out_soft {
        // Under soft limit — all clear.
        FanOutDecision::Proceed {
            batches: envelope.into_batches(),
            meta: GraphResponseMeta {
                shards_reached: shard_count,
                ..Default::default()
            },
        }
    } else if shard_count <= options.fan_out_hard {
        // Between soft and hard limit — proceed with warning.
        let batches = envelope.into_batches();
        let meta = GraphResponseMeta::with_warning(shard_count, 0, options.fan_out_hard);
        FanOutDecision::ProceedWithWarning { batches, meta }
    } else {
        // Exceeded hard limit.
        let mut all_batches = envelope.into_batches();
        let hard = options.fan_out_hard as usize;
        let skipped = all_batches.split_off(hard);
        let skipped_count = skipped.len() as u16;
        let dispatched_count = all_batches.len() as u16;

        let meta = if options.fan_out_partial {
            GraphResponseMeta::with_truncation(dispatched_count, skipped_count)
        } else {
            GraphResponseMeta {
                shards_reached: dispatched_count,
                shards_skipped: skipped_count,
                truncated: true,
                fan_out_warning: None,
                approximate: true,
            }
        };

        FanOutDecision::Exceeded {
            dispatched: all_batches,
            skipped,
            meta,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::VShardId;

    #[test]
    fn fan_out_under_soft_limit() {
        let mut env = ScatterEnvelope::new();
        for i in 0..5u32 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::Proceed { batches, meta } => {
                assert_eq!(batches.len(), 5);
                assert!(meta.is_clean());
                assert_eq!(meta.shards_reached, 5);
            }
            _ => panic!("expected Proceed"),
        }
    }

    #[test]
    fn fan_out_between_soft_and_hard() {
        let mut env = ScatterEnvelope::new();
        for i in 0..14u32 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let decision = apply_fan_out_limits(env, &GraphTraversalOptions::default());
        match decision {
            FanOutDecision::ProceedWithWarning { batches, meta } => {
                assert_eq!(batches.len(), 14);
                assert!(!meta.is_clean());
                assert!(meta.approximate);
                assert_eq!(meta.fan_out_warning, Some("14/16".to_string()));
            }
            _ => panic!("expected ProceedWithWarning"),
        }
    }

    #[test]
    fn fan_out_exceeded_no_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u32 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: false,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                meta,
            } => {
                assert_eq!(dispatched.len(), 16);
                assert_eq!(skipped.len(), 4);
                assert!(meta.truncated);
                assert_eq!(meta.shards_reached, 16);
                assert_eq!(meta.shards_skipped, 4);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn fan_out_exceeded_with_partial() {
        let mut env = ScatterEnvelope::new();
        for i in 0..20u32 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_partial: true,
            ..Default::default()
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched, meta, ..
            } => {
                assert_eq!(dispatched.len(), 16);
                assert!(meta.truncated);
            }
            _ => panic!("expected Exceeded"),
        }
    }

    #[test]
    fn custom_limits() {
        let mut env = ScatterEnvelope::new();
        for i in 0..10u32 {
            env.add(VShardId::new(i), format!("node_{i}"));
        }

        let opts = GraphTraversalOptions {
            fan_out_soft: 4,
            fan_out_hard: 8,
            fan_out_partial: true,
            max_visited: 100_000,
        };
        let decision = apply_fan_out_limits(env, &opts);
        match decision {
            FanOutDecision::Exceeded {
                dispatched,
                skipped,
                ..
            } => {
                assert_eq!(dispatched.len(), 8);
                assert_eq!(skipped.len(), 2);
            }
            _ => panic!("expected Exceeded"),
        }
    }
}
