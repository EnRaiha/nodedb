// SPDX-License-Identifier: BUSL-1.1

//! BSP coordinator for distributed graph algorithms.
//!
//! Runs on the Control Plane. Tracks which shards have completed each
//! superstep and aggregates convergence metrics.

use std::collections::HashMap;

use super::barrier::{BspBarrierError, SuperstepTotals};
use super::types::{AlgoComplete, SuperstepAck, SuperstepBarrier};

#[derive(Debug)]
pub struct BspCoordinator {
    pub algorithm: String,
    pub iteration: u32,
    pub max_iterations: u32,
    pub tolerance: f64,
    pub shard_ids: Vec<u32>,
    pub acks: HashMap<u32, SuperstepAck>,
    pub completed: bool,
    /// Bitemporal system-time ordinal for this run. Stamped onto every
    /// `SuperstepBarrier` so all shards materialize the same historical
    /// topology. `None` means current state.
    pub system_as_of: Option<i64>,
}

impl BspCoordinator {
    pub fn new(
        algorithm: String,
        max_iterations: u32,
        tolerance: f64,
        shard_ids: Vec<u32>,
    ) -> Self {
        Self::new_as_of(algorithm, max_iterations, tolerance, shard_ids, None)
    }

    pub fn new_as_of(
        algorithm: String,
        max_iterations: u32,
        tolerance: f64,
        shard_ids: Vec<u32>,
        system_as_of: Option<i64>,
    ) -> Self {
        Self {
            algorithm,
            iteration: 0,
            max_iterations,
            tolerance,
            shard_ids,
            acks: HashMap::new(),
            completed: false,
            system_as_of,
        }
    }

    pub fn record_ack(&mut self, ack: SuperstepAck) {
        self.acks.insert(ack.shard_id, ack);
    }

    pub fn all_acked(&self) -> bool {
        self.shard_ids.iter().all(|id| self.acks.contains_key(id))
    }

    /// Cluster-wide delta and vertex count for the current superstep.
    ///
    /// Refuses while any shard is still missing: a partial sum is a wrong
    /// aggregate that reads as a correct one, so it must never leave here as a
    /// plain number.
    pub fn totals(&self) -> Result<SuperstepTotals, BspBarrierError> {
        if !self.all_acked() {
            return Err(BspBarrierError::Incomplete {
                algorithm: self.algorithm.clone(),
                iteration: self.iteration,
                acked: self
                    .shard_ids
                    .iter()
                    .filter(|id| self.acks.contains_key(id))
                    .count(),
                expected: self.shard_ids.len(),
            });
        }
        Ok(SuperstepTotals::new(
            self.acks.values().map(|ack| ack.local_delta).sum(),
            self.acks.values().map(|ack| ack.vertex_count).sum(),
        ))
    }

    /// Advance to next superstep. Returns `true` if should continue.
    pub fn advance(&mut self) -> Result<bool, BspBarrierError> {
        let delta = self.totals()?.global_delta();
        self.iteration += 1;
        self.acks.clear();

        if delta < self.tolerance || self.iteration >= self.max_iterations {
            self.completed = true;
            return Ok(false);
        }
        Ok(true)
    }

    pub fn barrier_message(&self) -> SuperstepBarrier {
        SuperstepBarrier {
            algorithm: self.algorithm.clone(),
            iteration: self.iteration + 1,
            max_iterations: self.max_iterations,
            params: String::new(),
            system_as_of: self.system_as_of,
        }
    }

    pub fn completion_message(&self) -> Result<AlgoComplete, BspBarrierError> {
        let delta = self.totals()?.global_delta();
        Ok(AlgoComplete {
            iterations: self.iteration,
            converged: delta < self.tolerance,
            final_delta: delta,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(shard_id: u32, iteration: u32, local_delta: f64, vertex_count: usize) -> SuperstepAck {
        SuperstepAck {
            shard_id,
            iteration,
            local_delta,
            vertex_count,
            contributions_sent: 10,
        }
    }

    #[test]
    fn coordinator_convergence() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1, 2]);

        for id in 0..3u32 {
            coord.record_ack(ack(id, 1, 0.3, 100));
        }
        assert!(coord.all_acked());
        let totals = coord.totals().expect("all shards acked");
        assert!((totals.global_delta() - 0.9).abs() < 1e-10);
        assert!(coord.advance().expect("all shards acked"));

        for id in 0..3u32 {
            coord.record_ack(ack(id, 2, 1e-8, 100));
        }
        assert!(!coord.advance().expect("all shards acked"));
        assert!(coord.completed);
    }

    #[test]
    fn coordinator_max_iterations() {
        let mut coord = BspCoordinator::new("pagerank".into(), 2, 1e-10, vec![0]);

        coord.record_ack(ack(0, 1, 1.0, 10));
        assert!(coord.advance().expect("all shards acked"));

        coord.record_ack(ack(0, 2, 0.5, 10));
        assert!(!coord.advance().expect("all shards acked"));
        assert!(coord.completed);
    }

    #[test]
    fn totals_sum_every_shard_ack() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1, 2]);
        coord.record_ack(ack(0, 1, 0.25, 40));
        coord.record_ack(ack(1, 1, 0.5, 50));
        coord.record_ack(ack(2, 1, 0.125, 60));

        let totals = coord.totals().expect("all shards acked");
        assert!((totals.global_delta() - 0.875).abs() < 1e-12);
        assert_eq!(totals.total_vertices(), 150);
    }

    #[test]
    fn totals_refused_while_shards_missing() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1, 2]);
        coord.record_ack(ack(0, 1, 0.25, 40));
        coord.record_ack(ack(1, 1, 0.5, 50));

        assert_eq!(
            coord.totals().unwrap_err(),
            BspBarrierError::Incomplete {
                algorithm: "pagerank".into(),
                iteration: 0,
                acked: 2,
                expected: 3,
            }
        );
    }

    #[test]
    fn advance_refused_while_shards_missing() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1]);
        coord.record_ack(ack(0, 1, 0.25, 40));

        assert!(coord.advance().is_err());
        // The refused advance must not consume the superstep.
        assert_eq!(coord.iteration, 0);
        assert!(!coord.completed);
        assert!(coord.acks.contains_key(&0));
    }

    #[test]
    fn completion_message_refused_while_shards_missing() {
        let mut coord = BspCoordinator::new("pagerank".into(), 20, 1e-6, vec![0, 1]);
        coord.record_ack(ack(0, 1, 1e-9, 40));

        assert!(coord.completion_message().is_err());

        coord.record_ack(ack(1, 1, 1e-9, 40));
        let complete = coord.completion_message().expect("all shards acked");
        assert!(complete.converged);
        assert!((complete.final_delta - 2e-9).abs() < 1e-18);
    }
}
