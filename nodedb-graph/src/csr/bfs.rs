// SPDX-License-Identifier: Apache-2.0

//! Breadth-first traversal over the undirected projection.

use std::collections::VecDeque;

use super::CsrIndex;

#[cfg(not(target_arch = "wasm32"))]
static ACTIVE_BFS_WORKERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(not(target_arch = "wasm32"))]
struct BfsWorkerPermits(usize);

#[cfg(not(target_arch = "wasm32"))]
impl BfsWorkerPermits {
    fn reserve(requested: usize) -> Self {
        use std::sync::atomic::Ordering;

        const MAX_PROCESS_WORKERS: usize = 32;
        let mut active = ACTIVE_BFS_WORKERS.load(Ordering::Relaxed);
        loop {
            let granted = requested.min(MAX_PROCESS_WORKERS.saturating_sub(active));
            match ACTIVE_BFS_WORKERS.compare_exchange_weak(
                active,
                active + granted,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Self(granted),
                Err(updated) => active = updated,
            }
        }
    }

    fn workers(&self) -> usize {
        self.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for BfsWorkerPermits {
    fn drop(&mut self) {
        use std::sync::atomic::Ordering;

        ACTIVE_BFS_WORKERS.fetch_sub(self.0, Ordering::AcqRel);
    }
}

impl CsrIndex {
    /// Compute unweighted distances over outbound plus inbound adjacency.
    ///
    /// Compacted native graphs use a bounded parallel frontier traversal;
    /// mutable/deleted graphs and WASM retain the iterator-based sequential
    /// path. Unreachable nodes are `-1`.
    pub fn bfs_both_distances_raw(&self, source: u32) -> Vec<i64> {
        let node_count = self.node_count();
        assert!((source as usize) < node_count, "BFS source is out of range");
        let compacted = self
            .compacted_out_adjacency_raw()
            .zip(self.compacted_in_adjacency_raw());
        if let Some(((out_offsets, out_targets), (in_offsets, in_targets))) = compacted {
            return compacted_bfs(
                source,
                node_count,
                out_offsets,
                out_targets,
                in_offsets,
                in_targets,
            );
        }

        let mut distances = vec![-1; node_count];
        distances[source as usize] = 0;
        let mut queue = VecDeque::from([source]);
        while let Some(node) = queue.pop_front() {
            let next_distance = distances[node as usize] + 1;
            for (_label, neighbor) in self
                .iter_out_edges_raw(node)
                .chain(self.iter_in_edges_raw(node))
            {
                if distances[neighbor as usize] == -1 {
                    distances[neighbor as usize] = next_distance;
                    queue.push_back(neighbor);
                }
            }
        }
        distances
    }
}

#[cfg(target_arch = "wasm32")]
fn compacted_bfs(
    source: u32,
    node_count: usize,
    out_offsets: &[u32],
    out_targets: &[u32],
    in_offsets: &[u32],
    in_targets: &[u32],
) -> Vec<i64> {
    compacted_bfs_sequential(
        source,
        node_count,
        out_offsets,
        out_targets,
        in_offsets,
        in_targets,
    )
}

#[cfg(not(target_arch = "wasm32"))]
fn compacted_bfs(
    source: u32,
    node_count: usize,
    out_offsets: &[u32],
    out_targets: &[u32],
    in_offsets: &[u32],
    in_targets: &[u32],
) -> Vec<i64> {
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    const MAX_WORKERS: usize = 32;
    const PARALLEL_FRONTIER: usize = 1024;
    const CHUNK_SIZE: usize = 64;

    let desired_workers = std::thread::available_parallelism()
        .map_or(1, usize::from)
        .min(MAX_WORKERS);
    let permits = BfsWorkerPermits::reserve(desired_workers);
    let workers = permits.workers();
    if workers <= 1 {
        return compacted_bfs_sequential(
            source,
            node_count,
            out_offsets,
            out_targets,
            in_offsets,
            in_targets,
        );
    }

    let distances: Vec<AtomicI64> = (0..node_count).map(|_| AtomicI64::new(-1)).collect();
    distances[source as usize].store(0, Ordering::Relaxed);
    let mut frontier = vec![source];
    let mut depth = 0i64;
    while !frontier.is_empty() {
        let next_depth = depth + 1;
        if frontier.len() < PARALLEL_FRONTIER {
            let mut next = Vec::new();
            for &node in &frontier {
                visit_neighbors(
                    node,
                    next_depth,
                    out_offsets,
                    out_targets,
                    in_offsets,
                    in_targets,
                    &distances,
                    &mut next,
                );
            }
            frontier = next;
        } else {
            let next_chunk = AtomicUsize::new(0);
            frontier = std::thread::scope(|scope| {
                let handles: Vec<_> = (0..workers.min(frontier.len()))
                    .map(|_| {
                        let next_chunk = &next_chunk;
                        let frontier = &frontier;
                        let distances = &distances;
                        scope.spawn(move || {
                            let mut local = Vec::new();
                            loop {
                                let start = next_chunk.fetch_add(CHUNK_SIZE, Ordering::Relaxed);
                                if start >= frontier.len() {
                                    break;
                                }
                                let end = (start + CHUNK_SIZE).min(frontier.len());
                                for &node in &frontier[start..end] {
                                    visit_neighbors(
                                        node,
                                        next_depth,
                                        out_offsets,
                                        out_targets,
                                        in_offsets,
                                        in_targets,
                                        distances,
                                        &mut local,
                                    );
                                }
                            }
                            local
                        })
                    })
                    .collect();
                let mut next = Vec::new();
                for handle in handles {
                    next.extend(handle.join().expect("BFS worker panicked"));
                }
                next
            });
        }
        depth = next_depth;
    }
    distances
        .into_iter()
        .map(|distance| distance.into_inner())
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(clippy::too_many_arguments)]
fn visit_neighbors(
    node: u32,
    distance: i64,
    out_offsets: &[u32],
    out_targets: &[u32],
    in_offsets: &[u32],
    in_targets: &[u32],
    distances: &[std::sync::atomic::AtomicI64],
    next: &mut Vec<u32>,
) {
    use std::sync::atomic::Ordering;

    let node = node as usize;
    let outbound = &out_targets[out_offsets[node] as usize..out_offsets[node + 1] as usize];
    let inbound = &in_targets[in_offsets[node] as usize..in_offsets[node + 1] as usize];
    for &neighbor in outbound.iter().chain(inbound) {
        if distances[neighbor as usize]
            .compare_exchange(-1, distance, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            next.push(neighbor);
        }
    }
}

fn compacted_bfs_sequential(
    source: u32,
    node_count: usize,
    out_offsets: &[u32],
    out_targets: &[u32],
    in_offsets: &[u32],
    in_targets: &[u32],
) -> Vec<i64> {
    let mut distances = vec![-1; node_count];
    distances[source as usize] = 0;
    let mut queue = VecDeque::from([source]);
    while let Some(node) = queue.pop_front() {
        let index = node as usize;
        let next_distance = distances[index] + 1;
        let outbound = &out_targets[out_offsets[index] as usize..out_offsets[index + 1] as usize];
        let inbound = &in_targets[in_offsets[index] as usize..in_offsets[index + 1] as usize];
        for &neighbor in outbound.iter().chain(inbound) {
            if distances[neighbor as usize] == -1 {
                distances[neighbor as usize] = next_distance;
                queue.push_back(neighbor);
            }
        }
    }
    distances
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compacted_and_buffered_bfs_match_on_asymmetric_graph() {
        let mut buffered = CsrIndex::new();
        buffered.add_edge("a", "L", "b").unwrap();
        buffered.add_edge("c", "L", "b").unwrap();
        buffered.add_edge("c", "L", "d").unwrap();
        buffered.add_edge("a", "self", "a").unwrap();
        buffered.add_node("isolated").unwrap();
        let source = buffered.node_id_raw("a").unwrap();
        let expected = buffered.bfs_both_distances_raw(source);

        buffered.compact().unwrap();
        assert_eq!(buffered.bfs_both_distances_raw(source), expected);
        assert_eq!(expected, vec![0, 1, 2, 3, -1]);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn exhausted_worker_permits_fall_back_without_truncating_bfs() {
        let mut csr = CsrIndex::new();
        csr.add_edge("a", "L", "b").unwrap();
        csr.add_edge("b", "L", "c").unwrap();
        csr.compact().unwrap();
        let _held = BfsWorkerPermits::reserve(32);
        let distances = csr.bfs_both_distances_raw(csr.node_id_raw("a").unwrap());
        assert_eq!(distances, vec![0, 1, 2]);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn parallel_frontier_assigns_each_node_at_its_shortest_depth() {
        let mut csr = CsrIndex::new();
        for node in 0..2048 {
            csr.add_edge("root", "L", &format!("middle-{node}"))
                .unwrap();
            csr.add_edge(&format!("middle-{node}"), "L", &format!("leaf-{node}"))
                .unwrap();
        }
        csr.compact().unwrap();
        let distances = csr.bfs_both_distances_raw(csr.node_id_raw("root").unwrap());
        for node in 0..2048 {
            assert_eq!(
                distances[csr.node_id_raw(&format!("middle-{node}")).unwrap() as usize],
                1
            );
            assert_eq!(
                distances[csr.node_id_raw(&format!("leaf-{node}")).unwrap() as usize],
                2
            );
        }
    }
}
