// SPDX-License-Identifier: Apache-2.0

//! Fixed-capacity min-heap for top-k scoring in BMW.
//!
//! Maintains the k best `(score, surrogate)` pairs. The threshold (minimum
//! score to enter the heap) is the root's score when full, 0.0 when filling.
//!
//! Rank order is score descending, then surrogate ascending. The kept set
//! is the first k documents in that order, so a cut at k is a prefix of the
//! uncut result, ties included.

use std::cmp::Ordering;

use nodedb_types::Surrogate;

/// A scored candidate in the top-k heap.
#[derive(Debug, Clone, Copy)]
pub struct ScoredDoc {
    pub score: f32,
    pub doc_id: Surrogate,
}

/// `Greater` when `a` ranks above `b`: a higher score, or an equal score and
/// a lower surrogate.
fn rank_cmp(a: &ScoredDoc, b: &ScoredDoc) -> Ordering {
    a.score
        .total_cmp(&b.score)
        .then_with(|| b.doc_id.cmp(&a.doc_id))
}

/// Fixed-capacity min-heap: the lowest-ranked candidate is at the root.
///
/// When full, only a candidate that ranks above the root is admitted (the
/// root is replaced and the heap is sifted down).
pub struct TopKHeap {
    data: Vec<ScoredDoc>,
    capacity: usize,
}

/// Initial heap allocation. A larger `k` grows on demand; `usize::MAX` means every match.
const INITIAL_HEAP_CAPACITY: usize = 1024;

impl TopKHeap {
    /// Create a new heap that keeps the best `k` candidates.
    pub fn new(k: usize) -> Self {
        Self {
            data: Vec::with_capacity(k.min(INITIAL_HEAP_CAPACITY)),
            capacity: k,
        }
    }

    /// Current threshold: score must exceed this to be admitted.
    /// Returns 0.0 while the heap is not yet full.
    pub fn threshold(&self) -> f32 {
        if self.data.len() < self.capacity {
            0.0
        } else {
            self.data[0].score
        }
    }

    /// Try to insert a scored document.
    ///
    /// If the heap is not full, always inserts. If full, inserts only when
    /// the candidate ranks above the root, replacing the root.
    pub fn insert(&mut self, score: f32, doc_id: Surrogate) {
        let candidate = ScoredDoc { score, doc_id };
        if self.data.len() < self.capacity {
            self.data.push(candidate);
            if self.data.len() == self.capacity {
                // Build the min-heap once full.
                self.build_heap();
            }
        } else if rank_cmp(&candidate, &self.data[0]) == Ordering::Greater {
            self.data[0] = candidate;
            self.sift_down(0);
        }
    }

    /// Drain the heap into a vec in rank order: score descending, then
    /// surrogate ascending.
    pub fn into_sorted(mut self) -> Vec<ScoredDoc> {
        self.data.sort_by(|a, b| rank_cmp(b, a));
        self.data
    }

    /// Number of entries currently in the heap.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the heap is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn build_heap(&mut self) {
        let n = self.data.len();
        for i in (0..n / 2).rev() {
            self.sift_down(i);
        }
    }

    fn sift_down(&mut self, mut pos: usize) {
        let n = self.data.len();
        loop {
            let left = 2 * pos + 1;
            let right = 2 * pos + 2;
            let mut smallest = pos;

            if left < n && rank_cmp(&self.data[left], &self.data[smallest]) == Ordering::Less {
                smallest = left;
            }
            if right < n && rank_cmp(&self.data[right], &self.data[smallest]) == Ordering::Less {
                smallest = right;
            }

            if smallest == pos {
                break;
            }
            self.data.swap(pos, smallest);
            pos = smallest;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_top_k() {
        let mut heap = TopKHeap::new(3);
        heap.insert(1.0, Surrogate(1));
        heap.insert(5.0, Surrogate(5));
        heap.insert(3.0, Surrogate(3));
        heap.insert(2.0, Surrogate(2));
        heap.insert(4.0, Surrogate(4));

        let sorted = heap.into_sorted();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].doc_id, Surrogate(5));
        assert_eq!(sorted[1].doc_id, Surrogate(4));
        assert_eq!(sorted[2].doc_id, Surrogate(3));
    }

    #[test]
    fn threshold_while_filling() {
        let mut heap = TopKHeap::new(3);
        assert_eq!(heap.threshold(), 0.0);
        heap.insert(5.0, Surrogate(1));
        assert_eq!(heap.threshold(), 0.0); // Not full yet.
        heap.insert(3.0, Surrogate(2));
        assert_eq!(heap.threshold(), 0.0);
        heap.insert(1.0, Surrogate(3));
        // Now full — threshold is the minimum.
        assert!((heap.threshold() - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn rejects_below_threshold() {
        let mut heap = TopKHeap::new(2);
        heap.insert(5.0, Surrogate(1));
        heap.insert(3.0, Surrogate(2));
        heap.insert(1.0, Surrogate(99)); // Below threshold (3.0) — rejected.

        let sorted = heap.into_sorted();
        assert_eq!(sorted.len(), 2);
        assert!(sorted.iter().all(|d| d.doc_id != Surrogate(99)));
    }

    #[test]
    fn empty_heap() {
        let heap = TopKHeap::new(5);
        assert!(heap.is_empty());
        assert_eq!(heap.threshold(), 0.0);
    }

    #[test]
    fn unbounded_k_allocates_at_most_the_initial_capacity() {
        let mut heap = TopKHeap::new(usize::MAX);
        assert!(heap.data.capacity() <= INITIAL_HEAP_CAPACITY);
        for i in 1..=3000u32 {
            heap.insert(i as f32, Surrogate(i));
        }
        assert_eq!(heap.len(), 3000, "an unbounded heap keeps every match");
        assert_eq!(heap.into_sorted()[0].doc_id, Surrogate(3000));
    }

    /// A full heap of tied scores evicts the tie with the highest surrogate,
    /// so the kept set is the first k rows of the uncut order.
    #[test]
    fn eviction_among_ties_keeps_the_lowest_surrogates() {
        let mut heap = TopKHeap::new(2);
        heap.insert(1.0, Surrogate(1));
        heap.insert(1.0, Surrogate(2));
        heap.insert(2.0, Surrogate(3));
        let kept: Vec<Surrogate> = heap.into_sorted().iter().map(|d| d.doc_id).collect();
        assert_eq!(kept, vec![Surrogate(3), Surrogate(1)]);

        let mut heap = TopKHeap::new(3);
        for id in [7, 4, 9, 2] {
            heap.insert(1.0, Surrogate(id));
        }
        let kept: Vec<Surrogate> = heap.into_sorted().iter().map(|d| d.doc_id).collect();
        assert_eq!(kept, vec![Surrogate(2), Surrogate(4), Surrogate(7)]);
    }

    #[test]
    fn single_element() {
        let mut heap = TopKHeap::new(1);
        heap.insert(3.0, Surrogate(1));
        heap.insert(5.0, Surrogate(2));
        heap.insert(1.0, Surrogate(3));

        let sorted = heap.into_sorted();
        assert_eq!(sorted.len(), 1);
        assert_eq!(sorted[0].doc_id, Surrogate(2));
    }
}
