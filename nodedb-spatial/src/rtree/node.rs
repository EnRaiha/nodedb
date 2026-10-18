// SPDX-License-Identifier: Apache-2.0

//! R-tree node types and constants.

use nodedb_types::{BboxSpace, BoundingBox};
use serde::{Deserialize, Serialize};
use zerompk::{FromMessagePack, ToMessagePack};

pub const LEAF_CAPACITY: usize = 32;
pub const INTERNAL_CAPACITY: usize = 16;
pub const MIN_FILL_LEAF: usize = LEAF_CAPACITY * 2 / 5;
pub const MIN_FILL_INTERNAL: usize = INTERNAL_CAPACITY * 2 / 5;
pub const REINSERT_COUNT_LEAF: usize = LEAF_CAPACITY * 3 / 10;

/// Unique identifier for a spatial entry (document RID or opaque u64).
pub type EntryId = u64;

/// A spatial entry stored in an R-tree leaf.
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    ToMessagePack,
    FromMessagePack,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct RTreeEntry {
    pub id: EntryId,
    pub bbox: BoundingBox,
}

/// Internal node child reference.
#[derive(Debug, Clone)]
pub(crate) struct ChildRef {
    pub bbox: BoundingBox,
    pub node_idx: usize,
}

/// A node in the R-tree (either internal or leaf).
#[derive(Debug, Clone)]
pub(crate) enum NodeKind {
    Internal { children: Vec<ChildRef> },
    Leaf { entries: Vec<RTreeEntry> },
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub kind: NodeKind,
    pub bbox: BoundingBox,
    pub level: u32,
}

impl Node {
    pub fn new_leaf() -> Self {
        Self {
            kind: NodeKind::Leaf {
                entries: Vec::with_capacity(LEAF_CAPACITY),
            },
            bbox: BoundingBox::new(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ),
            level: 0,
        }
    }

    pub fn new_internal(level: u32) -> Self {
        Self {
            kind: NodeKind::Internal {
                children: Vec::with_capacity(INTERNAL_CAPACITY),
            },
            bbox: BoundingBox::new(
                f64::INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::NEG_INFINITY,
            ),
            level,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self.kind, NodeKind::Leaf { .. })
    }

    pub fn capacity(&self) -> usize {
        if self.is_leaf() {
            LEAF_CAPACITY
        } else {
            INTERNAL_CAPACITY
        }
    }

    pub fn len(&self) -> usize {
        match &self.kind {
            NodeKind::Internal { children } => children.len(),
            NodeKind::Leaf { entries } => entries.len(),
        }
    }

    pub fn is_overflow(&self) -> bool {
        self.len() > self.capacity()
    }

    pub fn min_fill(&self) -> usize {
        if self.is_leaf() {
            MIN_FILL_LEAF
        } else {
            MIN_FILL_INTERNAL
        }
    }

    pub fn is_underflow(&self) -> bool {
        self.len() < self.min_fill()
    }

    /// Recompute this node's bbox from its children, unioned in `space`.
    pub fn recompute_bbox(&mut self, space: BboxSpace) {
        match &self.kind {
            NodeKind::Internal { children } => {
                if children.is_empty() {
                    self.bbox = BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                    return;
                }
                let mut bb = children[0].bbox;
                for c in &children[1..] {
                    bb = bb.union_in(&c.bbox, space);
                }
                self.bbox = bb;
            }
            NodeKind::Leaf { entries } => {
                if entries.is_empty() {
                    self.bbox = BoundingBox::new(0.0, 0.0, 0.0, 0.0);
                    return;
                }
                let mut bb = entries[0].bbox;
                for e in &entries[1..] {
                    bb = bb.union_in(&e.bbox, space);
                }
                self.bbox = bb;
            }
        }
    }
}
