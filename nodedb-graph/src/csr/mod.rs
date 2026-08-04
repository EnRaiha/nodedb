// SPDX-License-Identifier: Apache-2.0

pub mod bulk;
mod compaction;
pub(crate) mod dense_array;
pub mod index;
pub mod local_node_id;
pub mod memory;
pub mod persist;
pub mod slice_accessors;
pub mod statistics;
pub mod weights;

pub use bulk::CsrBulkBuilder;
pub use index::{CsrIndex, Direction};
pub use local_node_id::LocalNodeId;
pub use statistics::{DegreeHistogram, GraphStatistics, LabelStats};
pub use weights::extract_weight_from_properties;
