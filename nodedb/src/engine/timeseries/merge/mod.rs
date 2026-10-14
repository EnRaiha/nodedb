// SPDX-License-Identifier: BUSL-1.1

pub mod partitions;

pub use partitions::{
    MergeOutcome, MergeSource, MergedPartition, merge_sources, schema_incompatibility,
};
