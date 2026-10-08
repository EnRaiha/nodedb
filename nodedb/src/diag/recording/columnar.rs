// SPDX-License-Identifier: BUSL-1.1

//! Capture sites for columnar segment reads.

use faultbox::{Capture, EventKind, error_chain_of};

use super::shared::error_class;
use crate::diag::context;

/// Report a flushed columnar segment that does not open or decode.
///
/// Called only from the shared flushed-segment readers in
/// `columnar_read/flushed_segment.rs`, which every segment read goes through.
/// The caller returns the error alongside this report.
pub fn columnar_segment_corrupt(
    err: &crate::Error,
    collection: &str,
    segment_id: u64,
    stage: &'static str,
    site: &'static str,
) {
    let class = error_class(err);
    let ctx = context::ColumnarSegmentCorrupt {
        collection,
        segment_id,
        stage,
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "flushed columnar segment does not decode, so the read is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}

/// Report an on-disk timeseries partition whose schema, column, or symbol
/// dictionary does not read.
///
/// Called only from `read_ts_partition`. The caller returns the error
/// alongside this report.
pub fn timeseries_partition_unreadable(
    err: &crate::Error,
    partition: &str,
    stage: &'static str,
    site: &'static str,
) {
    let class = error_class(err);
    let ctx = context::TimeseriesPartitionUnreadable {
        partition,
        stage,
        site,
        error_class: &class,
    };
    let _ = Capture::new(
        EventKind::Corruption,
        "timeseries partition does not read, so the scan is refused",
    )
    .error_chain(error_chain_of(err))
    .domain(&ctx)
    .with_backtrace()
    .emit();
}
