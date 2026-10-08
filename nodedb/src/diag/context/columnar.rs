// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for columnar segment capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A flushed columnar segment whose bytes do not open or decode.
pub(in crate::diag) struct ColumnarSegmentCorrupt<'a> {
    /// Collection that owns the segment.
    pub collection: &'a str,
    /// 1-based flushed segment id.
    pub segment_id: u64,
    /// Decode step that failed: `open`, `column`, or `cell`.
    pub stage: &'static str,
    /// Path that read the segment.
    pub site: &'static str,
    /// The error class: the error text before its first colon.
    pub error_class: &'a str,
}

impl DomainContext for ColumnarSegmentCorrupt<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.columnar_segment_corrupt"
    }

    fn grouping_key(&self) -> String {
        // The segment is the root cause. The site and row are occurrences,
        // so every read of one bad segment files one report with a count.
        format!(
            "collection={} segment={} stage={}",
            self.collection, self.segment_id, self.stage
        )
    }

    fn to_json(&self) -> Value {
        json!({
            "collection": self.collection,
            "segment_id": self.segment_id,
            "stage": self.stage,
            "site": self.site,
            "error_class": self.error_class,
            "why_fatal": "the segment is the only in-memory home of its rows between \
                          checkpoints, and a checkpoint persists the same bytes. Every \
                          read that touches it is refused until it is replaced",
            "operator_action": "restore the collection from a snapshot taken before the \
                                segment was damaged. A repeated CRC failure on the same \
                                segment also places it in quarantine",
        })
    }
}

/// An on-disk timeseries partition whose files do not read.
pub(in crate::diag) struct TimeseriesPartitionUnreadable<'a> {
    /// Partition directory name.
    pub partition: &'a str,
    /// File that failed: `schema`, `column`, or `symbol_dict`.
    pub stage: &'static str,
    /// Path that read the partition.
    pub site: &'static str,
    /// The error class: the error text before its first colon.
    pub error_class: &'a str,
}

impl DomainContext for TimeseriesPartitionUnreadable<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.timeseries_partition_unreadable"
    }

    fn grouping_key(&self) -> String {
        // The partition is the root cause. The site is the occurrence.
        format!("partition={} stage={}", self.partition, self.stage)
    }

    fn to_json(&self) -> Value {
        json!({
            "partition": self.partition,
            "stage": self.stage,
            "site": self.site,
            "error_class": self.error_class,
            "why_fatal": "the partition files are the only copy of its rows, so every \
                          scan that reaches it is refused until it is repaired",
            "operator_action": "inspect the named partition directory for a missing or \
                                damaged file. Restore the collection from a snapshot if \
                                the file cannot be recovered",
        })
    }
}
