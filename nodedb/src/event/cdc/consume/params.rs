// SPDX-License-Identifier: BUSL-1.1

//! Inputs and outputs of a stream consume.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::event::cdc::event::CdcEvent;
use crate::event::cdc::offset::CdcOffset;

/// Parameters for consuming events from a stream.
pub struct ConsumeParams<'a> {
    pub database_id: crate::types::DatabaseId,
    pub tenant_id: u64,
    pub stream_name: &'a str,
    pub group_name: &'a str,
    /// Optional: consume from a specific partition only.
    pub partition: Option<u32>,
    /// Maximum events to return.
    pub limit: usize,
}

/// Result of consuming events from a stream.
pub struct ConsumeResult {
    /// The events read from the buffer. Events are shared `Arc<CdcEvent>`
    /// so consumer fan-out (webhook, Kafka, SHOW, commit) doesn't deep-clone.
    pub events: Vec<Arc<CdcEvent>>,
    /// Per-partition latest composite position seen in this batch.
    pub partition_offsets: Vec<(u32, CdcOffset)>,
    /// Number of events dropped from this stream's buffer since the consumer
    /// group's previous poll. Zero on the first ever poll for this group, or
    /// when no evictions have occurred.
    pub evicted_since_last_poll: u64,
    /// Oldest composite position still available in the stream buffer. The
    /// initial position means the buffer is empty.
    pub oldest_available_offset: CdcOffset,
}

/// The highest position of each partition in `events`, by partition.
pub fn batch_tails(events: &[Arc<CdcEvent>]) -> Vec<(u32, CdcOffset)> {
    let mut tails: BTreeMap<u32, CdcOffset> = BTreeMap::new();
    for event in events {
        let tail = tails.entry(event.partition).or_insert(CdcOffset::ZERO);
        if event.position() > *tail {
            *tail = event.position();
        }
    }
    tails.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(partition: u32, index: u64, sequence: u64) -> Arc<CdcEvent> {
        Arc::new(CdcEvent {
            sequence,
            partition,
            collection: "orders".into(),
            op: "INSERT".into(),
            row_id: format!("row-{index}-{sequence}"),
            event_time: 0,
            lsn: 0,
            index,
            epoch: 0,
            database_id: crate::types::DatabaseId::DEFAULT,
            tenant_id: 1,
            new_value: None,
            old_value: None,
            schema_version: 0,
            field_diffs: None,
            system_time_ms: None,
            valid_time_ms: None,
            source: crate::event::EventSource::User,
        })
    }

    #[test]
    fn batch_tails_keep_each_partitions_highest_position() {
        let events = [
            event(2, 5, 2),
            event(1, 9, 4),
            event(2, 5, 4),
            event(1, 3, 2),
        ];
        assert_eq!(
            batch_tails(&events),
            vec![(1, CdcOffset::new(9, 4)), (2, CdcOffset::new(5, 4))]
        );
    }
}
