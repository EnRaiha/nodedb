// SPDX-License-Identifier: BUSL-1.1

//! The partition interval a flush of a timeseries collection cuts at.

use nodedb_types::timeseries::PartitionInterval;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::TsCollectionKey;

/// The interval `Auto` uses before the registry has adapted it: one day.
const AUTO_START_MS: u64 = 86_400_000;

impl CoreLoop {
    /// The interval a flush of `key` cuts partitions at.
    ///
    /// The collection's registered schema carries the declared interval. A
    /// collection registered without one flushes into one partition per flush.
    /// `Auto` takes the interval the registry adapted to.
    pub(in crate::data::executor) fn ts_partition_interval(
        &self,
        key: &TsCollectionKey,
    ) -> crate::Result<PartitionInterval> {
        let declared = self
            .doc_configs
            .get(key)
            .and_then(|config| config.timeseries.as_ref())
            .map(|schema| schema.partition_interval.trim())
            .filter(|text| !text.is_empty());
        let Some(text) = declared else {
            return Ok(PartitionInterval::Unbounded);
        };
        let interval = PartitionInterval::parse(text).map_err(|e| crate::Error::Storage {
            engine: "timeseries".into(),
            detail: format!(
                "collection {}: registered partition interval '{text}' does not parse: {e}",
                key.2
            ),
        })?;
        if interval == PartitionInterval::Auto {
            return Ok(self
                .ts_registries
                .get(key)
                .map(|registry| registry.current_interval().clone())
                .unwrap_or(PartitionInterval::Duration(AUTO_START_MS)));
        }
        Ok(interval)
    }
}
