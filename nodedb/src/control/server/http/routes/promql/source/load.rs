// SPDX-License-Identifier: BUSL-1.1

//! Load every series a set of selector reads needs.
//!
//! This is the one data source of the PromQL endpoints: stored metric
//! collections, plus this node's counters for principals allowed to see
//! them.

use std::collections::BTreeMap;

use crate::control::promql::types::{Labels, Sample, Series};

use super::budget::SampleBudget;
use super::discovery::metric_collections;
use super::node_metrics::{node_series, visible_to};
use super::rows::sorted_unique;
use super::scan::scan_metric;
use super::scope::ReadScope;
use super::windows::SelectorRead;

/// The series `reads` select, merged by label set.
///
/// Two reads of the same metric (`a + a[5m]`, or two `match[]` values) can
/// return the same series with overlapping windows; its samples are merged
/// and de-duplicated by timestamp.
pub(crate) async fn load_series(
    scope: &ReadScope<'_>,
    reads: &[SelectorRead],
    budget: &mut SampleBudget,
    now_ms: i64,
) -> crate::Result<Vec<Series>> {
    for read in reads {
        read.require_selective()?;
    }
    let mut merged: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
    // The scans run one after another on purpose. Each scan's row ceiling
    // is what the budget has left after the scans before it, and that is
    // what bounds the whole request. Concurrent scans would each need the
    // full remaining budget as their ceiling.
    for read in reads {
        for metric in metric_collections(scope, read)? {
            for series in scan_metric(scope, &metric, read, budget).await? {
                merged
                    .entry(series.labels)
                    .or_default()
                    .extend(series.samples);
            }
        }
    }
    if visible_to(scope.identity) {
        for series in node_series(scope.state, now_ms) {
            if reads.iter().any(|read| read.selects(&series.labels)) {
                budget.charge(series.samples.len())?;
                merged
                    .entry(series.labels)
                    .or_default()
                    .extend(series.samples);
            }
        }
    }
    Ok(merged
        .into_iter()
        .map(|(labels, samples)| Series {
            labels,
            samples: sorted_unique(samples),
        })
        .collect())
}
