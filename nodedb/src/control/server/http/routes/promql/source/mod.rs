// SPDX-License-Identifier: BUSL-1.1

//! The series PromQL endpoints read: stored metric collections, scanned
//! through the Control Plane → Data Plane dispatch, plus this node's
//! counters.

pub mod budget;
pub mod discovery;
pub mod load;
pub mod node_metrics;
pub mod pushdown;
pub mod rows;
pub mod scan;
pub mod scope;
pub mod windows;

pub(crate) use budget::SampleBudget;
pub(crate) use discovery::readable_metric_collections;
pub(crate) use load::load_series;
pub(crate) use node_metrics::{NODE_METRICS, visible_to as node_metrics_visible};
pub(crate) use scope::ReadScope;
pub(crate) use windows::{SelectorRead, selector_reads};
