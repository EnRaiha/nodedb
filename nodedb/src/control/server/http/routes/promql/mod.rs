// SPDX-License-Identifier: BUSL-1.1

//! Prometheus-compatible PromQL HTTP API at `/obsv/api/v1/*`.
//!
//! Grafana data source URL: `http://nodedb:6480/obsv/api`

pub mod buildinfo;
pub mod handlers;
pub(crate) mod helpers;
mod metric_registration;
pub mod params;
mod remote;
mod remote_read;
pub(crate) mod source;

pub use buildinfo::buildinfo;
pub use handlers::{
    annotations, instant_query, label_names, label_values, metadata, range_query, series_query,
};
pub use params::PromParams;
pub use remote::remote_write;
pub use remote_read::remote_read;
