// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral continuous aggregate DDL — CREATE / DROP / REFRESH / SHOW.

pub mod create;
pub mod drop;
pub mod parse;
pub mod periodic;
pub mod refresh;
pub mod register;
pub mod show;

pub use create::{CreateContinuousAggregateRequest, create_continuous_aggregate};
pub use drop::{continuous_aggregate_exists, drop_continuous_aggregate};
pub use periodic::spawn_periodic_refresh_loop;
pub use refresh::refresh_continuous_aggregate;
pub use register::register_persisted_continuous_aggregates;
pub use show::show_continuous_aggregates;
