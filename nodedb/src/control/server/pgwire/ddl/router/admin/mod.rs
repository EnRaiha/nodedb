// SPDX-License-Identifier: BUSL-1.1

mod observability_routes;
mod routes;

pub(in crate::control::server::pgwire::ddl::router) use routes::dispatch;
