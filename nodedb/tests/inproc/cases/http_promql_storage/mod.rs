// SPDX-License-Identifier: BUSL-1.1

//! PromQL reads over remote-written data.
//!
//! Endpoints covered (under `/v1/obsv/api/v1/`):
//! - POST /write, POST /read — Prometheus remote write and remote read
//! - /query, /query_range, /series, /labels, /label/{name}/values
//!
//! Contracts asserted:
//! - Remote-written samples read back through remote read and PromQL
//! - Label matchers give Prometheus' answer, pushed down or not
//! - Each database sees only its own metrics; collection grants and read
//!   policies apply; node counters need the Monitor role
//! - Sample and resolution limits and malformed requests fail loudly

mod isolation;
mod limits;
mod matchers;
mod round_trip;
mod support;
