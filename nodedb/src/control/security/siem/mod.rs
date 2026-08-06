// SPDX-License-Identifier: BUSL-1.1

//! SIEM export: CDC stream for audit_log + auth_events, webhook with HMAC.

pub mod config;
pub mod delivery;
pub mod exporter;
pub mod flush_loop;
pub mod metrics;

pub use config::SiemConfig;
pub use delivery::DeliveryOutcome;
pub use exporter::SiemExporter;
pub use flush_loop::spawn_export_loop;
