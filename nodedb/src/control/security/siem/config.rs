// SPDX-License-Identifier: BUSL-1.1

//! SIEM export configuration, loaded from the `[auth.siem]` section of the
//! server config file.
//!
//! An absent section leaves `destinations` empty and `webhook_url` blank,
//! which makes [`SiemExporter::is_configured`] false — the audit hook then
//! buffers nothing and no delivery loop is spawned.
//!
//! [`SiemExporter::is_configured`]: super::exporter::SiemExporter::is_configured

use serde::{Deserialize, Serialize};

/// SIEM export configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiemConfig {
    /// Export destinations: "splunk", "datadog", "webhook".
    #[serde(default)]
    pub destinations: Vec<String>,
    /// Webhook URL for audit events.
    #[serde(default)]
    pub webhook_url: String,
    /// HMAC secret for webhook signature (hex-encoded).
    #[serde(default)]
    pub webhook_hmac_secret: String,
    /// Maximum events to buffer before dropping oldest.
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    /// Per-request timeout (seconds) for the webhook POST.
    #[serde(default = "default_webhook_timeout_secs")]
    pub webhook_timeout_secs: u64,
    /// Interval (seconds) between webhook delivery attempts when the
    /// previous attempt succeeded or found nothing to send.
    #[serde(default = "default_flush_interval_secs")]
    pub flush_interval_secs: u64,
    /// Ceiling (seconds) for the exponential backoff applied after a failed
    /// delivery. Buffered events are retained across retries.
    #[serde(default = "default_max_backoff_secs")]
    pub max_backoff_secs: u64,
}

fn default_buffer_size() -> usize {
    10_000
}

fn default_webhook_timeout_secs() -> u64 {
    10
}

fn default_flush_interval_secs() -> u64 {
    10
}

fn default_max_backoff_secs() -> u64 {
    300
}

impl Default for SiemConfig {
    fn default() -> Self {
        Self {
            destinations: Vec::new(),
            webhook_url: String::new(),
            webhook_hmac_secret: String::new(),
            buffer_size: default_buffer_size(),
            webhook_timeout_secs: default_webhook_timeout_secs(),
            flush_interval_secs: default_flush_interval_secs(),
            max_backoff_secs: default_max_backoff_secs(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_not_configured_and_omitted_keys_fall_back() {
        let cfg: SiemConfig = toml::from_str("").expect("empty table is valid");
        assert!(cfg.destinations.is_empty());
        assert!(cfg.webhook_url.is_empty());
        assert_eq!(cfg.buffer_size, default_buffer_size());
        assert_eq!(cfg.flush_interval_secs, default_flush_interval_secs());
        assert_eq!(cfg.max_backoff_secs, default_max_backoff_secs());
    }

    #[test]
    fn toml_section_populates_every_knob() {
        let cfg: SiemConfig = toml::from_str(
            r#"
            destinations = ["webhook"]
            webhook_url = "https://siem.example/ingest"
            webhook_hmac_secret = "s3cret"
            buffer_size = 42
            webhook_timeout_secs = 3
            flush_interval_secs = 5
            max_backoff_secs = 60
            "#,
        )
        .expect("valid siem table");
        assert_eq!(cfg.destinations, vec!["webhook".to_string()]);
        assert_eq!(cfg.webhook_url, "https://siem.example/ingest");
        assert_eq!(cfg.buffer_size, 42);
        assert_eq!(cfg.webhook_timeout_secs, 3);
        assert_eq!(cfg.flush_interval_secs, 5);
        assert_eq!(cfg.max_backoff_secs, 60);
    }
}
