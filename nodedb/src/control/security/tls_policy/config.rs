// SPDX-License-Identifier: BUSL-1.1

//! [`TlsPolicyConfig`] — operator-facing knobs for TLS enforcement, reached
//! from the `[auth.tls_policy]` section of the server config.

use serde::{Deserialize, Serialize};

/// TLS enforcement configuration.
///
/// Enforcement is **off** unless [`TlsPolicyConfig::enabled`] is set. Turning
/// it on by default would refuse traffic that works today: `reject_cleartext`
/// would break every plaintext deployment (the pgwire, native, RESP, ILP and
/// HTTP listeners all accept cleartext unless a certificate is configured),
/// and a minimum version would refuse any client that cannot reach it. Both
/// are deployment facts the server cannot infer, so enabling this is an
/// explicit operator decision taken together with the values below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsPolicyConfig {
    /// Master switch. When `false` (the default) no connection is refused on
    /// transport grounds, on any listener.
    #[serde(default)]
    pub enabled: bool,

    /// Minimum TLS version a *TLS* connection must have negotiated, written
    /// the way an operator writes it (`"1.2"`, `"1.3"`). Parsed into an
    /// ordered [`TlsVersion`](super::TlsVersion) at load; an unparseable value
    /// fails startup rather than silently falling back.
    ///
    /// This governs encrypted connections only. Whether a *cleartext*
    /// connection is allowed at all is [`TlsPolicyConfig::reject_cleartext`] —
    /// raising the minimum does not implicitly ban plaintext.
    #[serde(default = "default_min_tls_version")]
    pub min_tls_version: String,

    /// Refuse connections that carry no TLS at all. Superusers are exempt (see
    /// [`TlsPolicy::check_connection`](super::TlsPolicy::check_connection)).
    #[serde(default)]
    pub reject_cleartext: bool,
}

fn default_min_tls_version() -> String {
    "1.2".into()
}

impl Default for TlsPolicyConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_tls_version: default_min_tls_version(),
            reject_cleartext: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_off_and_breaks_no_existing_deployment() {
        let config = TlsPolicyConfig::default();
        assert!(!config.enabled);
        assert!(!config.reject_cleartext);
        assert_eq!(config.min_tls_version, "1.2");
    }

    #[test]
    fn an_omitted_section_body_still_deserializes_to_the_defaults() {
        let config: TlsPolicyConfig = toml::from_str("").expect("empty section is valid");
        assert_eq!(config, TlsPolicyConfig::default());
    }

    #[test]
    fn operator_values_survive_deserialization() {
        let config: TlsPolicyConfig = toml::from_str(
            r#"
            enabled = true
            min_tls_version = "1.3"
            reject_cleartext = true
            "#,
        )
        .expect("valid section");

        assert!(config.enabled);
        assert_eq!(config.min_tls_version, "1.3");
        assert!(config.reject_cleartext);
    }
}
