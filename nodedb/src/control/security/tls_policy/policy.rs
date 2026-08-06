// SPDX-License-Identifier: BUSL-1.1

//! [`TlsPolicy`] — the parsed, comparison-ready form of `[auth.tls_policy]`,
//! and the decision it makes about one connection.
//!
//! The policy holds a [`TlsVersion`], never the operator's string: parsing
//! happens once, at load, so an unparseable minimum fails startup instead of
//! reaching a comparison that cannot be made correctly.

use super::config::TlsPolicyConfig;
use super::transport::TransportSecurity;
use super::version::TlsVersion;

/// Why a connection was refused on transport grounds.
///
/// Carries no tenant or identity: the guard that raises it owns that context
/// and wraps this into the transport-neutral authorization rejection.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TlsRefusal {
    /// The connection carries no TLS and the policy rejects cleartext.
    #[error("cleartext connections rejected by TLS policy")]
    CleartextRejected,
    /// TLS was negotiated below the configured minimum.
    #[error("TLS {negotiated} is below the required minimum TLS {required}")]
    VersionBelowMinimum {
        negotiated: TlsVersion,
        required: TlsVersion,
    },
    /// TLS was negotiated but its version could not be identified, so it
    /// cannot be shown to clear the minimum.
    #[error("negotiated TLS version could not be identified")]
    UnidentifiedVersion,
}

/// TLS enforcement policy for accepted connections.
///
/// Constructed once from [`TlsPolicyConfig`] at startup and read per
/// connection; it holds no mutable state and no per-connection memory, so a
/// long-lived server accumulates nothing here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPolicy {
    enabled: bool,
    min_version: TlsVersion,
    reject_cleartext: bool,
}

impl Default for TlsPolicy {
    /// Enforcement off, matching [`TlsPolicyConfig::default`].
    fn default() -> Self {
        Self {
            enabled: false,
            min_version: TlsVersion::Tls1_2,
            reject_cleartext: false,
        }
    }
}

impl TlsPolicy {
    /// Build the policy from the operator's configuration.
    ///
    /// Returns [`crate::Error::Config`] when `min_tls_version` is not a TLS
    /// version. The value is parsed even when `enabled` is false: a typo in a
    /// dormant section must not lie in wait until the day enforcement is
    /// switched on.
    pub fn from_config(config: &TlsPolicyConfig) -> crate::Result<Self> {
        Ok(Self {
            enabled: config.enabled,
            min_version: TlsVersion::parse(&config.min_tls_version)?,
            reject_cleartext: config.reject_cleartext,
        })
    }

    /// Whether this policy refuses anything at all.
    pub fn is_enforcing(&self) -> bool {
        self.enabled
    }

    /// The minimum version TLS connections must negotiate.
    pub fn min_version(&self) -> TlsVersion {
        self.min_version
    }

    /// Decide whether a connection may proceed.
    ///
    /// `transport` is what the connection actually negotiated, captured at
    /// accept. `is_superuser` comes from the authenticated identity, which is
    /// why this runs after authentication rather than at accept.
    ///
    /// The superuser carve-out is deliberately narrow: it exempts a superuser
    /// from the **cleartext** rejection only, so an operator who has just
    /// turned `reject_cleartext` on still has a local administrative way in.
    /// It does **not** exempt anyone from the minimum-version check — a
    /// superuser session on an obsolete TLS version is the most valuable
    /// session on the server to downgrade, so the carve-out that would let it
    /// happen does not exist.
    pub fn check_connection(
        &self,
        transport: TransportSecurity,
        is_superuser: bool,
    ) -> Result<(), TlsRefusal> {
        if !self.enabled {
            return Ok(());
        }

        match transport {
            TransportSecurity::Cleartext => {
                if self.reject_cleartext && !is_superuser {
                    return Err(TlsRefusal::CleartextRejected);
                }
                Ok(())
            }
            TransportSecurity::Tls(negotiated) => {
                if negotiated < self.min_version {
                    return Err(TlsRefusal::VersionBelowMinimum {
                        negotiated,
                        required: self.min_version,
                    });
                }
                Ok(())
            }
            TransportSecurity::TlsUnidentified => Err(TlsRefusal::UnidentifiedVersion),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(enabled: bool, min: &str, reject_cleartext: bool) -> TlsPolicy {
        TlsPolicy::from_config(&TlsPolicyConfig {
            enabled,
            min_tls_version: min.into(),
            reject_cleartext,
        })
        .expect("valid configuration")
    }

    // ── Cleartext ──────────────────────────────────────────────────────────

    #[test]
    fn cleartext_is_refused_when_configured_and_allowed_when_not() {
        let rejecting = policy(true, "1.2", true);
        assert_eq!(
            rejecting.check_connection(TransportSecurity::Cleartext, false),
            Err(TlsRefusal::CleartextRejected)
        );

        let permitting = policy(true, "1.2", false);
        assert!(
            permitting
                .check_connection(TransportSecurity::Cleartext, false)
                .is_ok()
        );
    }

    /// The carve-out, pinned: a superuser passes the cleartext rejection.
    #[test]
    fn superuser_is_exempt_from_the_cleartext_rejection() {
        let rejecting = policy(true, "1.2", true);
        assert!(
            rejecting
                .check_connection(TransportSecurity::Cleartext, true)
                .is_ok()
        );
    }

    /// The other half of the carve-out, pinned: it stops at cleartext. A
    /// superuser on a below-minimum TLS connection is still refused.
    #[test]
    fn superuser_is_not_exempt_from_the_minimum_version() {
        let strict = policy(true, "1.3", true);
        assert_eq!(
            strict.check_connection(TransportSecurity::Tls(TlsVersion::Tls1_2), true),
            Err(TlsRefusal::VersionBelowMinimum {
                negotiated: TlsVersion::Tls1_2,
                required: TlsVersion::Tls1_3,
            })
        );
    }

    // ── Minimum version ────────────────────────────────────────────────────

    #[test]
    fn tls_below_the_minimum_is_refused_and_at_or_above_is_allowed() {
        let strict = policy(true, "1.3", false);
        assert_eq!(
            strict.check_connection(TransportSecurity::Tls(TlsVersion::Tls1_2), false),
            Err(TlsRefusal::VersionBelowMinimum {
                negotiated: TlsVersion::Tls1_2,
                required: TlsVersion::Tls1_3,
            })
        );
        assert!(
            strict
                .check_connection(TransportSecurity::Tls(TlsVersion::Tls1_3), false)
                .is_ok()
        );

        let relaxed = policy(true, "1.2", false);
        assert!(
            relaxed
                .check_connection(TransportSecurity::Tls(TlsVersion::Tls1_2), false)
                .is_ok(),
            "a connection exactly at the minimum is allowed"
        );
        assert!(
            relaxed
                .check_connection(TransportSecurity::Tls(TlsVersion::Tls1_3), false)
                .is_ok()
        );
        assert_eq!(
            relaxed.check_connection(TransportSecurity::Tls(TlsVersion::Tls1_0), false),
            Err(TlsRefusal::VersionBelowMinimum {
                negotiated: TlsVersion::Tls1_0,
                required: TlsVersion::Tls1_2,
            })
        );
    }

    /// The version comparison is a real comparison, not the old
    /// string-in-the-error-message placeholder: raising the minimum changes
    /// the verdict for the very same connection.
    #[test]
    fn raising_the_minimum_changes_the_verdict_for_the_same_connection() {
        let connection = TransportSecurity::Tls(TlsVersion::Tls1_2);
        assert!(
            policy(true, "1.2", false)
                .check_connection(connection, false)
                .is_ok()
        );
        assert!(
            policy(true, "1.3", false)
                .check_connection(connection, false)
                .is_err()
        );
    }

    #[test]
    fn unidentified_tls_fails_closed() {
        let enforcing = policy(true, "1.2", false);
        assert_eq!(
            enforcing.check_connection(TransportSecurity::TlsUnidentified, false),
            Err(TlsRefusal::UnidentifiedVersion)
        );
        assert_eq!(
            enforcing.check_connection(TransportSecurity::TlsUnidentified, true),
            Err(TlsRefusal::UnidentifiedVersion),
            "the superuser carve-out covers cleartext only"
        );
    }

    // ── Master switch and configuration ────────────────────────────────────

    #[test]
    fn a_disabled_policy_refuses_nothing() {
        let disabled = policy(false, "1.3", true);
        assert!(
            disabled
                .check_connection(TransportSecurity::Cleartext, false)
                .is_ok()
        );
        assert!(
            disabled
                .check_connection(TransportSecurity::Tls(TlsVersion::Tls1_0), false)
                .is_ok()
        );
        assert!(
            disabled
                .check_connection(TransportSecurity::TlsUnidentified, false)
                .is_ok()
        );
    }

    #[test]
    fn the_default_policy_is_off() {
        let default = TlsPolicy::default();
        assert!(!default.is_enforcing());
        assert_eq!(
            default,
            TlsPolicy::from_config(&TlsPolicyConfig::default()).expect("default")
        );
        assert!(
            default
                .check_connection(TransportSecurity::Cleartext, false)
                .is_ok()
        );
    }

    #[test]
    fn an_unparseable_minimum_is_rejected_at_load_not_defaulted() {
        let result = TlsPolicy::from_config(&TlsPolicyConfig {
            enabled: true,
            min_tls_version: "1.2.3".into(),
            reject_cleartext: false,
        });
        assert!(matches!(result, Err(crate::Error::Config { .. })));
    }

    /// A typo in a dormant section is still a startup failure — it must not
    /// wait to surface until the day the operator flips `enabled`.
    #[test]
    fn an_unparseable_minimum_is_rejected_even_while_disabled() {
        let result = TlsPolicy::from_config(&TlsPolicyConfig {
            enabled: false,
            min_tls_version: "tls-one-point-two".into(),
            reject_cleartext: false,
        });
        assert!(matches!(result, Err(crate::Error::Config { .. })));
    }

    #[test]
    fn configured_minimum_reaches_the_policy() {
        assert_eq!(policy(true, "1.3", false).min_version(), TlsVersion::Tls1_3);
        assert_eq!(policy(true, "1.0", false).min_version(), TlsVersion::Tls1_0);
    }
}
